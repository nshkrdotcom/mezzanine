defmodule Mezzanine.ChassisWorkflowsTest do
  use ExUnit.Case, async: false

  alias Mezzanine.Outbox.ChassisDrainWorker
  alias Mezzanine.Read.ChassisDeploymentProjection
  alias Mezzanine.Read.ChassisEvolutionProjection
  alias Mezzanine.Workflow.ChassisDeploymentWorkflow
  alias Mezzanine.Workflow.ChassisRollbackWorkflow

  alias Mezzanine.Workflow.Chassis.Evolution.{
    CandidatePatchWorkflow,
    CandidateScoringWorkflow,
    FailureBatchWorkflow,
    ModelMaterializationWorkflow,
    PromotionApplyWorkflow,
    PromotionConsentWorkflow,
    SwapRollbackWorkflow,
    TensorPatchReloadWorkflow,
    TrialReplayWorkflow
  }

  alias Mezzanine.Workflow.Chassis.Evolution.Engine

  defmodule RecordingBoundary do
    def dispatch(envelope, opts) do
      send(
        self(),
        {:boundary_dispatch, envelope.protocol_ref, Keyword.fetch!(opts, :protocol_module)}
      )

      case envelope.protocol_ref do
        "boundary:mezzanine.chassis.evolution." <> _ ->
          Chassis.Mezzanine.Bridge.Evolution.LocalDispatcher.dispatch(envelope, opts)

        "boundary:chassis.model." <> _ ->
          Chassis.Mezzanine.Bridge.Evolution.LocalDispatcher.dispatch(envelope, opts)

        _other ->
          Chassis.Boundary.dispatch(envelope, opts)
      end
    end
  end

  setup do
    projection_file =
      Path.join(System.tmp_dir!(), "mezzanine_chassis_projection_#{unique()}.term")

    File.rm(projection_file)
    on_exit(fn -> File.rm(projection_file) end)

    {:ok, read_store} = ChassisDeploymentProjection.start_link(name: nil)
    {:ok, evolution_store} = ChassisEvolutionProjection.start_link(name: nil)

    %{projection_file: projection_file, read_store: read_store, evolution_store: evolution_store}
  end

  test "deployment workflow dispatches through Chassis boundary and drains a projection", ctx do
    assert {:ok, result} =
             ChassisDeploymentWorkflow.dispatch(default_attrs(),
               boundary_dispatcher: RecordingBoundary,
               read_store: ctx.read_store,
               projection_store: {:file, ctx.projection_file}
             )

    assert_receive {:boundary_dispatch, "boundary:mezzanine.chassis.materialize_deployment:v1",
                    Chassis.Mezzanine.Bridge.MaterializeDeployment}

    assert result.workflow == :chassis_deployment
    assert result.status == "active"
    assert result.deployment_receipt_ref =~ "receipt:deployment:"
    assert result.outbox_delivered == 1

    assert {:ok, in_memory} =
             ChassisDeploymentProjection.latest(ctx.read_store,
               tenant_ref: "tenant:dev",
               installation_ref: "installation:acme:demo"
             )

    assert in_memory.receipt_ref == result.deployment_receipt_ref

    assert {:ok, persisted} =
             ChassisDeploymentProjection.latest({:file, ctx.projection_file},
               tenant_ref: "tenant:dev",
               installation_ref: "installation:acme:demo"
             )

    assert persisted.receipt_ref == result.deployment_receipt_ref
  end

  test "deployment workflow failure returns boundary error and leaves projections empty", ctx do
    attrs = Map.put(default_attrs(), :runtime_profile_ref, "profile:missing")

    assert {:error, %Chassis.Boundary.Error{} = error} =
             ChassisDeploymentWorkflow.dispatch(attrs,
               read_store: ctx.read_store,
               projection_store: {:file, ctx.projection_file}
             )

    assert error.code in [:invalid_request, :non_retryable_failure]

    assert ChassisDeploymentProjection.latest(ctx.read_store, tenant_ref: "tenant:dev") ==
             {:error, :not_found}

    assert ChassisDeploymentProjection.latest({:file, ctx.projection_file},
             tenant_ref: "tenant:dev"
           ) == {:error, :not_found}
  end

  test "rollback workflow uses the Chassis rollback bridge", ctx do
    assert {:ok, deployed} =
             ChassisDeploymentWorkflow.dispatch(default_attrs(),
               read_store: ctx.read_store,
               projection_store: {:file, ctx.projection_file}
             )

    assert {:ok, rollback} =
             ChassisRollbackWorkflow.dispatch(%{
               app_ref: deployed.app_ref,
               tenant_ref: "tenant:dev",
               installation_ref: "installation:acme:demo",
               current_receipt_ref: deployed.deployment_receipt_ref
             })

    assert rollback.workflow == :chassis_rollback
    assert rollback.status == "rolled_back"
    assert rollback.rollback_receipt_ref =~ "receipt:rollback:"
  end

  test "outbox drain worker keeps failed events pending until projection reducer succeeds", ctx do
    {:ok, outbox} = Chassis.Mezzanine.Bridge.Outbox.start_link(name: nil)

    invalid_event = %{
      kind: :unknown,
      payload: %{},
      idempotency_key: "receipt:deployment:bad"
    }

    assert {:ok, _event} = Chassis.Mezzanine.Bridge.Outbox.enqueue(outbox, invalid_event)

    assert {:error, {:drain_failed, _event, {:unsupported_projection_event, :unknown}}} =
             ChassisDrainWorker.drain(outbox, read_store: ctx.read_store)

    assert [%{status: :pending}] = Chassis.Mezzanine.Bridge.Outbox.list(outbox)

    valid_event = %{
      kind: :chassis_deployment,
      payload: %{
        receipt_ref: "receipt:deployment:valid",
        tenant_ref: "tenant:dev",
        installation_ref: "installation:acme:demo",
        status: :active
      },
      idempotency_key: "receipt:deployment:valid"
    }

    {:ok, good_outbox} = Chassis.Mezzanine.Bridge.Outbox.start_link(name: nil)
    assert {:ok, _event} = Chassis.Mezzanine.Bridge.Outbox.enqueue(good_outbox, valid_event)

    assert {:ok, %{delivered: 1}} =
             ChassisDrainWorker.drain(good_outbox, read_store: ctx.read_store)

    assert [%{status: :delivered}] = Chassis.Mezzanine.Bridge.Outbox.list(good_outbox)
  end

  test "failure batch workflow dispatches the Chassis boundary and reduces evolution projection",
       ctx do
    assert {:ok, result} =
             FailureBatchWorkflow.dispatch(evolution_attrs(),
               boundary_dispatcher: RecordingBoundary,
               read_store: ctx.evolution_store
             )

    assert_receive {:boundary_dispatch,
                    "boundary:mezzanine.chassis.evolution.create_failure_batch:v1",
                    Chassis.Mezzanine.Bridge.Evolution.CreateFailureBatch}

    assert result.workflow == :chassis_failure_batch
    assert result.failure_batch_ref =~ "failure-batch:"
    assert result.outbox_delivered == 1

    assert {:ok, projection} =
             ChassisEvolutionProjection.latest(ctx.evolution_store,
               projection: :chassis_evolution,
               primary_ref: result.failure_batch_ref
             )

    assert projection.state_or_outcome == "created"
    assert projection.summary.failure_batch_ref == result.failure_batch_ref
  end

  test "all evolution workflows drive boundary protocols and reduce read projections", ctx do
    cases = [
      {CandidatePatchWorkflow, %{failure_batch_ref: "failure-batch:phase35"},
       ["boundary:mezzanine.chassis.evolution.start:v1"], :chassis_candidate},
      {TrialReplayWorkflow, %{candidate_ref: "candidate:phase35"},
       [
         "boundary:mezzanine.chassis.evolution.provision_trial_node:v1",
         "boundary:mezzanine.chassis.evolution.run_trial_replay:v1"
       ], :chassis_trial},
      {CandidateScoringWorkflow, %{trial_run_ref: "trial-run:phase35"},
       ["boundary:mezzanine.chassis.evolution.score_candidate:v1"], :chassis_score_matrix},
      {PromotionConsentWorkflow, %{candidate_ref: "candidate:phase35"},
       ["boundary:mezzanine.chassis.evolution.request_promotion:v1"], :chassis_promotion},
      {PromotionApplyWorkflow, %{candidate_ref: "candidate:phase35"},
       ["boundary:mezzanine.chassis.evolution.promote_candidate:v1"], :chassis_swap},
      {SwapRollbackWorkflow, %{swap_ref: "swap:phase35"},
       ["boundary:mezzanine.chassis.evolution.rollback_candidate:v1"], :chassis_swap},
      {ModelMaterializationWorkflow, %{model_ref: "model:hf:qwen3-small-fixture"},
       ["boundary:chassis.model.materialize_weight:v1"], :chassis_model_materialization},
      {TensorPatchReloadWorkflow, %{patch_ref: "patch:fixture:lora_001"},
       ["boundary:chassis.model.reload_tensor_patch:v1"], :chassis_tensor_reload}
    ]

    for {workflow, attrs, expected_protocols, projection_kind} <- cases do
      assert {:ok, result} =
               workflow.dispatch(Map.merge(evolution_attrs(), attrs),
                 boundary_dispatcher: RecordingBoundary,
                 read_store: ctx.evolution_store
               )

      assert result.status in ["accepted", "completed", "committed", "rolled_back"]

      for protocol_ref <- expected_protocols do
        assert_receive {:boundary_dispatch, ^protocol_ref, _module}
      end

      assert {:ok, projection} =
               ChassisEvolutionProjection.latest(ctx.evolution_store,
                 projection: projection_kind,
                 tenant_ref: "tenant:dev"
               )

      assert projection.projection == projection_kind
      assert projection.primary_ref
    end
  end

  test "promotion consent timeout transitions to stopped", ctx do
    assert {:ok, result} =
             PromotionConsentWorkflow.dispatch(
               Map.merge(evolution_attrs(), %{candidate_ref: "candidate:timeout"}),
               boundary_dispatcher: RecordingBoundary,
               read_store: ctx.evolution_store,
               consent_timeout_ms: 0,
               signals: []
             )

    assert result.status == "stopped"
    assert result.stop_reason == "operator_consent_timeout"

    assert_receive {:boundary_dispatch,
                    "boundary:mezzanine.chassis.evolution.request_promotion:v1", _module}

    assert_receive {:boundary_dispatch, "boundary:mezzanine.chassis.evolution.stop:v1", _module}

    assert {:ok, projection} =
             ChassisEvolutionProjection.latest(ctx.evolution_store,
               projection: :chassis_promotion,
               primary_ref: result.promotion_intent_ref
             )

    assert projection.state_or_outcome == "stopped"
  end

  test "evolution idempotency keys use sha256 workflow step input digest" do
    attrs = Map.merge(evolution_attrs(), %{failure_batch_ref: "failure-batch:idem"})
    input_digest = Engine.input_digest(attrs)

    expected =
      "idem:" <>
        (:crypto.hash(:sha256, "candidate_patch_workflow||start||#{input_digest}")
         |> Base.encode16(case: :lower))

    assert Engine.idempotency_key(:candidate_patch_workflow, :start, attrs) == expected
  end

  defp default_attrs do
    %{
      tenant_ref: "tenant:dev",
      installation_ref: "installation:acme:demo",
      actor_ref: "actor:operator",
      authority_ref: "authority:decision:phase16",
      app_atom: :demo,
      topology_ref: "topology:profile:monolith",
      service_spec_ref: "service:demo",
      runtime_profile_ref: "profile:monolith",
      placement_ref: "placement:local",
      environment: :dev,
      git_sha: "abcdef",
      release_version: "v1"
    }
  end

  defp evolution_attrs do
    %{
      tenant_ref: "tenant:dev",
      installation_ref: "installation:acme:demo",
      actor_ref: "actor:operator",
      authority_ref: "authority:decision:phase35",
      evidence_refs: ["ev:smoke:1"],
      summary: "bounded smoke",
      redaction_posture: "default",
      target_installation_ref: "installation:acme:demo",
      trace_id: "trace:phase35"
    }
  end

  defp unique, do: System.unique_integer([:positive]) |> Integer.to_string()
end
