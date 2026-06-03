defmodule Mezzanine.ChassisWorkflowsTest do
  use ExUnit.Case, async: false

  alias Mezzanine.Outbox.ChassisDrainWorker
  alias Mezzanine.Read.ChassisDeploymentProjection
  alias Mezzanine.Workflow.ChassisDeploymentWorkflow
  alias Mezzanine.Workflow.ChassisRollbackWorkflow

  defmodule RecordingBoundary do
    def dispatch(envelope, opts) do
      send(
        self(),
        {:boundary_dispatch, envelope.protocol_ref, Keyword.fetch!(opts, :protocol_module)}
      )

      Chassis.Boundary.dispatch(envelope, opts)
    end
  end

  setup do
    projection_file =
      Path.join(System.tmp_dir!(), "mezzanine_chassis_projection_#{unique()}.term")

    File.rm(projection_file)
    on_exit(fn -> File.rm(projection_file) end)

    {:ok, read_store} = ChassisDeploymentProjection.start_link(name: nil)

    %{projection_file: projection_file, read_store: read_store}
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

  test "future Chassis evolution placeholders fail closed" do
    assert {:error, {:not_implemented, Mezzanine.Workflow.Chassis.Evolution.FailureBatchWorkflow}} =
             Mezzanine.Workflow.Chassis.Evolution.FailureBatchWorkflow.dispatch()

    assert {:error, {:not_implemented, Mezzanine.Read.ChassisEvolutionProjection}} =
             Mezzanine.Read.ChassisEvolutionProjection.last()
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

  defp unique, do: System.unique_integer([:positive]) |> Integer.to_string()
end
