defmodule Mezzanine.AppKitBridge.SemanticFailureRecoveryServiceTest do
  use ExUnit.Case, async: false

  alias Ecto.Adapters.SQL.Sandbox

  alias Mezzanine.AppKitBridge.{
    OperatorQueryService,
    ReviewQueryService,
    SemanticFailureRecoveryService,
    WorkQueryService
  }

  alias Mezzanine.Control.ControlSession
  alias Mezzanine.Execution.{Dispatcher, ExecutionRecord}
  alias Mezzanine.Execution.Repo, as: ExecutionRepo
  alias Mezzanine.OpsDomain.Repo, as: OpsDomainRepo
  alias Mezzanine.Programs.{PolicyBundle, Program}
  alias Mezzanine.Runs.{Run, RunSeries}
  alias Mezzanine.Work.{WorkClass, WorkObject}

  setup do
    ops_domain_owner = Sandbox.start_owner!(OpsDomainRepo, shared: false)
    execution_owner = Sandbox.start_owner!(ExecutionRepo, shared: false)

    on_exit(fn ->
      Sandbox.stop_owner(execution_owner)
      Sandbox.stop_owner(ops_domain_owner)
    end)

    :ok
  end

  test "semantic failure recovery routes one reconciled execution into awaiting review without duplicating recovery state" do
    %{
      tenant_id: tenant_id,
      program: program,
      work_object: work_object,
      run: run,
      execution: execution
    } = fixture_stack("tenant-semantic-failure-recovery")

    assert {:ok, %{classification: :semantic_failure, execution: failed_execution}} =
             Dispatcher.reconcile_result(
               execution.id,
               {:semantic_failure,
                %{
                  "lower_receipt" => %{"run_id" => "lower-run-semantic"},
                  "error" => %{
                    "kind" => "semantic_failure",
                    "reason" => "model_confused"
                  }
                }},
               actor_ref: %{kind: :reconciler}
             )

    assert failed_execution.dispatch_state == :failed
    assert failed_execution.failure_kind == :semantic_failure

    assert {:ok, recovery} =
             SemanticFailureRecoveryService.recover_execution(tenant_id, failed_execution.id)

    assert recovery.run.id == run.id
    assert recovery.run.status == :failed
    assert recovery.work_object.id == work_object.id
    assert recovery.work_object.status == :awaiting_review
    assert recovery.review_unit.work_object_id == work_object.id
    assert recovery.review_unit.run_id == run.id
    assert recovery.review_unit.status == :pending
    assert recovery.review_created?
    assert recovery.review_unit.decision_profile["recovery_kind"] == "semantic_failure"
    assert recovery.review_unit.decision_profile["execution_id"] == failed_execution.id

    assert {:ok, detail} = WorkQueryService.get_subject_detail(tenant_id, work_object.id)
    assert detail.status == :awaiting_review
    assert detail.active_run_status == :failed
    assert detail.pending_review_ids == [recovery.review_unit.id]

    assert {:ok, operator_status} = OperatorQueryService.subject_status(tenant_id, work_object.id)
    assert operator_status.lifecycle_state == "awaiting_review"
    assert operator_status.pending_decision_refs |> Enum.map(& &1.id) == [recovery.review_unit.id]

    assert Enum.map(operator_status.payload.timeline, & &1.event_kind)
           |> Enum.sort() == ["review_created", "run_failed"]

    assert {:ok, pending_reviews} =
             ReviewQueryService.list_pending_reviews(tenant_id, program.id)

    assert Enum.map(pending_reviews, & &1.decision_ref.id) == [recovery.review_unit.id]

    assert {:ok, review_detail} =
             ReviewQueryService.get_review_detail(tenant_id, recovery.review_unit.id)

    assert review_detail.status == "pending"
    assert review_detail.payload.review_unit.id == recovery.review_unit.id

    assert review_detail.payload.review_unit.decision_profile["recovery_kind"] ==
             "semantic_failure"

    assert {:ok, second_recovery} =
             SemanticFailureRecoveryService.recover_execution(tenant_id, failed_execution.id)

    refute second_recovery.review_created?
    assert second_recovery.review_unit.id == recovery.review_unit.id

    assert {:ok, detail_after_second_pass} =
             WorkQueryService.get_subject_detail(tenant_id, work_object.id)

    assert detail_after_second_pass.pending_review_ids == [recovery.review_unit.id]
  end

  defp fixture_stack(tenant_id) do
    actor = %{tenant_id: tenant_id}
    trace_id = "trace-semantic-failure-#{System.unique_integer([:positive])}"

    {:ok, program} =
      Program.create_program(
        %{
          slug: "semantic-failure-#{System.unique_integer([:positive])}",
          name: "Semantic Failure Recovery Program",
          product_family: "operator_stack",
          configuration: %{},
          metadata: %{}
        },
        actor: actor,
        tenant: tenant_id
      )

    {:ok, bundle} =
      PolicyBundle.load_bundle(
        %{
          program_id: program.id,
          name: "default",
          version: "1.0.0",
          policy_kind: :workflow_md,
          source_ref: "WORKFLOW.md",
          body: workflow_body(),
          metadata: %{}
        },
        actor: actor,
        tenant: tenant_id
      )

    {:ok, work_class} =
      WorkClass.create_work_class(
        %{
          program_id: program.id,
          name: "coding_task_#{System.unique_integer([:positive])}",
          kind: "coding_task",
          intake_schema: %{"required" => ["title"]},
          policy_bundle_id: bundle.id,
          default_review_profile: %{"required" => false},
          default_run_profile: %{"runtime" => "session"}
        },
        actor: actor,
        tenant: tenant_id
      )

    {:ok, work_object} =
      WorkObject.ingest(
        %{
          program_id: program.id,
          work_class_id: work_class.id,
          external_ref: "linear:ENG-#{System.unique_integer([:positive])}",
          title: "Semantic failure recovery work",
          description: "Exercise post-acceptance semantic failure recovery",
          priority: 50,
          source_kind: "linear",
          payload: %{"issue_id" => "ENG-1"},
          normalized_payload: %{"issue_id" => "ENG-1"}
        },
        actor: actor,
        tenant: tenant_id
      )

    {:ok, work_object} =
      WorkObject.compile_plan(work_object, %{}, actor: actor, tenant: tenant_id)

    {:ok, control_session} =
      ControlSession.open(
        %{program_id: program.id, work_object_id: work_object.id},
        actor: actor,
        tenant: tenant_id
      )

    {:ok, run_series} =
      RunSeries.open_series(
        %{work_object_id: work_object.id, control_session_id: control_session.id},
        actor: actor,
        tenant: tenant_id
      )

    {:ok, run} =
      Run.schedule(
        %{
          run_series_id: run_series.id,
          attempt: 1,
          runtime_profile: %{"runtime" => "session"},
          grant_profile: %{"linear.issue.update" => "allow"}
        },
        actor: actor,
        tenant: tenant_id
      )

    {:ok, _run_series} =
      RunSeries.attach_current_run(run_series, %{current_run_id: run.id},
        actor: actor,
        tenant: tenant_id
      )

    {:ok, work_object} = WorkObject.mark_running(work_object, actor: actor, tenant: tenant_id)

    {:ok, execution} =
      ExecutionRecord.dispatch(
        %{
          installation_id: tenant_id,
          subject_id: work_object.id,
          recipe_ref: "triage_ticket",
          compiled_pack_revision: 1,
          binding_snapshot: %{"placement_ref" => "workspace_runtime"},
          dispatch_envelope: %{"capability" => "linear.issue.execute"},
          submission_dedupe_key: "#{tenant_id}:#{work_object.id}:triage_ticket:1",
          trace_id: trace_id,
          causation_id: "cause:#{trace_id}",
          actor_ref: %{kind: :scheduler}
        },
        actor: actor
      )

    {:ok, execution} =
      ExecutionRecord.record_accepted(
        execution,
        %{
          submission_ref: %{"status" => "accepted", "id" => "submission-#{trace_id}"},
          lower_receipt: %{"run_id" => "lower-run-#{trace_id}"},
          trace_id: trace_id,
          causation_id: "cause:#{trace_id}",
          actor_ref: %{kind: :dispatcher}
        },
        actor: actor
      )

    %{
      tenant_id: tenant_id,
      actor: actor,
      program: program,
      bundle: bundle,
      work_class: work_class,
      work_object: work_object,
      run: run,
      execution: execution
    }
  end

  defp workflow_body do
    """
    ---
    tracker:
      kind: linear
      endpoint: https://api.linear.app/graphql
    run:
      profile: default_session
      runtime_class: session
      capability: linear.issue.execute
      target: linear-default
    approval:
      mode: manual
      reviewers:
        - ops_lead
      escalation_required: true
    retry:
      strategy: exponential
      max_attempts: 4
      initial_backoff_ms: 5000
      max_backoff_ms: 300000
    placement:
      profile_id: default-placement
      strategy: affinity
      target_selector:
        runtime_driver: jido_session
      runtime_preferences:
        locality: same_region
    workspace:
      root_mode: per_work
      sandbox_profile: strict
    review:
      required: false
      required_decisions: 0
      gates: []
    capability_grants:
      - capability_id: linear.issue.read
        mode: allow
      - capability_id: linear.issue.update
        mode: allow
    ---
    # Operator Prompt
    """
  end
end
