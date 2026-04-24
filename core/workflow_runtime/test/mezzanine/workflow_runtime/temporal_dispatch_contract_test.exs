defmodule Mezzanine.WorkflowRuntime.TemporalDispatchContractTest do
  use ExUnit.Case, async: false

  alias Mezzanine.WorkflowRuntime.TemporalDispatchContract
  alias Mezzanine.WorkflowRuntime.TemporalSupervisor
  alias Mezzanine.WorkflowRuntime.WorkflowStarterOutbox

  defmodule RecordingOutboxStore do
    @behaviour Mezzanine.WorkflowRuntime.OutboxPersistence

    @impl true
    def record_start_outcome(original_row, outcome_row) do
      send(self(), {:record_start_outcome, original_row, outcome_row})
      :ok
    end

    @impl true
    def record_signal_outcome(_original_row, _outcome_row), do: {:error, :not_used}
  end

  defmodule FailingOutboxStore do
    @behaviour Mezzanine.WorkflowRuntime.OutboxPersistence

    @impl true
    def record_start_outcome(_original_row, _outcome_row), do: {:error, :store_down}

    @impl true
    def record_signal_outcome(_original_row, _outcome_row), do: {:error, :not_used}
  end

  test "builds the TemporalDispatchContract restart and replay evidence from Mezzanine workers" do
    assert {:ok, outbox_row} = WorkflowStarterOutbox.new_row(outbox_attrs())

    assert {:ok, evidence} =
             TemporalDispatchContract.restart_replay_evidence(lifecycle_attrs(),
               outbox_row: outbox_row,
               worker_specs: worker_specs()
             )

    assert evidence.contract_id == "TemporalDispatchContract.v1"
    assert evidence.temporal_namespace == "default"
    assert evidence.workflow_runtime_boundary == Mezzanine.WorkflowRuntime
    assert evidence.temporal_adapter == Mezzanine.WorkflowRuntime.TemporalexAdapter
    assert evidence.p5p_residual_ref == "P5P-007"

    assert "temporal-task-queue://default/mezzanine.hazmat" in evidence.task_queue_refs
    assert "workflow://Mezzanine.Workflows.ExecutionAttempt" in evidence.workflow_type_refs

    assert %{
             workflow_module: Mezzanine.Workflows.ExecutionAttempt,
             task_queue: "mezzanine.hazmat",
             workflow_id: "workflow-093"
           } = evidence.execution_attempt_workflow_refs

    assert Enum.any?(evidence.worker_health_refs, fn ref ->
             ref.task_queue == "mezzanine.hazmat" and
               ref.execution_attempt_registered? and
               ref.status == "configured"
           end)

    assert evidence.active_workflow_state_before_restart_ref ==
             "temporal-query://workflow-093/operator_state.v1#accepted_active"

    assert evidence.restart_procedure_ref == "mezzanine-just://temporal-restart"

    assert evidence.replay_or_continuation_evidence_ref ==
             "temporal-replay://workflow-093/idempotency/idem-093"

    assert evidence.persisted_outcome_state_ref ==
             "workflow-start-outbox://outbox-093/started/run-093"

    assert evidence.compact_describe_query_refs == [
             "temporal-describe://workflow-093/run-093",
             "temporal-query://workflow-093/operator_state.v1"
           ]

    assert evidence.outbox_gc_only_posture ==
             "gc_allowed_only_after_temporal_outcome_persisted"

    refute evidence.raw_workflow_history_included?
    refute Map.has_key?(evidence, :raw_workflow_history)
    assert "just dev-up" in evidence.local_substrate_command_refs
    assert "just dev-status" in evidence.local_substrate_command_refs
    assert "just temporal-restart" in evidence.local_substrate_command_refs
  end

  test "fails closed when the ExecutionAttempt worker is not registered" do
    assert {:ok, outbox_row} = WorkflowStarterOutbox.new_row(outbox_attrs())

    worker_specs =
      worker_specs()
      |> Enum.reject(&(&1.task_queue == "mezzanine.hazmat"))

    assert {:error, {:missing_worker, "mezzanine.hazmat"}} =
             TemporalDispatchContract.restart_replay_evidence(lifecycle_attrs(),
               outbox_row: outbox_row,
               worker_specs: worker_specs
             )
  end

  test "fails closed when the retained outbox start would dispatch to the wrong task queue" do
    assert {:ok, outbox_row} =
             outbox_attrs()
             |> Map.put(:workflow_type, "agent_run")
             |> Map.put(:workflow_version, "agent-run.v1")
             |> WorkflowStarterOutbox.new_row()

    assert {:error,
            {:wrong_task_queue, %{expected: "mezzanine.hazmat", got: "mezzanine.agentic"}}} =
             TemporalDispatchContract.restart_replay_evidence(lifecycle_attrs(),
               outbox_row: outbox_row,
               worker_specs: worker_specs()
             )
  end

  test "fails closed when Temporal outcome state is not persisted locally" do
    assert {:ok, outbox_row} = WorkflowStarterOutbox.new_row(outbox_attrs())

    outcome_row =
      Map.merge(Map.from_struct(outbox_row), %{
        dispatch_state: "started",
        workflow_run_id: "run-093"
      })

    assert {:ok, persisted_ref} =
             TemporalDispatchContract.persisted_outcome_state_ref(outbox_row, outcome_row,
               outbox_persistence: RecordingOutboxStore
             )

    assert persisted_ref == "workflow-start-outbox://outbox-093/started/run-093"
    assert_received {:record_start_outcome, _original_row, persisted_row}
    assert persisted_row.dispatch_state == "started"

    assert {:error, {:outbox_outcome_not_persisted, :store_down}} =
             TemporalDispatchContract.persisted_outcome_state_ref(outbox_row, outcome_row,
               outbox_persistence: FailingOutboxStore
             )
  end

  defp worker_specs do
    TemporalSupervisor.task_queue_specs(
      enabled?: true,
      address: "127.0.0.1:7233",
      namespace: "default",
      instance_base: Mezzanine.WorkflowRuntime.Phase6Temporal
    )
  end

  defp lifecycle_attrs do
    %{
      tenant_ref: "tenant-acme",
      installation_ref: "installation-main",
      workspace_ref: "workspace-main",
      project_ref: "project-main",
      environment_ref: "env-prod",
      principal_ref: "principal-operator",
      system_actor_ref: "system-workflow",
      resource_ref: "resource-work-1",
      subject_ref: "subject-093",
      workflow_id: "workflow-093",
      workflow_run_id: "run-093",
      workflow_type: "execution_attempt",
      workflow_version: "execution-attempt.v1",
      command_id: "cmd-093",
      command_receipt_ref: "command-receipt-093",
      workflow_input_ref: "claim://workflow-input/093",
      lower_submission_ref: "lower-submission-093",
      lower_idempotency_key: "lower-idem-093",
      activity_call_ref: "activity-call-093",
      authority_packet_ref: "authpkt-093",
      permission_decision_ref: "decision-093",
      idempotency_key: "idem-093",
      trace_id: "trace-093",
      correlation_id: "corr-093",
      release_manifest_ref: "phase6-m6-temporal-dispatch-contract",
      retry_policy: %{max_attempts: 3},
      terminal_policy: "quarantine_late_receipts",
      routing_facts: %{review_required: false, risk_band: "low"}
    }
  end

  defp outbox_attrs do
    %{
      outbox_id: "outbox-093",
      tenant_ref: "tenant-acme",
      installation_ref: "installation-main",
      principal_ref: "principal-operator",
      resource_ref: "resource-work-1",
      command_receipt_ref: "command-receipt-093",
      command_id: "cmd-093",
      workflow_type: "execution_attempt",
      workflow_id: "workflow-093",
      workflow_version: "execution-attempt.v1",
      workflow_input_version: "execution-attempt-input.v1",
      workflow_input_ref: "claim://workflow-input/093",
      authority_packet_ref: "authpkt-093",
      permission_decision_ref: "decision-093",
      idempotency_key: "idem-093",
      dedupe_scope: "tenant:tenant-acme/resource:resource-work-1",
      trace_id: "trace-093",
      correlation_id: "corr-093",
      release_manifest_ref: "phase6-m6-temporal-dispatch-contract",
      payload_hash: "sha256:workflow-input-093"
    }
  end
end
