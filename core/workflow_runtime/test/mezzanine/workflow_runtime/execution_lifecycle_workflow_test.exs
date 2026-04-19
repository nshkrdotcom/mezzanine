defmodule Mezzanine.WorkflowRuntime.ExecutionLifecycleWorkflowTest do
  use ExUnit.Case, async: false

  alias Mezzanine.WorkflowRuntime.ExecutionLifecycleWorkflow
  alias Mezzanine.Workflows.ExecutionAttempt

  defmodule QueryRuntime do
    @behaviour Mezzanine.WorkflowRuntime

    @impl true
    def start_workflow(_request), do: {:error, :not_used}

    @impl true
    def signal_workflow(_request), do: {:ok, %{dispatch_state: "delivered_to_temporal"}}

    @impl true
    def query_workflow(request) do
      {:ok,
       %{
         workflow_id: request.workflow_id,
         query_name: request.query_name,
         workflow_state: "awaiting_receipt",
         projection_state: "fresh",
         raw_temporalex_result: :forbidden
       }}
    end

    @impl true
    def cancel_workflow(_request), do: {:error, :not_used}

    @impl true
    def describe_workflow(_request), do: {:error, :not_used}

    @impl true
    def fetch_workflow_history_ref(_request), do: {:error, :not_used}
  end

  setup do
    previous = Application.get_env(:mezzanine_core, :workflow_runtime_impl)
    Application.put_env(:mezzanine_core, :workflow_runtime_impl, QueryRuntime)

    on_exit(fn ->
      if previous do
        Application.put_env(:mezzanine_core, :workflow_runtime_impl, previous)
      else
        Application.delete_env(:mezzanine_core, :workflow_runtime_impl)
      end
    end)
  end

  test "defines lifecycle contract and consumes the enterprise pre-cut envelope" do
    contract = ExecutionLifecycleWorkflow.contract()

    assert contract.workflow_module == Mezzanine.Workflows.ExecutionAttempt
    assert contract.workflow_contract == "Mezzanine.WorkflowExecutionLifecycleInput.v1"
    assert contract.receipt_signal_contract == "Mezzanine.WorkflowReceiptSignal.v1"
    assert contract.terminal_policy_contract == "Mezzanine.WorkflowTerminalReceiptPolicy.v1"
    assert :compile_citadel_authority in contract.activity_sequence
    assert :submit_jido_lower_run in contract.activity_sequence
    assert :persist_terminal_receipt in contract.activity_sequence

    assert {:ok, input} = ExecutionLifecycleWorkflow.new_input(lifecycle_attrs())
    assert input.workflow_type == "execution_attempt"
    assert input.permission_decision_ref == "decision-093"

    assert {:error, {:missing_required_fields, missing}} =
             ExecutionLifecycleWorkflow.new_input(
               Map.delete(lifecycle_attrs(), :authority_packet_ref)
             )

    assert :authority_packet_ref in missing
  end

  test "workflow run emits compact lifecycle history and no raw lower payloads" do
    assert {:ok, result} = ExecutionAttempt.run(lifecycle_attrs())

    assert result.workflow_state == "awaiting_receipt"
    assert result.workflow_id == "workflow-093"

    assert result.activity_refs == [
             "activity://workflow-093/compile-authority",
             "activity://workflow-093/submit-lower"
           ]

    assert result.lower_refs == ["lower-submission-093"]
    assert result.routing_facts.review_required == false
    refute Map.has_key?(result, :raw_lower_payload)
    refute Map.has_key?(result, :temporalex_struct)
  end

  test "activities compile authority, submit lower work idempotently, and persist terminal receipts" do
    attrs = lifecycle_attrs()

    assert {:ok, authority} = ExecutionLifecycleWorkflow.compile_citadel_authority_activity(attrs)
    assert authority.owner_repo == :citadel
    assert authority.authority_packet_ref == "authpkt-093"
    assert authority.permission_decision_ref == "decision-093"

    assert {:ok, lower} = ExecutionLifecycleWorkflow.submit_jido_lower_run_activity(attrs)
    assert lower.owner_repo == :jido_integration
    assert lower.execution_plane_owner_repo == :execution_plane
    assert lower.lower_submission_ref == "lower-submission-093"
    assert lower.idempotency_key == "lower-idem-093"
    assert lower.lease_broker == Mezzanine.ActivityLeaseBroker

    assert {:ok, persisted} =
             ExecutionLifecycleWorkflow.persist_terminal_receipt_activity(
               Map.merge(attrs, %{
                 terminal_state: "completed",
                 terminal_event_ref: "workflow-event-terminal",
                 lower_receipt_ref: "lower-receipt-095"
               })
             )

    assert persisted.owner_repo == :mezzanine
    assert persisted.terminal_state == "completed"
    assert persisted.lower_receipt_ref == "lower-receipt-095"
  end

  test "receipt signals are tenant-scoped, idempotent, and late receipts are policy classified" do
    assert {:ok, signal} = ExecutionLifecycleWorkflow.receipt_signal(receipt_signal_attrs())

    assert signal.signal_name == "lower_receipt"
    assert signal.idempotency_key == "idem-signal-094"

    state = ExecutionLifecycleWorkflow.initial_state(lifecycle_attrs())
    assert {:ok, advanced} = ExecutionLifecycleWorkflow.apply_receipt_signal(state, signal)
    assert advanced.workflow_state == "completed"
    assert advanced.seen_signal_keys == MapSet.new(["idem-signal-094"])

    assert {:ok, duplicate} = ExecutionLifecycleWorkflow.apply_receipt_signal(advanced, signal)
    assert duplicate.workflow_state == "completed"
    assert duplicate.signal_state == "duplicate_suppressed"

    assert {:ok, late_policy} =
             ExecutionLifecycleWorkflow.terminal_receipt_policy(advanced, signal)

    assert late_policy.policy_result == "quarantined_late_receipt"
    assert late_policy.incident_ref == "incident://workflow-093/late-receipt/lower-receipt-094"
  end

  test "operator query goes through WorkflowRuntime and worker failover is replay-safe" do
    assert {:ok, query} = ExecutionLifecycleWorkflow.query_operator_state(lifecycle_attrs())

    assert query.workflow_id == "workflow-093"
    assert query.workflow_state == "awaiting_receipt"
    refute Map.has_key?(query, :raw_temporalex_result)

    assert {:ok, recovery} =
             ExecutionLifecycleWorkflow.worker_failover_recovery(lifecycle_attrs())

    assert recovery.recovery_mode == "temporal_replay"
    assert recovery.task_queue == "mezzanine.hazmat"
    assert recovery.stranded? == false
    assert recovery.idempotency_key == "idem-093"
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
      release_manifest_ref: "phase4-v6-milestone27-execution-lifecycle-workflow",
      retry_policy: %{maximum_attempts: 3},
      terminal_policy: "quarantine_late_receipts",
      routing_facts: %{review_required: false, risk_band: "low"}
    }
  end

  defp receipt_signal_attrs do
    %{
      tenant_ref: "tenant-acme",
      installation_ref: "installation-main",
      workspace_ref: "workspace-main",
      project_ref: "project-main",
      environment_ref: "env-prod",
      principal_ref: "principal-operator",
      system_actor_ref: "system-workflow",
      resource_ref: "resource-work-1",
      workflow_id: "workflow-093",
      workflow_run_id: "run-093",
      signal_id: "signal-094",
      signal_name: "lower_receipt",
      signal_version: "lower-receipt.v1",
      lower_receipt_ref: "lower-receipt-094",
      lower_run_ref: "lower-run-094",
      lower_attempt_ref: "lower-attempt-1",
      lower_event_ref: "lower-event-094",
      authority_packet_ref: "authpkt-094",
      permission_decision_ref: "decision-094",
      idempotency_key: "idem-signal-094",
      trace_id: "trace-094",
      correlation_id: "corr-094",
      release_manifest_ref: "phase4-v6-milestone27-execution-lifecycle-workflow",
      receipt_state: "completed",
      terminal?: true,
      routing_facts: %{terminal_class: "completed"}
    }
  end
end
