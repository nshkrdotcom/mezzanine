defmodule Mezzanine.WorkflowRuntime.ExecutionLifecycleWorkflowTest do
  use ExUnit.Case, async: false
  use Temporalex.Testing

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
         workflow_state: "accepted_active",
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
    assert :cleanup_workspace in contract.activity_sequence
    assert :publish_source in contract.activity_sequence
    assert :materialize_evidence in contract.activity_sequence
    assert :create_review in contract.activity_sequence
    assert contract.activity_owners.cleanup_workspace == :mezzanine
    assert contract.activity_owners.publish_source == :jido_integration
    assert contract.activity_owners.materialize_evidence == :mezzanine
    assert contract.activity_owners.create_review == :mezzanine

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
    assert {:ok, result} =
             run_workflow(ExecutionAttempt, lifecycle_attrs(),
               activities: %{
                 Mezzanine.Activities.RequestDecision =>
                   &ExecutionLifecycleWorkflow.compile_citadel_authority_activity/1,
                 Mezzanine.Activities.StartLowerExecution =>
                   &ExecutionLifecycleWorkflow.submit_jido_lower_run_activity/1
               }
             )

    assert result.workflow_state == "accepted_active"
    assert result.workflow_id == "workflow-093"

    assert result.activity_refs == [
             "activity://workflow-093/compile-authority",
             "activity://workflow-093/submit-lower"
           ]

    assert result.lower_refs == ["lower-submission-093"]
    assert result.routing_facts.review_required == false
    refute Map.has_key?(result, :raw_lower_payload)
    refute Map.has_key?(result, :temporalex_struct)

    assert_activity_called(Mezzanine.Activities.RequestDecision)
    assert_activity_called(Mezzanine.Activities.StartLowerExecution)
    assert get_workflow_state().workflow_state == "accepted_active"
  end

  test "execution attempt can stay active until lower receipt for restart replay proof" do
    assert {:ok, result} =
             run_workflow(ExecutionAttempt, Map.put(lifecycle_attrs(), :hold_for_receipt?, true),
               activities: %{
                 Mezzanine.Activities.RequestDecision =>
                   &ExecutionLifecycleWorkflow.compile_citadel_authority_activity/1,
                 Mezzanine.Activities.StartLowerExecution =>
                   &ExecutionLifecycleWorkflow.submit_jido_lower_run_activity/1,
                 Mezzanine.Activities.RecordEvidence =>
                   &ExecutionLifecycleWorkflow.persist_terminal_receipt_activity/1,
                 Mezzanine.Activities.CleanupWorkspace =>
                   &ExecutionLifecycleWorkflow.cleanup_workspace_activity/1,
                 Mezzanine.Activities.PublishSource =>
                   &ExecutionLifecycleWorkflow.publish_source_activity/1,
                 Mezzanine.Activities.MaterializeEvidence =>
                   &ExecutionLifecycleWorkflow.materialize_evidence_activity/1,
                 Mezzanine.Activities.CreateReview =>
                   &ExecutionLifecycleWorkflow.create_review_activity/1
               },
               signals: [
                 {"lower_receipt",
                  %{lower_receipt_ref: "lower-receipt-096", signal_id: "signal-096"}}
               ]
             )

    assert result.workflow_state == "completed"
    assert result.signal_state == "accepted"
    assert result.last_receipt_ref == "lower-receipt-096"
    assert result.replay_resume_mode == "temporal_signal_resume"

    assert result.terminal_activity_refs == [
             "activity://workflow-093/persist-terminal-receipt",
             "activity://workflow-093/cleanup-workspace",
             "activity://workflow-093/publish-source",
             "activity://workflow-093/materialize-evidence",
             "activity://workflow-093/create-review"
           ]

    assert result.terminal_refs.workspace_cleanup_ref ==
             "workspace-cleanup://workflow-093/workspace-main"

    assert result.terminal_refs.source_publish_ref ==
             "source-publish://workflow-093/resource-work-1"

    assert result.terminal_refs.evidence_ref == "evidence://workflow-093/lower-receipt-096"
    assert result.terminal_refs.review_ref == "review://workflow-093/lower-receipt-096"
    refute Map.has_key?(result, :raw_temporalex_result)
    refute Map.has_key?(result, :raw_history_event)
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

    terminal_attrs =
      Map.merge(attrs, %{
        terminal_state: "completed",
        terminal_event_ref: "workflow-event-terminal",
        lower_receipt_ref: "lower-receipt-095"
      })

    assert {:ok, cleanup} = ExecutionLifecycleWorkflow.cleanup_workspace_activity(terminal_attrs)
    assert cleanup.owner_repo == :mezzanine
    assert cleanup.workspace_ref == "workspace-main"
    assert cleanup.result_ref == "workspace-cleanup://workflow-093/workspace-main"

    assert {:ok, publish} = ExecutionLifecycleWorkflow.publish_source_activity(terminal_attrs)
    assert publish.owner_repo == :jido_integration
    assert publish.source_publish_ref == "source-publish://workflow-093/resource-work-1"

    assert {:ok, evidence} =
             ExecutionLifecycleWorkflow.materialize_evidence_activity(terminal_attrs)

    assert evidence.owner_repo == :mezzanine
    assert evidence.evidence_ref == "evidence://workflow-093/lower-receipt-095"

    assert {:ok, review} = ExecutionLifecycleWorkflow.create_review_activity(terminal_attrs)
    assert review.owner_repo == :mezzanine
    assert review.review_ref == "review://workflow-093/lower-receipt-095"
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

  test "receipt signal delivery goes only through WorkflowRuntime and records a local receipt" do
    assert {:ok, delivered} =
             ExecutionLifecycleWorkflow.deliver_receipt_signal(receipt_signal_attrs())

    assert delivered.signal.signal_name == "lower_receipt"
    assert delivered.runtime_receipt.dispatch_state == "delivered_to_temporal"
    assert delivered.signal_receipt.dispatch_state == "delivered_to_temporal"
    assert delivered.signal_receipt.workflow_effect_state == "pending_ack"
    assert delivered.signal_receipt.projection_state == "pending"
    refute Map.has_key?(delivered.runtime_receipt, :raw_temporalex_result)
    refute Map.has_key?(delivered.runtime_receipt, :task_token)
  end

  test "operator query goes through WorkflowRuntime and worker failover is replay-safe" do
    assert {:ok, query} = ExecutionLifecycleWorkflow.query_operator_state(lifecycle_attrs())

    assert query.workflow_id == "workflow-093"
    assert query.workflow_state == "accepted_active"
    refute Map.has_key?(query, :raw_temporalex_result)

    assert {:ok, recovery} =
             ExecutionLifecycleWorkflow.worker_failover_recovery(lifecycle_attrs())

    assert recovery.recovery_mode == "temporal_replay"
    assert recovery.task_queue == "mezzanine.hazmat"
    assert recovery.stranded? == false
    assert recovery.idempotency_key == "idem-093"
  end

  test "turn-loop policy covers max turns, stall timeout, non-interactive blocks, and continuation" do
    assert policy =
             ExecutionLifecycleWorkflow.execution_control_policy(
               Map.merge(lifecycle_attrs(), %{max_turns: 2, stall_timeout_ms: 60_000})
             )

    assert policy.retry_policy == %{max_attempts: 3}
    assert policy.max_turns == 2
    assert policy.stall_timeout_ms == 60_000
    assert "operator.rework" in policy.operator_signals

    assert {:stop, %{reason: :max_turns_reached, safe_action: :finalize_or_review}} =
             ExecutionLifecycleWorkflow.turn_loop_decision(%{turn_count: 2, max_turns: 2})

    assert {:retry, %{reason: :stall_timeout, safe_action: :retry_or_cancel}} =
             ExecutionLifecycleWorkflow.turn_loop_decision(%{
               stalled?: true,
               stall_timeout_ms: 60_000
             })

    assert {:blocked, %{reason: :input_required, safe_action: :operator_review}} =
             ExecutionLifecycleWorkflow.turn_loop_decision(%{receipt_state: "input_required"})

    assert {:failure, %{reason: :approval_required, safe_action: :operator_review}} =
             ExecutionLifecycleWorkflow.turn_loop_decision(%{receipt_state: "approval_required"})

    assert {:finalize, %{reason: :source_terminal, safe_action: :terminal_cleanup}} =
             ExecutionLifecycleWorkflow.turn_loop_decision(%{source_state: "terminal"})

    assert {:continue, %{reason: :active_state_continuation, safe_action: :next_turn}} =
             ExecutionLifecycleWorkflow.turn_loop_decision(%{
               source_state: "active",
               turn_count: 1
             })
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
      retry_policy: %{max_attempts: 3},
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
