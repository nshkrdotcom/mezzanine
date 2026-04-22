defmodule Mezzanine.WorkflowRuntime.OperatorSignalControlTest do
  use ExUnit.Case, async: false

  alias Mezzanine.WorkflowRuntime.OperatorSignalControl
  alias Mezzanine.WorkflowRuntime.WorkflowSignalOutboxWorker
  alias Mezzanine.Workflows.DecisionReview

  defmodule SignalRuntime do
    @behaviour Mezzanine.WorkflowRuntime

    @impl true
    def start_workflow(_request), do: {:error, :not_used}

    @impl true
    def signal_workflow(request) do
      {:ok,
       %{
         signal_ref: "temporal-signal://#{request.workflow_id}/#{request.signal_id}",
         status: "delivered_to_temporal",
         dispatch_state: "delivered_to_temporal",
         trace_id: request.trace_id,
         raw_temporalex_result: :forbidden
       }}
    end

    @impl true
    def query_workflow(_request), do: {:error, :not_used}

    @impl true
    def cancel_workflow(_request), do: {:error, :not_used}

    @impl true
    def describe_workflow(_request), do: {:error, :not_used}

    @impl true
    def fetch_workflow_history_ref(_request), do: {:error, :not_used}
  end

  defmodule RecordingOutboxStore do
    @behaviour Mezzanine.WorkflowRuntime.OutboxPersistence

    @impl true
    def record_start_outcome(_original_row, _outcome_row), do: {:error, :not_used}

    @impl true
    def record_signal_outcome(original_row, outcome_row) do
      send(self(), {:record_signal_outcome, original_row, outcome_row})
      :ok
    end
  end

  defmodule FailingOutboxStore do
    @behaviour Mezzanine.WorkflowRuntime.OutboxPersistence

    @impl true
    def record_start_outcome(_original_row, _outcome_row), do: {:error, :not_used}

    @impl true
    def record_signal_outcome(_original_row, _outcome_row), do: {:error, :store_down}
  end

  setup do
    previous = Application.get_env(:mezzanine_core, :workflow_runtime_impl)
    previous_outbox = Application.get_env(:mezzanine_workflow_runtime, :outbox_persistence)

    Application.put_env(:mezzanine_core, :workflow_runtime_impl, SignalRuntime)

    Application.put_env(:mezzanine_workflow_runtime, :outbox_persistence,
      store: RecordingOutboxStore
    )

    on_exit(fn ->
      if previous do
        Application.put_env(:mezzanine_core, :workflow_runtime_impl, previous)
      else
        Application.delete_env(:mezzanine_core, :workflow_runtime_impl)
      end

      if previous_outbox do
        Application.put_env(:mezzanine_workflow_runtime, :outbox_persistence, previous_outbox)
      else
        Application.delete_env(:mezzanine_workflow_runtime, :outbox_persistence)
      end
    end)
  end

  test "decision review workflow schedules durable timer semantics without Oban expiry jobs" do
    assert {:ok, result} = DecisionReview.run(timer_attrs())

    assert result.workflow_state == "awaiting_decision_or_timer"
    assert result.timer_state == "scheduled"
    assert result.timer_ref == "workflow-timer://workflow-096/timer-096"
    assert result.history_policy == "temporal_timer_history"
    refute Map.has_key?(result, :oban_job_id)
    refute Map.has_key?(result, :expiry_job_id)
  end

  test "operator signal registry covers every M28 action and rejects unregistered signal names" do
    registered =
      OperatorSignalControl.signal_registry()
      |> Enum.map(&{&1.signal_name, &1.signal_version})

    assert {"operator.cancel", "operator-cancel.v1"} in registered
    assert {"operator.pause", "operator-pause.v1"} in registered
    assert {"operator.resume", "operator-resume.v1"} in registered
    assert {"operator.retry", "operator-retry.v1"} in registered
    assert {"operator.replan", "operator-replan.v1"} in registered

    assert OperatorSignalControl.registered_signal?("operator.cancel", "operator-cancel.v1")
    refute OperatorSignalControl.registered_signal?("operator.cancel", "bad-version")
  end

  test "decision review workflow rejects unversioned operator signals without deriving old shape" do
    payload =
      signal_attrs()
      |> Map.delete(:signal_name)
      |> Map.delete(:signal_version)

    state = OperatorSignalControl.initial_ordering_state()

    assert {:noreply, next_state} =
             DecisionReview.handle_signal("operator.cancel", payload, state)

    assert next_state.workflow_mode == "running"
    assert next_state.ordering_state == "ready"
    assert next_state.last_signal_error == {:missing_required_fields, [:signal_version]}
  end

  test "authorized cancel persists local receipt/outbox before dispatch and never claims workflow effect early" do
    assert {:ok, accepted} = OperatorSignalControl.accept_operator_signal(signal_attrs())

    assert accepted.signal.signal_name == "operator.cancel"
    assert accepted.receipt.authority_state == "authorized"
    assert accepted.receipt.local_state == "accepted"
    assert accepted.receipt.dispatch_state == "queued"
    assert accepted.receipt.workflow_effect_state == "pending"
    assert accepted.receipt.projection_state == "lagging"
    assert accepted.outbox.dispatch_state == "queued"

    assert {:ok, delivered} = OperatorSignalControl.dispatch_operator_signal(accepted)

    assert delivered.receipt.dispatch_state == "delivered_to_temporal"
    assert delivered.receipt.workflow_effect_state == "pending"
    assert delivered.receipt.projection_state == "lagging"
    refute Map.has_key?(delivered.runtime_receipt, :raw_temporalex_result)

    assert {:ok, acked} =
             OperatorSignalControl.apply_workflow_ack(delivered.receipt, %{
               acknowledged_at: "2026-04-18T12:00:02Z",
               workflow_event_ref: "workflow-event://workflow-097/signal-097/ack"
             })

    assert acked.receipt.workflow_effect_state == "processed_by_workflow"
    assert acked.receipt.projection_state == "fresh"
    assert acked.ack.signal_effect == "cancel_requested"
  end

  test "retained signal worker persists Temporal outcome before acking" do
    assert {:ok, accepted} = OperatorSignalControl.accept_operator_signal(signal_attrs())

    assert :ok =
             WorkflowSignalOutboxWorker.perform(%Oban.Job{
               args: Map.from_struct(accepted.outbox)
             })

    assert_received {:record_signal_outcome, original_row, outcome_row}
    assert original_row.outbox_id == accepted.outbox.outbox_id
    assert outcome_row.dispatch_state == "delivered_to_temporal"
    assert outcome_row.workflow_effect_state == "pending_ack"
    assert outcome_row.projection_state == "pending"
    assert outcome_row.dispatch_attempt_count == 1
  end

  test "retained signal worker does not ack when outcome persistence fails" do
    Application.put_env(:mezzanine_workflow_runtime, :outbox_persistence,
      store: FailingOutboxStore
    )

    assert {:ok, accepted} = OperatorSignalControl.accept_operator_signal(signal_attrs())

    assert {:error, {:outbox_outcome_not_persisted, :store_down}} =
             WorkflowSignalOutboxWorker.perform(%Oban.Job{
               args: Map.from_struct(accepted.outbox)
             })
  end

  test "unauthorized signal is denied before outbox dispatch" do
    attrs = Map.merge(signal_attrs(), %{permission_decision_result: "deny"})

    assert {:ok, denied} = OperatorSignalControl.accept_operator_signal(attrs)
    assert denied.receipt.authority_state == "denied"
    assert denied.receipt.local_state == "rejected"
    assert denied.receipt.dispatch_state == "not_dispatched"
    assert denied.receipt.workflow_effect_state == "rejected_by_authority"
    refute Map.has_key?(denied, :outbox)
  end

  test "duplicate pause/resume signals are sequence-safe and idempotent" do
    state = OperatorSignalControl.initial_ordering_state()

    assert {:ok, paused} =
             OperatorSignalControl.apply_ordered_signal(
               state,
               Map.merge(signal_attrs(), %{
                 signal_id: "signal-098-pause",
                 signal_name: "operator.pause",
                 signal_version: "operator-pause.v1",
                 signal_sequence: 1,
                 idempotency_key: "idem-signal-098-pause"
               })
             )

    assert paused.workflow_mode == "paused"
    assert paused.last_signal_sequence == 1

    assert {:ok, duplicate} =
             OperatorSignalControl.apply_ordered_signal(
               paused,
               Map.merge(signal_attrs(), %{
                 signal_id: "signal-098-pause",
                 signal_name: "operator.pause",
                 signal_version: "operator-pause.v1",
                 signal_sequence: 1,
                 idempotency_key: "idem-signal-098-pause"
               })
             )

    assert duplicate.workflow_mode == "paused"
    assert duplicate.ordering_state == "duplicate_suppressed"

    assert {:ok, resumed} =
             OperatorSignalControl.apply_ordered_signal(
               duplicate,
               Map.merge(signal_attrs(), %{
                 signal_id: "signal-098-resume",
                 signal_name: "operator.resume",
                 signal_version: "operator-resume.v1",
                 signal_sequence: 2,
                 idempotency_key: "idem-signal-098-resume"
               })
             )

    assert resumed.workflow_mode == "running"
    assert resumed.last_signal_sequence == 2
  end

  test "bounded wait reads projection state and stale Temporal delivery never renders complete" do
    assert {:ok, result} =
             OperatorSignalControl.operator_signal_result(%{
               command_id: "cmd-097",
               signal_id: "signal-097",
               workflow_ref: "workflow://workflow-097/run-097",
               tenant_ref: "tenant-acme",
               installation_ref: "installation-main",
               operator_ref: "operator-1",
               resource_ref: "resource-work-1",
               authority_packet_ref: "authpkt-097",
               permission_decision_ref: "decision-097",
               idempotency_key: "idem-signal-097",
               authority_state: "authorized",
               local_state: "accepted",
               dispatch_state: "delivered_to_temporal",
               workflow_effect_state: "pending",
               projection_state: "stale",
               trace_id: "trace-097",
               correlation_id: "corr-097",
               release_manifest_version: "phase4.v6.milestone28",
               incident_bundle_ref: "incident://workflow-097/signal-097/stale"
             })

    assert result.workflow_effect_state == "pending"
    assert result.projection_state == "stale"
    assert result.operator_message =~ "pending"
    refute Map.has_key?(result, :temporal_query_result)
  end

  defp timer_attrs do
    %{
      tenant_ref: "tenant-acme",
      installation_ref: "installation-main",
      system_actor_ref: "system-workflow",
      resource_ref: "resource-work-1",
      subject_ref: "subject-096",
      workflow_id: "workflow-096",
      workflow_run_id: "run-096",
      decision_id: "decision-096",
      decision_kind: "operator_review",
      timer_id: "timer-096",
      timer_version: "decision-timer.v1",
      timer_duration_ms: 300_000,
      expires_at: "2026-04-18T12:05:00Z",
      authority_packet_ref: "authpkt-096",
      permission_decision_ref: "decision-auth-096",
      idempotency_key: "idem-timer-096",
      trace_id: "trace-096",
      correlation_id: "corr-096",
      release_manifest_ref: "phase4-v6-milestone28",
      workflow_history_ref: "temporal-history://workflow-096/timer-096",
      projection_ref: "projection://workflow-096/decision-timer",
      timer_state: "scheduled"
    }
  end

  defp signal_attrs do
    %{
      tenant_ref: "tenant-acme",
      installation_ref: "installation-main",
      workspace_ref: "workspace-main",
      project_ref: "project-main",
      environment_ref: "env-prod",
      principal_ref: "principal-operator",
      operator_ref: "operator-1",
      resource_ref: "resource-work-1",
      workflow_id: "workflow-097",
      workflow_run_id: "run-097",
      signal_id: "signal-097",
      signal_name: "operator.cancel",
      signal_version: "operator-cancel.v1",
      signal_sequence: 1,
      signal_effect: "cancel_requested",
      authority_packet_ref: "authpkt-097",
      permission_decision_ref: "decision-097",
      permission_decision_result: "allow",
      idempotency_key: "idem-signal-097",
      trace_id: "trace-097",
      correlation_id: "corr-097",
      release_manifest_ref: "phase4-v6-milestone28",
      acknowledgement_ttl_ms: 30_000,
      reason: "operator requested cancel",
      payload_hash: String.duplicate("d", 64),
      payload_ref: "claim://operator-signal/097"
    }
  end
end
