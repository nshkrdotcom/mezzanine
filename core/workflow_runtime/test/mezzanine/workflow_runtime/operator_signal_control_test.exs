defmodule Mezzanine.WorkflowRuntime.OperatorSignalControlTest do
  use ExUnit.Case, async: false

  alias Mezzanine.WorkflowRuntime.ControlSignalProtocol
  alias Mezzanine.WorkflowRuntime.RecoveryControl
  alias Mezzanine.WorkflowRuntime.RecoverySignalDispatcher
  alias Mezzanine.Workflows.DecisionReview

  defmodule Store do
    def claim_signal_outboxes("signal-dispatcher-test", 1, opts) do
      send(Keyword.fetch!(opts, :test_pid), :claimed_committed_signal)

      {:ok,
       [
         %{
           outbox_ref: "outbox://mezzanine/control/test",
           dispatch_fence: 2,
           signal_payload: %{
             "workflow_id" => "workflow://temporal/run-control",
             "signal_id" => "signal://mezzanine/control/test",
             "signal_name" => "operator.pause",
             "signal_version" => "operator-pause.v1",
             "idempotency_key" => "pause:test"
           }
         }
       ]}
    end

    def complete_signal_outbox(outbox_ref, fence, outcome, opts) do
      send(
        Keyword.fetch!(opts, :test_pid),
        {:signal_outcome_persisted, outbox_ref, fence, outcome}
      )

      {:ok, %{outbox_ref: outbox_ref, state: "delivered"}}
    end
  end

  defmodule Runtime do
    def signal_workflow(request) do
      send(
        Application.fetch_env!(:mezzanine_workflow_runtime, :signal_dispatcher_test_pid),
        {:temporal_signal, request}
      )

      {:ok, %{status: "delivered_to_temporal"}}
    end
  end

  test "decision review uses Temporal history state and rejects unversioned signals" do
    assert {:ok, result} = DecisionReview.run(timer_attrs())
    assert result.workflow_state == "awaiting_decision_or_timer"
    assert result.history_policy == "temporal_timer_history"

    state = ControlSignalProtocol.initial_workflow_state()
    payload = signal_attrs() |> Map.delete(:signal_name) |> Map.delete(:signal_version)

    assert {:noreply, next_state} =
             DecisionReview.handle_signal("operator.cancel", payload, state)

    assert next_state.workflow_mode == "running"
    assert next_state.last_signal_error == {:unregistered_signal, "operator.cancel", nil}
  end

  test "versioned pause, resume, retry, and supersession are deterministic and idempotent" do
    registered =
      ControlSignalProtocol.registry()
      |> Enum.map(&{&1.signal_name, &1.signal_version})

    assert {"operator.pause", "operator-pause.v1"} in registered
    assert {"operator.resume", "operator-resume.v1"} in registered
    assert {"operator.retry", "operator-retry.v1"} in registered
    assert {"operator.supersede", "operator-supersede.v1"} in registered

    assert {:ok, paused} =
             ControlSignalProtocol.reduce_signal(
               ControlSignalProtocol.initial_workflow_state(),
               signal_attrs("operator.pause", "operator-pause.v1", 1, "pause")
             )

    assert paused.workflow_mode == "paused"

    assert {:ok, duplicate} =
             ControlSignalProtocol.reduce_signal(
               paused,
               signal_attrs("operator.pause", "operator-pause.v1", 1, "pause")
             )

    assert duplicate.ordering_state == "duplicate_suppressed"

    assert {:ok, resumed} =
             ControlSignalProtocol.reduce_signal(
               duplicate,
               signal_attrs("operator.resume", "operator-resume.v1", 2, "resume")
             )

    assert resumed.workflow_mode == "running"

    assert {:ok, superseded} =
             ControlSignalProtocol.reduce_signal(
               resumed,
               signal_attrs(
                 "operator.supersede",
                 "operator-supersede.v1",
                 3,
                 "supersede"
               )
             )

    assert superseded.workflow_mode == "supersede_requested"
  end

  test "only the post-commit dispatcher can call the Temporal signal boundary" do
    Application.put_env(
      :mezzanine_workflow_runtime,
      :signal_dispatcher_test_pid,
      self()
    )

    on_exit(fn ->
      Application.delete_env(:mezzanine_workflow_runtime, :signal_dispatcher_test_pid)
    end)

    start_supervised!(
      {RecoverySignalDispatcher,
       name: :signal_dispatcher_test,
       schedule?: false,
       store: Store,
       runtime: Runtime,
       lock_owner: "signal-dispatcher-test",
       batch_size: 1,
       store_opts: [test_pid: self()]}
    )

    assert :ok = RecoverySignalDispatcher.dispatch_once(:signal_dispatcher_test)
    assert_received :claimed_committed_signal
    assert_received {:temporal_signal, request}
    assert request["signal_name"] == "operator.pause"

    assert_received {:signal_outcome_persisted, "outbox://mezzanine/control/test", 2,
                     {:ok, %{status: "delivered_to_temporal"}}}

    refute function_exported?(RecoveryControl, :dispatch_operator_signal, 1)
  end

  defp timer_attrs do
    %{
      tenant_ref: "tenant://acme",
      installation_ref: "installation://acme/main",
      system_actor_ref: "actor://mezzanine/system",
      resource_ref: "run://mezzanine/review",
      subject_ref: "subject://synapse/review",
      workflow_id: "workflow://temporal/review",
      workflow_run_id: "temporal-run-review",
      decision_id: "decision://citadel/review",
      decision_kind: "operator_review",
      timer_id: "timer-review",
      timer_version: "decision-timer.v1",
      timer_duration_ms: 300_000,
      expires_at: "2026-08-01T00:05:00Z",
      authority_packet_ref: "authority://citadel/review",
      permission_decision_ref: "decision://citadel/review",
      idempotency_key: "review:timer",
      trace_id: "trace://synapse/review",
      correlation_id: "correlation://synapse/review",
      release_manifest_ref: "release://nshkr/mezzanine-recovery-control-v1",
      workflow_history_ref: "temporal-history://review",
      projection_ref: "projection://mezzanine/review",
      timer_state: "scheduled"
    }
  end

  defp signal_attrs, do: signal_attrs("operator.cancel", "operator-cancel.v1", 1, "cancel")

  defp signal_attrs(name, version, sequence, suffix) do
    %{
      tenant_ref: "tenant://acme",
      installation_ref: "installation://acme/main",
      principal_ref: "actor://synapse/operator",
      operator_ref: "operator://synapse/one",
      resource_ref: "run://mezzanine/control",
      workflow_id: "workflow://temporal/run-control",
      signal_id: "signal://mezzanine/control/#{suffix}",
      signal_name: name,
      signal_version: version,
      signal_sequence: sequence,
      authority_packet_ref: "authority://citadel/control",
      permission_decision_ref: "decision://citadel/control",
      idempotency_key: "control:#{suffix}",
      trace_id: "trace://synapse/control",
      correlation_id: "correlation://synapse/control",
      release_manifest_ref: "release://nshkr/mezzanine-recovery-control-v1",
      acknowledgement_ttl_ms: 30_000,
      payload_hash: "sha256:" <> String.duplicate("d", 64),
      payload_ref: "artifact://mezzanine/control/#{suffix}"
    }
  end
end
