defmodule Mezzanine.Execution.LifecycleContinuationTest do
  use Mezzanine.Execution.DataCase, async: false

  alias Mezzanine.Execution.LifecycleContinuation

  @now ~U[2026-04-18 19:00:00Z]

  defmodule LockTimeoutDispatcher do
    def dispatch_lifecycle_continuation(_continuation, %{"kind" => "owner_command"}),
      do: {:error, :lock_timeout}
  end

  defmodule InvalidTransitionDispatcher do
    def dispatch_lifecycle_continuation(_continuation, %{"kind" => "owner_command"}),
      do: {:error, {:invalid_transition, "bad-target"}}
  end

  defmodule OkOwnerCommandDispatcher do
    def dispatch_lifecycle_continuation(_continuation, %{"kind" => "owner_command"}), do: :ok
  end

  defmodule OkWorkflowSignalDispatcher do
    def dispatch_lifecycle_continuation(_continuation, %{"kind" => "workflow_signal"}), do: :ok
  end

  test "anonymous handlers are rejected before a continuation is claimed" do
    continuation = continuation_fixture!("anonymous")

    assert {:error, :missing_lifecycle_continuation_dispatcher} =
             LifecycleContinuation.process(continuation.continuation_id,
               now: @now,
               handler: fn _continuation -> flunk("anonymous handler should not run") end
             )

    assert {:ok, fetched} = LifecycleContinuation.fetch(continuation.continuation_id)
    assert fetched.status == :pending
    assert fetched.attempt_count == 0
  end

  test "transient failures schedule retry with operator-visible diagnostics" do
    continuation = continuation_fixture!("transient")

    assert {:ok, retry} =
             LifecycleContinuation.process(continuation.continuation_id,
               now: @now,
               dispatcher: LockTimeoutDispatcher,
               backoff_ms: 10_000
             )

    assert retry.status == :retry_scheduled
    assert retry.attempt_count == 1
    assert retry.last_error_class == "transient_lock"
    [evidence] = retry.metadata["compensation_evidence"]
    assert evidence_field(evidence, "event_kind") in [:retry_scheduled, "retry_scheduled"]
    assert evidence_field(evidence, "attempt_number") == 1
    assert evidence_field(evidence, "max_attempts") == 3

    assert evidence_field(evidence, "dead_letter_ref") ==
             "dead-letter:lifecycle-continuation:continuation-transient"

    assert DateTime.compare(retry.next_attempt_at, DateTime.add(@now, 10_000, :millisecond)) ==
             :eq

    assert {:ok, [operator_row]} =
             LifecycleContinuation.list_operator("tenant-1", "installation-1")

    assert operator_row.continuation_id == continuation.continuation_id
    assert operator_row.last_error_class == "transient_lock"
  end

  test "invalid transitions dead-letter immediately" do
    continuation = continuation_fixture!("invalid")

    assert {:ok, dead_lettered} =
             LifecycleContinuation.process(continuation.continuation_id,
               now: @now,
               dispatcher: InvalidTransitionDispatcher
             )

    assert dead_lettered.status == :dead_lettered
    assert dead_lettered.attempt_count == 1
    assert dead_lettered.last_error_class == "invalid_transition"
    [evidence] = dead_lettered.metadata["compensation_evidence"]
    assert evidence_field(evidence, "event_kind") in [:dead_lettered, "dead_lettered"]
    assert evidence_field(evidence, "failure_class") == "invalid_transition"
    assert evidence_field(evidence, "owner_command_or_signal")["kind"] == "owner_command"
  end

  test "continuations without a declared target dead-letter instead of running dispatcher code" do
    continuation = continuation_fixture!("missing-target", metadata: %{})

    assert {:ok, dead_lettered} =
             LifecycleContinuation.process(continuation.continuation_id,
               now: @now,
               dispatcher: OkOwnerCommandDispatcher
             )

    assert dead_lettered.status == :dead_lettered
    assert dead_lettered.last_error_class == "invalid_transition"
    assert dead_lettered.last_error_message =~ "missing_lifecycle_continuation_target"
    [evidence] = dead_lettered.metadata["compensation_evidence"]
    assert evidence_field(evidence, "owner_command_or_signal")["kind"] == "invalid_target"
  end

  test "workflow signal targets are explicit declared dispatch targets" do
    continuation =
      continuation_fixture!("workflow-signal",
        metadata: %{
          "continuation_target" => %{
            "kind" => "workflow_signal",
            "workflow_id" => "workflow-execution-1",
            "signal" => "execution_completed",
            "idempotency_key" => "continuation-workflow-signal"
          }
        }
      )

    assert {:ok, completed} =
             LifecycleContinuation.process(continuation.continuation_id,
               now: @now,
               dispatcher: OkWorkflowSignalDispatcher
             )

    assert completed.status == :completed
  end

  test "operator retry moves one dead-lettered continuation back to pending and completes once" do
    continuation = continuation_fixture!("retry")

    assert {:ok, dead_lettered} =
             LifecycleContinuation.process(continuation.continuation_id,
               now: @now,
               dispatcher: InvalidTransitionDispatcher
             )

    assert dead_lettered.status == :dead_lettered

    assert {:error, {:missing_operator_action_evidence_fields, missing_fields}} =
             LifecycleContinuation.retry(dead_lettered.continuation_id,
               now: DateTime.add(@now, 1, :second)
             )

    assert :operator_action_ref in missing_fields
    assert :operator_actor_ref in missing_fields
    assert :authority_decision_ref in missing_fields

    assert {:ok, retryable} =
             LifecycleContinuation.retry(dead_lettered.continuation_id,
               now: DateTime.add(@now, 1, :second),
               operator_action_ref: "operator-action:retry:continuation-retry",
               operator_actor_ref: "actor:operator:retry",
               authority_decision_ref: "authority-decision:retry",
               safe_action: "retry owner command once",
               blast_radius: "lifecycle continuation row only"
             )

    assert retryable.status == :pending
    assert retryable.last_error_class == nil
    [_dead_letter, operator_evidence] = retryable.metadata["compensation_evidence"]

    assert evidence_field(operator_evidence, "event_kind") in [
             :operator_action,
             "operator_action"
           ]

    assert evidence_field(operator_evidence, "compensation_kind") in [
             :operator_retry,
             "operator_retry"
           ]

    assert evidence_field(operator_evidence, "operator_action_ref") ==
             "operator-action:retry:continuation-retry"

    assert {:ok, completed} =
             LifecycleContinuation.process(retryable.continuation_id,
               now: DateTime.add(@now, 2, :second),
               dispatcher: OkOwnerCommandDispatcher
             )

    assert completed.status == :completed

    assert {:ok, :already_completed} =
             LifecycleContinuation.process(completed.continuation_id,
               now: DateTime.add(@now, 3, :second),
               dispatcher: OkOwnerCommandDispatcher
             )
  end

  test "operator waive requires operator action evidence before completion" do
    continuation = continuation_fixture!("waive")

    assert {:error, {:missing_operator_action_evidence_fields, missing_fields}} =
             LifecycleContinuation.waive(continuation.continuation_id,
               now: DateTime.add(@now, 1, :second)
             )

    assert :operator_action_ref in missing_fields

    assert {:ok, waived} =
             LifecycleContinuation.waive(continuation.continuation_id,
               now: DateTime.add(@now, 2, :second),
               reason: "operator accepted external completion evidence",
               operator_action_ref: "operator-action:waive:continuation-waive",
               operator_actor_ref: "actor:operator:waive",
               authority_decision_ref: "authority-decision:waive",
               safe_action: "waive continuation only",
               blast_radius: "lifecycle continuation row only"
             )

    assert waived.status == :completed
    assert waived.last_error_class == "operator_waived"
    [operator_evidence] = waived.metadata["compensation_evidence"]

    assert evidence_field(operator_evidence, "event_kind") in [
             :operator_action,
             "operator_action"
           ]

    assert evidence_field(operator_evidence, "compensation_kind") in [
             :operator_waive,
             "operator_waive"
           ]

    assert evidence_field(operator_evidence, "operator_action_ref") ==
             "operator-action:waive:continuation-waive"
  end

  defp continuation_fixture!(suffix, opts \\ []) do
    {:ok, continuation} =
      LifecycleContinuation.enqueue(%{
        continuation_id: "continuation-#{suffix}",
        tenant_id: "tenant-1",
        installation_id: "installation-1",
        subject_id: Ecto.UUID.generate(),
        execution_id: Ecto.UUID.generate(),
        from_state: "processing",
        target_transition: "execution_completed:expense_capture",
        next_attempt_at: @now,
        trace_id: "trace-#{suffix}",
        status: :pending,
        actor_ref: %{"kind" => "test"},
        metadata: Keyword.get(opts, :metadata, owner_command_metadata(suffix))
      })

    continuation
  end

  defp owner_command_metadata(suffix) do
    %{
      "continuation_target" => %{
        "kind" => "owner_command",
        "owner" => "object_lifecycle",
        "command" => "advance_after_execution",
        "idempotency_key" => "continuation-#{suffix}"
      }
    }
  end

  defp evidence_field(evidence, field),
    do: Map.get(evidence, field) || Map.get(evidence, evidence_field_key(field))

  defp evidence_field_key("attempt_ref"), do: :attempt_ref
  defp evidence_field_key("attempt_number"), do: :attempt_number
  defp evidence_field_key("compensation_kind"), do: :compensation_kind
  defp evidence_field_key("dead_letter_ref"), do: :dead_letter_ref
  defp evidence_field_key("event_kind"), do: :event_kind
  defp evidence_field_key("failure_class"), do: :failure_class
  defp evidence_field_key("failure_reason"), do: :failure_reason
  defp evidence_field_key("max_attempts"), do: :max_attempts
  defp evidence_field_key("operator_action_ref"), do: :operator_action_ref
  defp evidence_field_key("owner_command_or_signal"), do: :owner_command_or_signal
  defp evidence_field_key("source_event_ref"), do: :source_event_ref
  defp evidence_field_key(_field), do: nil
end
