defmodule Mezzanine.Execution.LifecycleContinuationTest do
  use Mezzanine.Execution.DataCase, async: false

  alias Mezzanine.Execution.LifecycleContinuation

  @now ~U[2026-04-18 19:00:00Z]

  test "transient failures schedule retry with operator-visible diagnostics" do
    continuation = continuation_fixture!("transient")

    assert {:ok, retry} =
             LifecycleContinuation.process(continuation.continuation_id,
               now: @now,
               handler: fn _continuation -> {:error, :lock_timeout} end,
               backoff_ms: 10_000
             )

    assert retry.status == :retry_scheduled
    assert retry.attempt_count == 1
    assert retry.last_error_class == "transient_lock"

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
               handler: fn _continuation -> {:error, {:invalid_transition, "bad-target"}} end
             )

    assert dead_lettered.status == :dead_lettered
    assert dead_lettered.attempt_count == 1
    assert dead_lettered.last_error_class == "invalid_transition"
  end

  test "operator retry moves one dead-lettered continuation back to pending and completes once" do
    continuation = continuation_fixture!("retry")

    assert {:ok, dead_lettered} =
             LifecycleContinuation.process(continuation.continuation_id,
               now: @now,
               handler: fn _continuation -> {:error, :invalid_transition} end
             )

    assert dead_lettered.status == :dead_lettered

    assert {:ok, retryable} =
             LifecycleContinuation.retry(dead_lettered.continuation_id,
               now: DateTime.add(@now, 1, :second)
             )

    assert retryable.status == :pending
    assert retryable.last_error_class == nil

    assert {:ok, completed} =
             LifecycleContinuation.process(retryable.continuation_id,
               now: DateTime.add(@now, 2, :second),
               handler: fn _continuation -> :ok end
             )

    assert completed.status == :completed

    assert {:ok, :already_completed} =
             LifecycleContinuation.process(completed.continuation_id,
               now: DateTime.add(@now, 3, :second),
               handler: fn _continuation -> flunk("completed continuation processed twice") end
             )
  end

  defp continuation_fixture!(suffix) do
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
        actor_ref: %{"kind" => "test"}
      })

    continuation
  end
end
