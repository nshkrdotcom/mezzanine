defmodule Mezzanine.Planner.RetrySchedulerTest do
  use ExUnit.Case, async: true

  alias Mezzanine.Planner

  test "computes exponential backoff within max bounds" do
    profile = %{
      strategy: :exponential,
      max_attempts: 4,
      initial_backoff_ms: 5_000,
      max_backoff_ms: 300_000
    }

    now = ~U[2026-04-13 12:00:00Z]

    assert {:ok, %{attempt: 1, delay_ms: 5_000, due_at: due_at}} =
             Planner.next_retry(%{attempts: 0}, profile, now)

    assert due_at == DateTime.add(now, 5_000, :millisecond)

    assert {:ok, %{attempt: 4, delay_ms: 40_000}} =
             Planner.next_retry(%{attempts: 3}, profile, now)
  end

  test "returns retry exhausted at max attempts" do
    profile = %{
      strategy: :linear,
      max_attempts: 2,
      initial_backoff_ms: 1_000,
      max_backoff_ms: 5_000
    }

    assert {:error, :retry_exhausted} = Planner.next_retry(%{attempts: 2}, profile)
  end
end
