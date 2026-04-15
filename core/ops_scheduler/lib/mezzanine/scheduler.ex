defmodule Mezzanine.Scheduler do
  @moduledoc """
  Internal runtime owner for durable work scheduling.
  """

  alias Mezzanine.Scheduler.{ConcurrencyGate, RetryQueue, StallDetector, TickLoop, WorkSelector}

  @default_tick_interval_ms 5_000
  @default_stall_threshold_ms 900_000

  @spec children_from_env() :: [{module(), keyword()}]
  def children_from_env do
    if Application.get_env(:mezzanine_ops_scheduler, :enabled, false) do
      [
        {TickLoop,
         [
           name: TickLoop,
           interval_ms:
             Application.get_env(
               :mezzanine_ops_scheduler,
               :tick_interval_ms,
               @default_tick_interval_ms
             ),
           tick_fun: fn -> :ok end
         ]}
      ]
    else
      []
    end
  end

  @spec tick_snapshot(String.t(), DateTime.t(), keyword()) :: {:ok, map()} | {:error, term()}
  def tick_snapshot(tenant_id, now \\ DateTime.utc_now(), opts \\ []) do
    stall_threshold_ms = Keyword.get(opts, :stall_threshold_ms, @default_stall_threshold_ms)
    max_concurrent_runs = Keyword.get(opts, :max_concurrent_runs, 1)

    with {:ok, ready_work} <- WorkSelector.ready_work(tenant_id, now),
         {:ok, due_runs} <- RetryQueue.due_runs(tenant_id, now),
         {:ok, stalled_runs} <- StallDetector.stalled_runs(tenant_id, stall_threshold_ms, now),
         {:ok, dispatch_allowed?} <-
           ConcurrencyGate.allow_dispatch?(tenant_id, max_concurrent_runs) do
      {:ok,
       %{
         ready_work: ready_work,
         due_runs: due_runs,
         stalled_runs: stalled_runs,
         dispatch_allowed?: dispatch_allowed?
       }}
    end
  end
end
