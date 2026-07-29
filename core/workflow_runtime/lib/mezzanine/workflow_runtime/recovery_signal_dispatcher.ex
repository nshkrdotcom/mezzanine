defmodule Mezzanine.WorkflowRuntime.RecoverySignalDispatcher do
  @moduledoc """
  Post-commit dispatcher for the durable run-control signal outbox.

  The dispatcher owns no business state. It claims fenced rows, calls the
  configured workflow runtime with the retained signal identity, and persists
  the outcome before acknowledging the dispatch.
  """

  use GenServer

  require Logger

  @default_interval_ms 1_000
  @default_batch_size 10

  def child_spec(opts) do
    %{
      id: Keyword.get(opts, :name, __MODULE__),
      start: {__MODULE__, :start_link, [opts]},
      type: :worker
    }
  end

  def start_link(opts) do
    GenServer.start_link(__MODULE__, opts, name: Keyword.get(opts, :name, __MODULE__))
  end

  @doc "Claims and dispatches one bounded signal batch immediately."
  def dispatch_once(server \\ __MODULE__), do: GenServer.call(server, :dispatch_once, 30_000)

  @impl true
  def init(opts) do
    state = %{
      batch_size: Keyword.get(opts, :batch_size, @default_batch_size),
      interval_ms: Keyword.get(opts, :interval_ms, @default_interval_ms),
      lock_owner: Keyword.get_lazy(opts, :lock_owner, &default_lock_owner/0),
      runtime: Keyword.get(opts, :runtime, Mezzanine.WorkflowRuntime.TemporalexAdapter),
      store: Keyword.get(opts, :store, Mezzanine.WorkflowRuntime.RecoveryControl),
      store_opts: Keyword.get(opts, :store_opts, [])
    }

    if Keyword.get(opts, :schedule?, true), do: schedule(state.interval_ms)
    {:ok, state}
  end

  @impl true
  def handle_call(:dispatch_once, _from, state), do: {:reply, dispatch_batch(state), state}

  @impl true
  def handle_info(:dispatch, state) do
    case dispatch_batch(state) do
      :ok ->
        :ok

      {:error, reason} ->
        Logger.error("Mezzanine control signal dispatch failed: #{inspect(reason)}")
    end

    schedule(state.interval_ms)
    {:noreply, state}
  end

  defp dispatch_batch(state) do
    with {:ok, rows} <-
           state.store.claim_signal_outboxes(
             state.lock_owner,
             state.batch_size,
             state.store_opts
           ) do
      Enum.reduce(rows, :ok, fn row, aggregate ->
        case dispatch(row, state) do
          :ok -> aggregate
          {:error, reason} when aggregate == :ok -> {:error, reason}
          {:error, _reason} -> aggregate
        end
      end)
    end
  end

  defp dispatch(row, state) do
    outcome = state.runtime.signal_workflow(row.signal_payload)

    case state.store.complete_signal_outbox(
           row.outbox_ref,
           row.dispatch_fence,
           outcome,
           state.store_opts
         ) do
      {:ok, _persisted} -> :ok
      {:error, reason} -> {:error, {:outbox_outcome_not_persisted, reason}}
    end
  end

  defp default_lock_owner do
    "#{inspect(node())}:recovery-signal:#{System.unique_integer([:positive, :monotonic])}"
  end

  defp schedule(interval_ms), do: Process.send_after(self(), :dispatch, interval_ms)
end
