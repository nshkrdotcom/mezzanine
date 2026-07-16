defmodule Mezzanine.WorkflowRuntime.RunOutboxDispatcher do
  @moduledoc "Post-commit dispatcher for canonical Mezzanine run-start outbox rows."

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

  @doc "Claims and dispatches one bounded batch immediately."
  def dispatch_once(server \\ __MODULE__), do: GenServer.call(server, :dispatch_once, 30_000)

  @impl true
  def init(opts) do
    state = %{
      batch_size: Keyword.get(opts, :batch_size, @default_batch_size),
      interval_ms: Keyword.get(opts, :interval_ms, @default_interval_ms),
      lock_owner: Keyword.get_lazy(opts, :lock_owner, &default_lock_owner/0),
      runtime: Keyword.get(opts, :runtime, Mezzanine.WorkflowRuntime.TemporalexAdapter),
      store: Keyword.get(opts, :store, Mezzanine.WorkflowRuntime.Store),
      store_opts: Keyword.get(opts, :store_opts, [])
    }

    if Keyword.get(opts, :schedule?, true), do: schedule(state.interval_ms)
    {:ok, state}
  end

  @impl true
  def handle_call(:dispatch_once, _from, state) do
    {:reply, dispatch_batch(state), state}
  end

  @impl true
  def handle_info(:dispatch, state) do
    case dispatch_batch(state) do
      :ok ->
        :ok

      {:error, reason} ->
        Logger.error("Mezzanine workflow outbox dispatch failed: #{inspect(reason)}")
    end

    schedule(state.interval_ms)
    {:noreply, state}
  end

  defp dispatch_batch(state) do
    with {:ok, handoffs} <-
           state.store.claim_workflow_handoffs(
             state.lock_owner,
             state.batch_size,
             state.store_opts
           ) do
      Enum.reduce(handoffs, :ok, fn handoff, result ->
        case dispatch_handoff(handoff, state) do
          :ok -> result
          {:error, reason} when result == :ok -> {:error, reason}
          {:error, _reason} -> result
        end
      end)
    end
  end

  defp dispatch_handoff(handoff, state) do
    with {:ok, projection} <- state.store.fetch_projection(handoff.run_ref, state.store_opts) do
      result = state.runtime.start_workflow(start_request(handoff, projection))
      persist_outcome(handoff, result, state)
    end
  end

  defp persist_outcome(handoff, {:ok, _receipt}, state) do
    complete(state, handoff, "acknowledged", nil)
  end

  defp persist_outcome(handoff, {:error, {:already_started, _run_ref}}, state) do
    complete(state, handoff, "acknowledged", nil)
  end

  defp persist_outcome(handoff, {:error, reason}, state) do
    complete(state, handoff, "ambiguous", error_ref(reason))
  end

  defp complete(state, handoff, next_state, error_ref) do
    case state.store.complete_workflow_handoff(
           handoff.outbox_ref,
           next_state,
           error_ref,
           state.store_opts
         ) do
      {:ok, _handoff} -> :ok
      {:error, reason} -> {:error, {:outbox_outcome_not_persisted, reason}}
    end
  end

  defp start_request(handoff, projection) do
    projection_body = Map.fetch!(projection, :projection)

    %{
      args: %{
        command_id: handoff.event_ref,
        outbox_ref: handoff.outbox_ref,
        resource_ref: handoff.run_ref,
        run_ref: handoff.run_ref,
        tenant_ref: Map.fetch!(projection, :tenant_ref),
        trace_id: Map.fetch!(projection_body, "trace_ref")
      },
      idempotency_key: handoff.idempotency_key,
      release_manifest_ref: "release://nshkr/mezzanine-agent-run-v1",
      resource_ref: handoff.run_ref,
      task_queue: handoff.task_queue,
      tenant_ref: Map.fetch!(projection, :tenant_ref),
      trace_id: Map.fetch!(projection_body, "trace_ref"),
      workflow_id: handoff.workflow_ref,
      workflow_module: Mezzanine.Workflows.NshkrAgentRun,
      workflow_type: handoff.workflow_type,
      workflow_version: "mezzanine.agent-run.v1"
    }
  end

  defp error_ref(reason) do
    digest =
      reason
      |> :erlang.term_to_binary()
      |> then(&:crypto.hash(:sha256, &1))
      |> Base.url_encode64(padding: false)

    "error://mezzanine/temporal/#{digest}"
  end

  defp default_lock_owner do
    "#{inspect(node())}:#{System.unique_integer([:positive, :monotonic])}"
  end

  defp schedule(interval_ms), do: Process.send_after(self(), :dispatch, interval_ms)
end

defmodule Mezzanine.Workflows.NshkrAgentRun do
  @moduledoc "Durable NSHKR agent-run workflow initialized from committed Mezzanine refs."

  use Temporalex.Workflow, task_queue: "nshkr.mezzanine.agent-run.v1"

  @impl Temporalex.Workflow
  def run(input) do
    state = %{
      command_id: value(input, :command_id),
      resource_ref: value(input, :resource_ref),
      run_ref: value(input, :run_ref),
      status: "accepted",
      tenant_ref: value(input, :tenant_ref),
      trace_id: value(input, :trace_id)
    }

    set_state(state)
    {:ok, state}
  end

  @impl Temporalex.Workflow
  def handle_query("status", _args, state), do: {:reply, state || %{status: "starting"}}

  def handle_query("run.state.v1", _args, state),
    do: {:reply, state || %{status: "starting"}}

  defp value(input, key) when is_map(input),
    do: Map.get(input, key, Map.get(input, Atom.to_string(key)))
end
