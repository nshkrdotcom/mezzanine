defmodule Mezzanine.AgentTurnEngine.Store.Memory do
  @moduledoc """
  Caller-owned in-memory store for deterministic tests and host adapters.

  This module starts no processes and owns no supervision tree. Production
  persistence adapters can implement the same behaviour around Mezzanine's
  durable store while preserving the reducer semantics exercised here.
  """

  @behaviour Mezzanine.AgentTurnEngine.Store

  alias Mezzanine.AgentTurnEngine.{
    AgentConversationEvent,
    AgentExecutionEvent,
    AgentPendingInteraction,
    AgentRunCursor,
    AgentTurnLedger,
    ExecutionReplay,
    PendingDecision,
    Projection,
    Reducer
  }

  defstruct ledgers: %{},
            states: %{},
            events: %{},
            cursors: %{},
            pending: %{},
            projections: %{},
            replay_records: %{},
            decisions: %{},
            lower_dispatch_count: 0

  @type t :: %__MODULE__{}
  @type event :: AgentConversationEvent.t() | AgentExecutionEvent.t()

  @spec new() :: t()
  def new do
    %__MODULE__{}
  end

  @impl true
  def put_ledger({:ok, %__MODULE__{} = store}, %AgentTurnLedger{} = ledger) do
    put_ledger(store, ledger)
  end

  def put_ledger(%__MODULE__{} = store, %AgentTurnLedger{} = ledger) do
    with {:ok, state} <- Reducer.new(ledger) do
      {:ok, put_state(store, state)}
    end
  end

  def put_ledger(_store, _ledger), do: {:error, {:invalid, :ledger, :agent_turn_ledger_required}}

  @impl true
  def append_event({:ok, %__MODULE__{} = store}, event), do: append_event(store, event)

  def append_event(%__MODULE__{} = store, event) when is_struct(event) do
    with {:ok, state} <- fetch_state(store, event.ledger_ref) do
      case Reducer.append(state, event) do
        {:ok, next_state} ->
          {:ok, put_state(store, next_state)}

        {:duplicate, _state} ->
          {:duplicate, store}

        {:error, reason} ->
          {:error, reason}
      end
    end
  end

  def append_event(_store, _event), do: {:error, {:invalid, :event, :agent_event_required}}

  @impl true
  def open_pending({:ok, %__MODULE__{} = store}, %AgentPendingInteraction{} = pending) do
    open_pending(store, pending)
  end

  def open_pending(%__MODULE__{} = store, %AgentPendingInteraction{} = pending) do
    with {:ok, state} <- fetch_state(store, pending.ledger_ref),
         {:ok, next_state} <- Reducer.open_pending(state, pending) do
      {:ok,
       store
       |> put_state(next_state)
       |> put_pending(next_state.pending_interaction)}
    end
  end

  def open_pending(_store, _pending), do: {:error, {:invalid, :pending_interaction, :required}}

  @impl true
  def resolve_pending(_store, _pending_ref, nil) do
    {:error, {:invalid, :decision_binding, :required}}
  end

  def resolve_pending(%__MODULE__{} = store, pending_ref, %PendingDecision{} = decision) do
    with :not_duplicate <- duplicate_decision(store, decision),
         {:ok, state} <- fetch_state_for_pending(store, pending_ref),
         :ok <- validate_decision_for_pending(state, pending_ref, decision),
         {:ok, next_state, resolved} <-
           Reducer.resolve_pending(state, pending_ref, decision.decision, decision.decided_at) do
      {:ok,
       store
       |> put_state(next_state)
       |> put_pending(resolved)
       |> put_decision(decision, resolved), resolved}
    else
      {:duplicate, duplicate_store, resolved} -> {:duplicate, duplicate_store, resolved}
      {:error, reason} -> {:error, reason}
    end
  end

  def resolve_pending(_store, _pending_ref, _decision) do
    {:error, {:invalid, :decision_binding, :pending_decision_required}}
  end

  @impl true
  def catch_up(%__MODULE__{} = store, %AgentRunCursor{} = cursor) do
    with {:ok, state} <- fetch_state(store, cursor.ledger_ref),
         :ok <- validate_cursor(state.ledger, cursor),
         {:ok, next_cursor} <- next_cursor(cursor, visible_events(state.events, cursor)) do
      {:ok, put_cursor(store, next_cursor),
       %{events: visible_events(state.events, cursor), cursor: next_cursor}}
    end
  end

  def catch_up(_store, _cursor), do: {:error, {:invalid, :cursor, :agent_run_cursor_required}}

  @impl true
  def replay(%__MODULE__{} = store, %ExecutionReplay{} = replay) do
    with {:ok, state} <- fetch_state(store, replay.ledger_ref),
         :ok <- validate_replay(state.ledger, replay) do
      run_replay(store, state, replay)
    end
  end

  def replay(_store, _replay), do: {:error, {:invalid, :replay, :execution_replay_required}}

  @impl true
  def projection_rows(%__MODULE__{} = store, ledger_ref) when is_binary(ledger_ref) do
    Map.get(store.projections, ledger_ref, [])
  end

  def projection_rows(_store, _ledger_ref), do: []

  defp run_replay(store, state, %ExecutionReplay{replay_kind: :catchup} = replay) do
    replay_page = %{
      replay: replay,
      events: events_in_range(state.events, replay.from_seq, replay.to_seq)
    }

    {:ok, put_replay(store, replay, replay_page), replay_page}
  end

  defp run_replay(store, _state, %ExecutionReplay{replay_kind: :reconstruct_projection} = replay) do
    projection = %{replay: replay, rows: projection_rows(store, replay.ledger_ref)}
    {:ok, put_replay(store, replay, projection), projection}
  end

  defp run_replay(store, _state, %ExecutionReplay{replay_kind: :resume_pending} = replay) do
    pending =
      store.pending
      |> Map.values()
      |> Enum.filter(&(&1.ledger_ref == replay.ledger_ref and &1.status == :open))

    result = %{replay: replay, pending: pending}
    {:ok, put_replay(store, replay, result), result}
  end

  defp run_replay(store, _state, %ExecutionReplay{replay_kind: :retry_lower_effect} = replay) do
    if replay.evidence_refs == [] do
      {:error, {:invalid, :retry_lower_effect, :evidence_required}}
    else
      result = %{replay: replay, retry_policy: :explicit, evidence_refs: replay.evidence_refs}
      {:ok, put_replay(store, replay, result), result}
    end
  end

  defp put_state(%__MODULE__{} = store, %Reducer.State{} = state) do
    ledger_ref = state.ledger.ledger_ref

    %{
      store
      | ledgers: Map.put(store.ledgers, ledger_ref, state.ledger),
        states: Map.put(store.states, ledger_ref, state),
        events: Map.put(store.events, ledger_ref, state.events),
        projections: Map.put(store.projections, ledger_ref, reduce_projection_rows(state.events))
    }
  end

  defp put_pending(%__MODULE__{} = store, %AgentPendingInteraction{} = pending) do
    %{store | pending: Map.put(store.pending, pending.pending_ref, pending)}
  end

  defp put_cursor(%__MODULE__{} = store, %AgentRunCursor{} = cursor) do
    %{store | cursors: Map.put(store.cursors, cursor.cursor_ref, cursor)}
  end

  defp put_replay(%__MODULE__{} = store, %ExecutionReplay{} = replay, result) do
    %{store | replay_records: Map.put(store.replay_records, replay.replay_ref, result)}
  end

  defp put_decision(%__MODULE__{} = store, %PendingDecision{} = decision, resolved) do
    %{store | decisions: Map.put(store.decisions, decision.idempotency_key, resolved)}
  end

  defp fetch_state(%__MODULE__{} = store, ledger_ref) do
    case Map.fetch(store.states, ledger_ref) do
      {:ok, state} -> {:ok, state}
      :error -> {:error, {:invalid, :ledger_ref, :unknown}}
    end
  end

  defp fetch_state_for_pending(%__MODULE__{} = store, pending_ref) do
    with {:ok, pending} <- fetch_pending(store, pending_ref) do
      fetch_state(store, pending.ledger_ref)
    end
  end

  defp fetch_pending(%__MODULE__{} = store, pending_ref) do
    case Map.fetch(store.pending, pending_ref) do
      {:ok, pending} -> {:ok, pending}
      :error -> {:error, {:invalid, :pending_interaction, :not_open}}
    end
  end

  defp duplicate_decision(%__MODULE__{} = store, %PendingDecision{} = decision) do
    case Map.fetch(store.decisions, decision.idempotency_key) do
      {:ok, resolved} -> {:duplicate, store, resolved}
      :error -> :not_duplicate
    end
  end

  defp validate_decision_for_pending(
         %Reducer.State{} = state,
         pending_ref,
         %PendingDecision{} = decision
       ) do
    with {:ok, pending} <- current_pending(state),
         :ok <- matching_ref(:pending_ref, pending.pending_ref, pending_ref),
         :ok <- matching_ref(:pending_ref, decision.pending_ref, pending_ref),
         :ok <- matching_ref(:decision_ref, pending.decision_ref, decision.decision_ref),
         :ok <- matching_ref(:tenant_ref, pending.tenant_ref, decision.tenant_ref),
         :ok <- matching_ref(:actor_ref, pending.actor_ref, decision.actor_ref),
         :ok <- current_authority(state.ledger.authority_ref, decision.authority_ref) do
      current_authority(pending.authority_ref, decision.authority_ref)
    end
  end

  defp current_pending(%Reducer.State{
         pending_interaction: %AgentPendingInteraction{status: :open} = pending
       }) do
    {:ok, pending}
  end

  defp current_pending(_state), do: {:error, {:invalid, :pending_interaction, :not_open}}

  defp matching_ref(_field, value, value), do: :ok
  defp matching_ref(field, _left, _right), do: {:error, {:invalid, field, :mismatch}}

  defp current_authority(authority_ref, authority_ref), do: :ok
  defp current_authority(_current, _candidate), do: {:error, {:invalid, :authority_ref, :stale}}

  defp validate_cursor(%AgentTurnLedger{} = ledger, %AgentRunCursor{} = cursor) do
    cond do
      ledger.tenant_ref != cursor.tenant_ref -> {:error, {:invalid, :tenant_ref, :mismatch}}
      ledger.actor_ref != cursor.actor_ref -> {:error, {:invalid, :actor_ref, :mismatch}}
      true -> :ok
    end
  end

  defp validate_replay(%AgentTurnLedger{} = ledger, %ExecutionReplay{} = replay) do
    if ledger.authority_ref == replay.authority_ref do
      :ok
    else
      {:error, {:invalid, :authority_ref, :stale}}
    end
  end

  defp visible_events(events, %AgentRunCursor{} = cursor) do
    Enum.filter(events, &(&1.seq > cursor.last_seq_seen and visible_to?(&1, cursor.visibility)))
  end

  defp events_in_range(events, from_seq, to_seq) do
    Enum.filter(events, &(&1.seq > from_seq and &1.seq <= to_seq))
  end

  defp visible_to?(%AgentConversationEvent{visibility: :product}, :product), do: true

  defp visible_to?(%AgentConversationEvent{visibility: visibility}, :operator),
    do: visibility in [:product, :operator]

  defp visible_to?(_event, :internal), do: true
  defp visible_to?(_event, _visibility), do: false

  defp next_cursor(%AgentRunCursor{} = cursor, events) do
    last_seq_seen =
      events
      |> Enum.map(& &1.seq)
      |> Enum.max(fn -> cursor.last_seq_seen end)

    cursor_attrs =
      cursor
      |> Map.from_struct()
      |> Map.put(:cursor_ref, cursor_ref(cursor.cursor_ref, last_seq_seen))
      |> Map.put(:last_seq_seen, last_seq_seen)
      |> Map.put(:issued_at, DateTime.add(cursor.issued_at, 1, :second))

    AgentRunCursor.new(cursor_attrs)
  end

  defp cursor_ref(cursor_ref, last_seq_seen) do
    cursor_ref <> "/after/" <> Integer.to_string(last_seq_seen)
  end

  defp reduce_projection_rows(events) do
    events
    |> Enum.flat_map(&projection_row/1)
  end

  defp projection_row(%AgentConversationEvent{} = event) do
    case Projection.reduce(event) do
      {:ok, row} -> [row]
    end
  end

  defp projection_row(%AgentExecutionEvent{}), do: []
end
