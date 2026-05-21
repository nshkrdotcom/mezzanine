defmodule Mezzanine.AgentTurnEngine.Reducer do
  @moduledoc """
  Pure reducers for agent turn ledgers.
  """

  alias Mezzanine.AgentTurnEngine.{
    AgentConversationEvent,
    AgentExecutionEvent,
    AgentPendingInteraction,
    AgentRunCursor,
    AgentTurnLedger
  }

  defmodule State do
    @moduledoc """
    In-memory reducer state used by pure tests and store adapters.
    """

    @enforce_keys [:ledger]
    defstruct ledger: nil,
              events: [],
              event_refs: MapSet.new(),
              idempotency_keys: MapSet.new(),
              pending_interaction: nil

    @type t :: %__MODULE__{
            ledger: AgentTurnLedger.t(),
            events: [AgentConversationEvent.t() | AgentExecutionEvent.t()],
            event_refs: MapSet.t(String.t()),
            idempotency_keys: MapSet.t(String.t()),
            pending_interaction: AgentPendingInteraction.t() | nil
          }
  end

  @resolved_pending_statuses [:approved, :denied, :expired, :cancelled]

  @spec new(AgentTurnLedger.t()) :: {:ok, State.t()} | {:error, term()}
  def new(%AgentTurnLedger{} = ledger), do: {:ok, %State{ledger: ledger}}
  def new(_other), do: {:error, {:invalid, :ledger, :agent_turn_ledger_required}}

  @spec new!(AgentTurnLedger.t()) :: State.t()
  def new!(ledger) do
    case new(ledger) do
      {:ok, state} -> state
      {:error, reason} -> raise ArgumentError, "invalid reducer state: #{inspect(reason)}"
    end
  end

  @spec append(State.t(), AgentConversationEvent.t() | AgentExecutionEvent.t()) ::
          {:ok, State.t()} | {:duplicate, State.t()} | {:error, term()}
  def append(%State{} = state, event) do
    cond do
      not event_struct?(event) ->
        {:error, {:invalid, :event, :agent_event_required}}

      event.ledger_ref != state.ledger.ledger_ref ->
        {:error, {:invalid, :ledger_ref, :mismatch}}

      duplicate?(state, event) ->
        {:duplicate, state}

      event.seq != state.ledger.next_seq ->
        {:error, {:invalid, :seq, {:expected, state.ledger.next_seq}}}

      true ->
        append_new_event(state, event)
    end
  end

  @spec append!(State.t(), AgentConversationEvent.t() | AgentExecutionEvent.t()) ::
          {:ok, State.t()} | {:duplicate, State.t()} | {:error, term()}
  def append!(state, event), do: append(state, event)

  @spec open_pending(State.t(), AgentPendingInteraction.t()) ::
          {:ok, State.t()} | {:error, term()}
  def open_pending(%State{} = state, %AgentPendingInteraction{} = pending) do
    cond do
      pending.ledger_ref != state.ledger.ledger_ref ->
        {:error, {:invalid, :ledger_ref, :mismatch}}

      pending.status != :open ->
        {:error, {:invalid, :status, :open_required}}

      pending.opened_seq != state.ledger.next_seq ->
        {:error, {:invalid, :opened_seq, {:expected, state.ledger.next_seq}}}

      true ->
        ledger =
          state.ledger
          |> Map.put(:status, :pending)
          |> Map.put(:pending_interaction_ref, pending.pending_ref)
          |> Map.put(:next_seq, state.ledger.next_seq + 1)

        {:ok, %{state | ledger: ledger, pending_interaction: pending}}
    end
  end

  def open_pending(_state, _pending), do: {:error, {:invalid, :pending_interaction, :required}}

  @spec resolve_pending(State.t(), String.t(), atom(), DateTime.t()) ::
          {:ok, State.t(), AgentPendingInteraction.t()} | {:error, term()}
  def resolve_pending(%State{pending_interaction: nil}, _pending_ref, _status, _resolved_at) do
    {:error, {:invalid, :pending_interaction, :not_open}}
  end

  def resolve_pending(%State{} = state, pending_ref, status, %DateTime{} = resolved_at)
      when status in @resolved_pending_statuses do
    pending = state.pending_interaction

    if pending.pending_ref == pending_ref do
      resolved = %{pending | status: status, resolved_at: resolved_at}

      ledger =
        state.ledger
        |> Map.put(:status, ledger_status_after_pending(status))
        |> Map.put(:pending_interaction_ref, nil)

      {:ok, %{state | ledger: ledger, pending_interaction: resolved}, resolved}
    else
      {:error, {:invalid, :pending_ref, :mismatch}}
    end
  end

  def resolve_pending(_state, _pending_ref, status, _resolved_at) do
    if status in @resolved_pending_statuses do
      {:error, {:invalid, :resolved_at, :datetime_required}}
    else
      {:error, {:invalid, :status, {:one_of, @resolved_pending_statuses}}}
    end
  end

  @spec issue_cursor(State.t(), map()) :: {:ok, State.t(), AgentRunCursor.t()} | {:error, term()}
  def issue_cursor(%State{} = state, attrs) when is_map(attrs) do
    cursor_attrs =
      attrs
      |> Map.put(:ledger_ref, state.ledger.ledger_ref)
      |> Map.put(:tenant_ref, state.ledger.tenant_ref)
      |> Map.put(:last_seq_seen, state.ledger.next_seq - 1)

    with {:ok, cursor} <- AgentRunCursor.new(cursor_attrs) do
      ledger = %{state.ledger | cursor_ref: cursor.cursor_ref}
      {:ok, %{state | ledger: ledger}, cursor}
    end
  end

  defp append_new_event(state, event) do
    with :ok <- terminal_receipt_present(event) do
      ledger = update_ledger_from_event(state.ledger, event)
      state = track_event(%{state | ledger: ledger}, event)
      {:ok, state}
    end
  end

  defp terminal_receipt_present(%AgentConversationEvent{
         event_type: :run_completed,
         evidence_refs: []
       }) do
    {:error, {:invalid, :terminal_event, :receipt_or_evidence_required}}
  end

  defp terminal_receipt_present(%AgentExecutionEvent{
         event_type: :terminal_reduction_completed,
         lower_receipt_ref: nil
       }) do
    {:error, {:invalid, :terminal_event, :receipt_or_evidence_required}}
  end

  defp terminal_receipt_present(_event), do: :ok

  defp update_ledger_from_event(ledger, event) do
    ledger
    |> Map.put(:next_seq, ledger.next_seq + 1)
    |> Map.put(:last_reduced_seq, max(ledger.last_reduced_seq, event.seq))
    |> update_event_class_seq(event)
    |> Map.put(:status, status_after_event(ledger.status, event))
    |> Map.put(:updated_at, event.occurred_at)
  end

  defp update_event_class_seq(ledger, %AgentConversationEvent{seq: seq}) do
    %{ledger | last_conversation_seq: max(ledger.last_conversation_seq, seq)}
  end

  defp update_event_class_seq(ledger, %AgentExecutionEvent{seq: seq}) do
    %{ledger | last_execution_seq: max(ledger.last_execution_seq, seq)}
  end

  defp status_after_event(_status, %AgentConversationEvent{event_type: :run_completed}),
    do: :completed

  defp status_after_event(_status, %AgentConversationEvent{event_type: :run_failed}), do: :failed

  defp status_after_event(_status, %AgentConversationEvent{event_type: :run_cancelled}),
    do: :cancelled

  defp status_after_event(_status, %AgentExecutionEvent{event_type: :failure_classified}),
    do: :failed

  defp status_after_event(:initialized, _event), do: :running
  defp status_after_event(status, _event), do: status

  defp track_event(state, event) do
    %{
      state
      | events: state.events ++ [event],
        event_refs: MapSet.put(state.event_refs, event.event_ref),
        idempotency_keys: track_idempotency_key(state.idempotency_keys, event)
    }
  end

  defp track_idempotency_key(keys, %AgentExecutionEvent{idempotency_key: idempotency_key}) do
    MapSet.put(keys, idempotency_key)
  end

  defp track_idempotency_key(keys, _event), do: keys

  defp duplicate?(state, event) do
    MapSet.member?(state.event_refs, event.event_ref) or
      duplicate_idempotency_key?(state, event)
  end

  defp duplicate_idempotency_key?(state, %AgentExecutionEvent{idempotency_key: idempotency_key}) do
    MapSet.member?(state.idempotency_keys, idempotency_key)
  end

  defp duplicate_idempotency_key?(_state, _event), do: false

  defp event_struct?(%AgentConversationEvent{}), do: true
  defp event_struct?(%AgentExecutionEvent{}), do: true
  defp event_struct?(_other), do: false

  defp ledger_status_after_pending(:approved), do: :running
  defp ledger_status_after_pending(:denied), do: :failed
  defp ledger_status_after_pending(:expired), do: :failed
  defp ledger_status_after_pending(:cancelled), do: :cancelled
end
