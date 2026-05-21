defmodule Mezzanine.AgentTurnEngine.Store do
  @moduledoc """
  Store behaviour for agent turn ledgers, replay, projections, and pending work.

  Implementations own persistence mechanics. They must preserve reducer
  idempotency, monotonic sequence order, and the rule that replay/catch-up reads
  existing facts without dispatching lower work.
  """

  alias Mezzanine.AgentTurnEngine.{
    AgentConversationEvent,
    AgentExecutionEvent,
    AgentPendingInteraction,
    AgentRunCursor,
    AgentTurnLedger,
    ExecutionReplay,
    PendingDecision
  }

  @type store :: term()
  @type event :: AgentConversationEvent.t() | AgentExecutionEvent.t()

  @callback put_ledger(store(), AgentTurnLedger.t()) :: {:ok, store()} | {:error, term()}
  @callback append_event(store(), event()) ::
              {:ok, store()} | {:duplicate, store()} | {:error, term()}
  @callback open_pending(store(), AgentPendingInteraction.t()) ::
              {:ok, store()} | {:error, term()}
  @callback resolve_pending(store(), String.t(), PendingDecision.t() | nil) ::
              {:ok, store(), AgentPendingInteraction.t()}
              | {:duplicate, store(), AgentPendingInteraction.t()}
              | {:error, term()}
  @callback catch_up(store(), AgentRunCursor.t()) :: {:ok, store(), map()} | {:error, term()}
  @callback replay(store(), ExecutionReplay.t()) :: {:ok, store(), map()} | {:error, term()}
  @callback projection_rows(store(), String.t()) :: [term()]
end
