defmodule Mezzanine.AgentTurnEngine do
  @moduledoc """
  Pure native agent turn ledger contracts and reducers.

  This package owns data validation and deterministic reductions for agent
  turn truth. Persistence, process ownership, product DTO mapping, authority
  compilation, and lower dispatch are owned by adjacent packages.
  """

  alias Mezzanine.AgentTurnEngine.AgentTurnLedger
  alias Mezzanine.AgentTurnEngine.Reducer

  @spec new_ledger(map()) :: {:ok, AgentTurnLedger.t()} | {:error, term()}
  defdelegate new_ledger(attrs), to: AgentTurnLedger, as: :new

  @spec new_state(AgentTurnLedger.t()) :: {:ok, Reducer.State.t()} | {:error, term()}
  defdelegate new_state(ledger), to: Reducer, as: :new
end
