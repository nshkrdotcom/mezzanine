defmodule MezzanineDecisionEngine do
  @moduledoc """
  Neutral decision-ledger contract entrypoint for the Mezzanine rebuild.
  """

  @spec components() :: [module()]
  def components do
    [
      Mezzanine.Decisions,
      Mezzanine.Decisions.DecisionRecord,
      Mezzanine.DecisionCommands
    ]
  end
end
