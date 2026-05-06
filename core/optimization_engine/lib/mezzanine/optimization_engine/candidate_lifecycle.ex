defmodule Mezzanine.OptimizationEngine.CandidateLifecycle do
  @moduledoc """
  Candidate lifecycle classes for governed GEPA optimization.
  """

  @states [
    :proposed,
    :leased_for_eval,
    :evaluated,
    :rejected,
    :promotion_pending,
    :promoted,
    :rolled_back
  ]

  @spec states() :: [atom()]
  def states, do: @states
end
