defmodule Mezzanine.AIExecution.OptimizerAdapter do
  @moduledoc """
  Behaviour implemented by GEPA-compatible optimization adapters.
  """

  alias OuterBrain.ContextABI.Failure

  @type optimization_request :: %{
          required(:tenant_ref) => String.t(),
          required(:objective_ref) => String.t(),
          required(:candidate_source_refs) => [String.t()],
          required(:promotion_policy_ref) => String.t(),
          required(:trace_ref) => String.t()
        }

  @type candidate_receipt :: %{
          required(:candidate_ref) => String.t(),
          required(:lineage_refs) => [String.t()],
          required(:objective_score_ref) => String.t(),
          required(:promotion_required?) => true,
          required(:trace_ref) => String.t()
        }

  @callback propose(optimization_request(), keyword()) ::
              {:ok, [candidate_receipt()]} | {:error, Failure.t()}
end
