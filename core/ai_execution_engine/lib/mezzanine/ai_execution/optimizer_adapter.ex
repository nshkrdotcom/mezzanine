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
          required(:trace_ref) => String.t(),
          optional(:run_ref) => String.t(),
          optional(:framework_run_ref) => String.t(),
          optional(:context_packet_ref) => String.t(),
          optional(:route_decision_ref) => String.t(),
          optional(:optimization_target_ref) => String.t(),
          optional(:eval_refs) => [String.t()],
          optional(:cost_refs) => [String.t()],
          optional(:promotion_ref) => String.t(),
          optional(:rollback_ref) => String.t()
        }

  @type candidate_receipt :: %{
          required(:candidate_ref) => String.t(),
          required(:lineage_refs) => [String.t()],
          required(:objective_score_ref) => String.t(),
          required(:promotion_required?) => true,
          required(:trace_ref) => String.t(),
          optional(:context_packet_ref) => String.t(),
          optional(:route_decision_ref) => String.t(),
          optional(:eval_refs) => [String.t()],
          optional(:cost_refs) => [String.t()],
          optional(:promotion_refs) => [String.t()],
          optional(:rollback_refs) => [String.t()]
        }

  @callback propose(optimization_request(), keyword()) ::
              {:ok, [candidate_receipt()]} | {:error, Failure.t()}
end
