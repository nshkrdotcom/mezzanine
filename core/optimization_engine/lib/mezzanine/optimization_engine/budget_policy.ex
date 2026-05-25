defmodule Mezzanine.OptimizationEngine.BudgetPolicy.Decision do
  @moduledoc "Budget-aware search decision."

  @type t :: %__MODULE__{
          decision_class: :allow | :deny,
          blocked_limit: atom() | nil,
          budget_ref_set: [String.t()]
        }

  @enforce_keys [:decision_class, :budget_ref_set]
  defstruct [:decision_class, :budget_ref_set, blocked_limit: nil]
end

defmodule Mezzanine.OptimizationEngine.BudgetPolicy do
  @moduledoc """
  Fail-closed optimization budget policy.
  """

  alias Mezzanine.OptimizationEngine.BudgetPolicy.Decision
  alias Mezzanine.OptimizationEngine.Value

  @limit_order [
    {:candidate_limit, :candidate_count, "budget:candidate_limit"},
    {:metric_call_limit, :metric_calls, "budget:metric_call_limit"},
    {:provider_cost_limit, :provider_cost_units, "budget:provider_cost_limit"},
    {:wall_clock_ms_limit, :wall_clock_ms, "budget:wall_clock_ms_limit"},
    {:token_limit, :tokens, "budget:token_limit"},
    {:gpu_minute_limit, :gpu_minutes, "budget:gpu_minute_limit"},
    {:retry_limit, :retries, "budget:retry_limit"},
    {:live_call_limit, :live_calls, "budget:live_call_limit"}
  ]

  @spec enforce(map(), map()) :: {:ok, Decision.t()} | {:error, Decision.t()}
  def enforce(limits, usage) when is_map(limits) and is_map(usage) do
    budget_ref_set = Enum.map(@limit_order, fn {_limit_key, _usage_key, ref} -> ref end)

    case exceeded_limit(limits, usage) do
      nil ->
        {:ok, %Decision{decision_class: :allow, budget_ref_set: budget_ref_set}}

      limit_key ->
        {:error,
         %Decision{
           decision_class: :deny,
           blocked_limit: limit_key,
           budget_ref_set: budget_ref_set
         }}
    end
  end

  def enforce(_limits, _usage),
    do:
      {:error,
       %Decision{decision_class: :deny, blocked_limit: :invalid_budget, budget_ref_set: []}}

  defp exceeded_limit(limits, usage) do
    Enum.find_value(@limit_order, fn {limit_key, usage_key, _ref} ->
      limit = Value.get(limits, limit_key, 0)
      requested = Value.get(usage, usage_key, 0)

      if is_number(limit) and is_number(requested) and requested > limit do
        limit_key
      end
    end)
  end
end
