defmodule Mezzanine.OptimizationEngine.BudgetPolicyTest do
  use ExUnit.Case, async: true

  alias Mezzanine.OptimizationEngine.BudgetPolicy

  test "enforces candidate, metric, provider cost, wall clock, token, GPU, retry, and live call limits" do
    limits = %{
      candidate_limit: 2,
      metric_call_limit: 8,
      provider_cost_limit: 20,
      wall_clock_ms_limit: 1_000,
      token_limit: 400,
      gpu_minute_limit: 0,
      retry_limit: 1,
      live_call_limit: 0
    }

    assert {:ok, %BudgetPolicy.Decision{} = decision} =
             BudgetPolicy.enforce(limits, %{
               candidate_count: 1,
               metric_calls: 4,
               provider_cost_units: 10,
               wall_clock_ms: 250,
               tokens: 100,
               gpu_minutes: 0,
               retries: 1,
               live_calls: 0
             })

    assert decision.decision_class == :allow

    assert decision.budget_ref_set == [
             "budget:candidate_limit",
             "budget:metric_call_limit",
             "budget:provider_cost_limit",
             "budget:wall_clock_ms_limit",
             "budget:token_limit",
             "budget:gpu_minute_limit",
             "budget:retry_limit",
             "budget:live_call_limit"
           ]

    assert {:error, %BudgetPolicy.Decision{} = denied} =
             BudgetPolicy.enforce(limits, %{
               candidate_count: 1,
               metric_calls: 4,
               provider_cost_units: 10,
               wall_clock_ms: 250,
               tokens: 100,
               gpu_minutes: 0,
               retries: 1,
               live_calls: 1
             })

    assert denied.decision_class == :deny
    assert denied.blocked_limit == :live_call_limit
  end
end
