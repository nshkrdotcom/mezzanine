defmodule Mezzanine.Planner.ObligationDeriver do
  @moduledoc """
  Derives obligations from a compiled work plan.
  """

  alias MezzanineOpsModel.{Obligation, WorkPlan}

  @spec derive(WorkPlan.t()) :: [Obligation.t()]
  def derive(%WorkPlan{} = plan) do
    Enum.map(plan.derived_review_intents, fn review_intent ->
      Obligation.new!(%{
        obligation_id: "obligation:" <> review_intent.intent_id,
        work_id: plan.work_id,
        obligation_type: :review,
        state: :pending,
        subject: %{
          review_intent_id: review_intent.intent_id,
          gate: review_intent.gate,
          required_decisions: review_intent.required_decisions
        },
        metadata: %{source: :plan_compiler}
      })
    end)
  end
end
