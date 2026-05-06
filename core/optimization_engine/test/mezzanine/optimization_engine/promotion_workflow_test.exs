defmodule Mezzanine.OptimizationEngine.PromotionWorkflowTest do
  use ExUnit.Case, async: true

  alias Mezzanine.OptimizationEngine.PromotionWorkflow

  test "blocks failed or regressed candidates from promotion" do
    attrs = %{
      candidate_ref: "candidate:component:instruction:v2",
      parent_candidate_ref: "candidate:component:instruction:v1",
      eval_gate: :pass,
      replay_gate: :pass,
      guardrail_gate: :pass,
      budget_gate: :pass,
      shadow_gate: :pass,
      canary_gate: :pass,
      human_approval_gate: :pass,
      score_delta: -0.1,
      rollback_ref: "rollback:candidate:instruction:v2",
      trace_refs: ["trace:promotion:v2"]
    }

    assert {:error, %PromotionWorkflow.Decision{} = decision} =
             PromotionWorkflow.evaluate(attrs)

    assert decision.decision_class == :blocked
    assert decision.blocked_gate_refs == ["gate:regression"]
    assert decision.rollback_ref == "rollback:candidate:instruction:v2"

    failed_eval = %{attrs | eval_gate: :fail, score_delta: 0.1}

    assert {:error, %PromotionWorkflow.Decision{} = failed_decision} =
             PromotionWorkflow.evaluate(failed_eval)

    assert failed_decision.blocked_gate_refs == ["gate:eval"]
  end

  test "allows promotion only when all gates pass and candidate improves" do
    assert {:ok, %PromotionWorkflow.Decision{} = decision} =
             PromotionWorkflow.evaluate(%{
               candidate_ref: "candidate:component:instruction:v2",
               parent_candidate_ref: "candidate:component:instruction:v1",
               eval_gate: :pass,
               replay_gate: :pass,
               guardrail_gate: :pass,
               budget_gate: :pass,
               shadow_gate: :pass,
               canary_gate: :pass,
               human_approval_gate: :pass,
               score_delta: 0.1,
               promotion_ref: "promotion:candidate:instruction:v2",
               trace_refs: ["trace:promotion:v2"]
             })

    assert decision.decision_class == :promote
    assert decision.promotion_ref == "promotion:candidate:instruction:v2"
    assert decision.blocked_gate_refs == []
  end
end
