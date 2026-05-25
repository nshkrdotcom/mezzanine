defmodule Mezzanine.OptimizationEngine.RunSpecTest do
  use ExUnit.Case, async: true

  alias Mezzanine.OptimizationEngine
  alias Mezzanine.OptimizationEngine.RunSpec

  test "builds optimizer requests with context, route, eval, cost, promotion, and rollback refs" do
    spec = RunSpec.new!(base_spec())

    assert RunSpec.optimizer_request(spec) == %{
             tenant_ref: "tenant://phase13",
             run_ref: "run:gepa:phase13",
             objective_ref: "eval-suite://phase13",
             candidate_source_refs: [
               "memory://phase13/promoted",
               "prompt-artifact://phase13/instruction",
               "context-budget://phase13/optimizer",
               "guardrail://phase13/input",
               "drift://phase13/window"
             ],
             promotion_policy_ref: "promotion://phase13/candidate",
             trace_ref: "trace://phase13/optimization",
             context_packet_ref: "context-budget://phase13/optimizer",
             route_decision_ref: "target://phase13/role-worker",
             eval_refs: ["eval-suite://phase13"],
             cost_refs: ["cost-budget://phase13"],
             promotion_ref: "promotion://phase13/candidate",
             rollback_ref: "rollback://phase13/candidate",
             optimization_target_ref: "target://phase13/role-worker"
           }
  end

  test "proposes candidates through the GEPA Mezzanine optimizer adapter" do
    runtime_deps = %Mezzanine.AIExecution.RuntimeDeps{
      optimizer_adapter: GEPA.MezzanineOptimizerAdapter
    }

    assert {:ok, [candidate]} =
             OptimizationEngine.propose_candidates(base_spec(), runtime_deps,
               examples: ["example://phase13/1"]
             )

    assert candidate.candidate_ref == "candidate:component:gepa:mezzanine:1"
    assert candidate.context_packet_ref == "context-budget://phase13/optimizer"
    assert candidate.route_decision_ref == "target://phase13/role-worker"
    assert candidate.eval_refs == ["eval-suite://phase13"]
    assert candidate.cost_refs == ["cost-budget://phase13"]
    assert candidate.promotion_refs == ["promotion://phase13/candidate"]
    assert candidate.rollback_refs == ["rollback://phase13/candidate"]
  end

  defp base_spec do
    %{
      run_ref: "optimization-run://phase13",
      tenant_ref: "tenant://phase13",
      authority_ref: "authority://phase13/optimization",
      target_ref: "target://phase13/role-worker",
      framework_run_ref: "run:gepa:phase13",
      checkpoint_ref: "checkpoint://phase13/gepa",
      budget_ref: "budget://phase13/optimization",
      eval_suite_ref: "eval-suite://phase13",
      replay_bundle_ref: "replay://phase13/window",
      trace_ref: "trace://phase13/optimization",
      memory_ref_set: ["memory://phase13/promoted"],
      prompt_ref_set: ["prompt-artifact://phase13/instruction"],
      context_budget_ref: "context-budget://phase13/optimizer",
      guardrail_ref_set: ["guardrail://phase13/input"],
      cost_budget_ref_set: ["cost-budget://phase13"],
      drift_ref_set: ["drift://phase13/window"],
      persistence_ref_set: ["persistence://phase13/memory"],
      promotion_ref_set: ["promotion://phase13/candidate"],
      rollback_ref_set: ["rollback://phase13/candidate"]
    }
  end
end
