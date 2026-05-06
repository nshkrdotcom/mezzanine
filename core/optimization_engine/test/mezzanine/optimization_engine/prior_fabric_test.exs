defmodule Mezzanine.OptimizationEngine.PriorFabricTest do
  use ExUnit.Case, async: true

  alias Mezzanine.OptimizationEngine

  test "binds inherited memory, prompt, guardrail, eval, replay, cost, and persistence refs" do
    assert {:ok, receipt} = OptimizationEngine.bind_prior_fabric(fabric_attrs())

    assert receipt.fixture_refs == [
             "AOC-032",
             "AOC-033",
             "PERSIST-AOC-004",
             "PERSIST-AOC-005"
           ]

    assert receipt.memory_ref_set == ["memory-profile://role-worker"]
    assert receipt.prompt_ref_set == ["prompt-artifact://role-worker:v1"]
    assert receipt.guardrail_ref_set == ["guardrail://input", "guardrail://output"]
    assert receipt.eval_ref_set == ["eval-suite://coordination-repair"]
    assert receipt.replay_ref_set == ["replay-bundle://trace-window"]
    assert receipt.cost_budget_ref_set == ["cost-budget://optimization-search"]
    assert receipt.context_budget_ref == "context-budget://optimizer-batch"
    assert receipt.persistence_refs.selected_tier_ref == "store-tier://memory-ephemeral"
    assert receipt.local_restart_safe_profile_ref == "restart-safe://local-selected-categories"
    assert receipt.status == :pass
  end

  test "rejects raw prompt, provider payload, model output, credential, and memory body fields" do
    attrs =
      fabric_attrs()
      |> Map.put(:raw_prompt, "do not project")

    assert {:error, {:forbidden_raw_field, :raw_prompt}} =
             OptimizationEngine.bind_prior_fabric(attrs)
  end

  defp fabric_attrs do
    %{
      run_ref: "optimization-run://phase12",
      tenant_ref: "tenant://adaptive",
      optimization_target_ref: "gepa-target://role/worker",
      memory_ref_set: ["memory-profile://role-worker"],
      prompt_ref_set: ["prompt-artifact://role-worker:v1"],
      context_budget_ref: "context-budget://optimizer-batch",
      guardrail_ref_set: ["guardrail://input", "guardrail://output"],
      eval_ref_set: ["eval-suite://coordination-repair"],
      replay_ref_set: ["replay-bundle://trace-window"],
      drift_ref_set: ["drift-run://role-worker"],
      cost_budget_ref_set: ["cost-budget://optimization-search"],
      trace_ref_set: ["trace://optimization/prior-fabric"],
      gate_evidence_refs: ["gate-evidence://eval", "gate-evidence://guardrail"],
      promotion_ref: "promotion://candidate/pending",
      rollback_ref: "rollback://candidate/available",
      persistence_refs: %{
        profile_ref: "persistence-profile://memory-default",
        selected_tier_ref: "store-tier://memory-ephemeral",
        store_ref: "store://memory/adaptive",
        retention_ref: "retention://ephemeral",
        debug_tap_posture_ref: "debug-tap://redacted-only",
        restart_safety_ref: "restart-safe://local-selected-categories"
      },
      local_restart_safe_profile_ref: "restart-safe://local-selected-categories"
    }
  end
end
