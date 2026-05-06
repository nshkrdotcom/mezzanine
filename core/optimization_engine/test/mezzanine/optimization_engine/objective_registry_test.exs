defmodule Mezzanine.OptimizationEngine.ObjectiveRegistryTest do
  use ExUnit.Case, async: true

  alias Mezzanine.OptimizationEngine.ObjectiveRegistry

  test "covers governed objective types required by Phase 7" do
    assert ObjectiveRegistry.supported_types() == [
             :exact,
             :semantic,
             :faithfulness,
             :retrieval,
             :tool_success,
             :latency,
             :cost,
             :safety,
             :human_preference,
             :verifier,
             :constrained,
             :pareto
           ]

    assert {:ok, %ObjectiveRegistry.Objective{} = objective} =
             ObjectiveRegistry.fetch(:faithfulness)

    assert objective.objective_ref == "objective:faithfulness"
    assert objective.eval_foundation_ref == "eval_foundation:faithfulness"
    assert objective.replay_foundation_ref == "replay_foundation:faithfulness"
    assert objective.cost_foundation_ref == "cost_foundation:faithfulness"
  end
end
