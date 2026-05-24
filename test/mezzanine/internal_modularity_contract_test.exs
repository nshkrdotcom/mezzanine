defmodule Mezzanine.InternalModularityContractTest do
  use ExUnit.Case, async: true

  alias Mezzanine.Build.InternalModularityContract

  test "covers the live neutral core package graph" do
    assert InternalModularityContract.package_paths() == [
             "core/adaptive_control_engine",
             "core/agent_turn_engine",
             "core/ai_execution_engine",
             "core/ai_run_model",
             "core/archival_engine",
             "core/audit_engine",
             "core/barriers",
             "core/budget_enforcement_engine",
             "core/config_registry",
             "core/context_budget_admission",
             "core/context_packet_engine",
             "core/coordination_engine",
             "core/cost_attribution_engine",
             "core/decision_engine",
             "core/eval_engine",
             "core/evidence_engine",
             "core/execution_engine",
             "core/governed_effects",
             "core/headless_coding_ops",
             "core/leasing",
             "core/lifecycle_engine",
             "core/m1_m2_runtime",
             "core/mezzanine_core",
             "core/object_engine",
             "core/operator_engine",
             "core/ops_domain",
             "core/ops_model",
             "core/optimization_engine",
             "core/pack_compiler",
             "core/pack_model",
             "core/projection_engine",
             "core/runtime_profile",
             "core/runtime_scheduler",
             "core/substrate_model",
             "core/workflow_runtime",
             "core/workspace_build_model"
           ]
  end

  test "matches the declared internal path dependencies for every governed core package" do
    Enum.each(InternalModularityContract.package_specs(), fn spec ->
      assert InternalModularityContract.declared_internal_deps(spec.path) ==
               spec.allowed_internal_deps,
             """
             internal dependency contract drifted for #{spec.path}
             expected: #{inspect(spec.allowed_internal_deps)}
             actual: #{inspect(InternalModularityContract.declared_internal_deps(spec.path))}
             """
    end)
  end

  test "keeps headless coding ops outside the lower runtime dependency graph" do
    spec = InternalModularityContract.spec_for("core/headless_coding_ops")

    assert spec.allowed_internal_deps == []
    assert InternalModularityContract.declared_internal_deps("core/headless_coding_ops") == []
  end
end
