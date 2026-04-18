defmodule Mezzanine.InternalModularityContractTest do
  use ExUnit.Case, async: true

  alias Mezzanine.Build.InternalModularityContract

  test "covers the live neutral core package graph" do
    assert InternalModularityContract.package_paths() == [
             "core/archival_engine",
             "core/audit_engine",
             "core/barriers",
             "core/config_registry",
             "core/decision_engine",
             "core/evidence_engine",
             "core/execution_engine",
             "core/leasing",
             "core/lifecycle_engine",
             "core/mezzanine_core",
             "core/object_engine",
             "core/operator_engine",
             "core/ops_domain",
             "core/ops_model",
             "core/pack_compiler",
             "core/pack_model",
             "core/projection_engine",
             "core/runtime_scheduler"
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
end
