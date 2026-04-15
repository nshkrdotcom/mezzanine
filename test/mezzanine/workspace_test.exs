defmodule Mezzanine.WorkspaceTest do
  use ExUnit.Case, async: true

  test "declares the neutral package scaffold" do
    assert "core/pack_model" in Mezzanine.Workspace.neutral_package_paths()
    assert "core/pack_compiler" in Mezzanine.Workspace.neutral_package_paths()
    assert "core/config_registry" in Mezzanine.Workspace.neutral_package_paths()
    assert "core/object_engine" in Mezzanine.Workspace.neutral_package_paths()
    assert "core/execution_engine" in Mezzanine.Workspace.neutral_package_paths()
    assert "core/runtime_scheduler" in Mezzanine.Workspace.neutral_package_paths()
    assert "core/decision_engine" in Mezzanine.Workspace.neutral_package_paths()
    assert "core/evidence_engine" in Mezzanine.Workspace.neutral_package_paths()
    assert "core/projection_engine" in Mezzanine.Workspace.neutral_package_paths()
    assert "core/operator_engine" in Mezzanine.Workspace.neutral_package_paths()
    assert "core/audit_engine" in Mezzanine.Workspace.neutral_package_paths()
    assert "core/archival_engine" in Mezzanine.Workspace.neutral_package_paths()
  end

  test "declares the deprecated coexistence scaffold and gates" do
    assert "core/ops_model" in Mezzanine.Workspace.deprecated_package_paths()
    assert "bridges/app_kit_bridge" in Mezzanine.Workspace.deprecated_package_paths()
    assert "surfaces/work_surface" in Mezzanine.Workspace.deprecated_package_paths()
    assert "NO_NEW_PRODUCT_DEP_ON_OLD_MEZZANINE" in Mezzanine.Workspace.coexistence_gates()
    assert "MEZZANINE_NEUTRAL_CORE_CUTOVER" in Mezzanine.Workspace.coexistence_gates()
  end

  test "exposes the workspace project globs" do
    assert "." in Mezzanine.Workspace.active_project_globs()
    assert "core/*" in Mezzanine.Workspace.active_project_globs()
  end
end
