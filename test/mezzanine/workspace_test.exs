defmodule Mezzanine.WorkspaceTest do
  use ExUnit.Case, async: true

  alias Mezzanine.Build.WorkspaceContract

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

  test "keeps the runtime workspace inventory aligned with the build contract" do
    assert Mezzanine.Workspace.deprecated_packages() == WorkspaceContract.deprecated_packages()

    assert Mezzanine.Workspace.deprecated_package_paths() ==
             WorkspaceContract.deprecated_package_paths()
  end

  test "classifies delete-ready and blocked deprecated packages explicitly" do
    assert Enum.sort(Mezzanine.Workspace.delete_ready_deprecated_package_paths()) == [
             "core/ops_scheduler",
             "surfaces/operator_surface",
             "surfaces/review_surface",
             "surfaces/work_surface"
           ]

    program_surface =
      Enum.find(Mezzanine.Workspace.blocked_deprecated_packages(), fn package ->
        package.path == "surfaces/program_surface"
      end)

    assert "extravaganza/runtime_provisioner" in program_surface.blocking_consumers

    app_kit_bridge =
      Enum.find(Mezzanine.Workspace.blocked_deprecated_packages(), fn package ->
        package.path == "bridges/app_kit_bridge"
      end)

    assert "app_kit/bridges/mezzanine_bridge" in app_kit_bridge.blocking_consumers
    assert "stack_lab/support/citadel_spine_harness" in app_kit_bridge.blocking_consumers

    assert Enum.all?(Mezzanine.Workspace.delete_ready_deprecated_package_paths(), fn path ->
             package =
               Enum.find(Mezzanine.Workspace.deprecated_packages(), fn candidate ->
                 candidate.path == path
               end)

             package.blocking_consumers == []
           end)
  end

  test "kept lower bridges stay limited to active lower seams" do
    assert "bridges/citadel_bridge" in Mezzanine.Workspace.kept_package_paths()
    assert "bridges/integration_bridge" in Mezzanine.Workspace.kept_package_paths()
    refute "bridges/execution_plane_bridge" in Mezzanine.Workspace.kept_package_paths()
  end

  test "exposes the workspace project globs" do
    assert "." in Mezzanine.Workspace.active_project_globs()
    assert "core/*" in Mezzanine.Workspace.active_project_globs()
  end
end
