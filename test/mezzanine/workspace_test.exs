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

  test "declares the remaining deprecated coexistence scaffold and gates" do
    assert "core/ops_model" in Mezzanine.Workspace.deprecated_package_paths()
    refute "surfaces/program_surface" in Mezzanine.Workspace.deprecated_package_paths()
    assert "NO_NEW_PRODUCT_DEP_ON_OLD_MEZZANINE" in Mezzanine.Workspace.coexistence_gates()
    assert "MEZZANINE_NEUTRAL_CORE_CUTOVER" in Mezzanine.Workspace.coexistence_gates()
  end

  test "keeps the runtime workspace inventory aligned with the build contract" do
    assert Mezzanine.Workspace.deprecated_packages() == WorkspaceContract.deprecated_packages()

    assert Mezzanine.Workspace.deprecated_package_paths() ==
             WorkspaceContract.deprecated_package_paths()
  end

  test "classifies the remaining blocked deprecated packages explicitly" do
    assert Mezzanine.Workspace.delete_ready_deprecated_package_paths() == []

    ops_domain =
      Enum.find(Mezzanine.Workspace.blocked_deprecated_packages(), fn package ->
        package.path == "core/ops_domain"
      end)

    assert "app_kit/bridges/mezzanine_bridge" in ops_domain.blocking_consumers
    assert "stack_lab/support/citadel_spine_harness" in ops_domain.blocking_consumers
    refute "extravaganza/runtime_provisioner" in ops_domain.blocking_consumers

    ops_model =
      Enum.find(Mezzanine.Workspace.blocked_deprecated_packages(), fn package ->
        package.path == "core/ops_model"
      end)

    assert "app_kit/bridges/mezzanine_bridge" in ops_model.blocking_consumers

    refute Enum.any?(Mezzanine.Workspace.blocked_deprecated_packages(), fn package ->
             package.path == "bridges/app_kit_bridge"
           end)

    refute Enum.any?(Mezzanine.Workspace.blocked_deprecated_packages(), fn package ->
             package.path == "surfaces/program_surface"
           end)

    assert Enum.all?(Mezzanine.Workspace.deprecated_packages(), fn package ->
             package.delete_ready? == false
           end)
  end

  test "removes retired packages from the live workspace when phases 6.1.2, 6.1.3.3, and 7.1 close" do
    delete_ready_paths = [
      "core/ops_scheduler",
      "bridges/app_kit_bridge",
      "surfaces/program_surface",
      "surfaces/work_surface",
      "surfaces/operator_surface",
      "surfaces/review_surface"
    ]

    assert Enum.all?(delete_ready_paths, fn path ->
             path not in Mezzanine.Workspace.package_paths()
           end)

    assert Enum.all?(delete_ready_paths, fn path ->
             path not in WorkspaceContract.package_paths()
           end)

    assert Enum.all?(delete_ready_paths, fn path ->
             path not in Mezzanine.Workspace.deprecated_package_paths()
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
    refute "surfaces/*" in Mezzanine.Workspace.active_project_globs()
  end
end
