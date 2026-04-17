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
    assert "core/ops_domain" in Mezzanine.Workspace.deprecated_package_paths()
    refute "core/ops_policy" in Mezzanine.Workspace.deprecated_package_paths()
    refute "core/ops_planner" in Mezzanine.Workspace.deprecated_package_paths()
    refute "core/ops_audit" in Mezzanine.Workspace.deprecated_package_paths()
    refute "core/ops_control" in Mezzanine.Workspace.deprecated_package_paths()
    refute "core/ops_assurance" in Mezzanine.Workspace.deprecated_package_paths()
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

    ops_model =
      Enum.find(Mezzanine.Workspace.blocked_deprecated_packages(), fn package ->
        package.path == "core/ops_model"
      end)

    ops_domain =
      Enum.find(Mezzanine.Workspace.blocked_deprecated_packages(), fn package ->
        package.path == "core/ops_domain"
      end)

    assert "core/ops_domain" in ops_model.blocking_consumers
    assert "core/execution_engine" in ops_domain.blocking_consumers
    refute "app_kit/bridges/mezzanine_bridge" in ops_domain.blocking_consumers
    refute "stack_lab/support/citadel_spine_harness" in ops_domain.blocking_consumers
    refute "extravaganza/runtime_provisioner" in ops_domain.blocking_consumers

    refute Enum.any?(Mezzanine.Workspace.blocked_deprecated_packages(), fn package ->
             package.path == "core/ops_policy"
           end)

    refute Enum.any?(Mezzanine.Workspace.blocked_deprecated_packages(), fn package ->
             package.path == "core/ops_planner"
           end)

    refute Enum.any?(Mezzanine.Workspace.blocked_deprecated_packages(), fn package ->
             package.path == "core/ops_audit"
           end)

    refute Enum.any?(Mezzanine.Workspace.blocked_deprecated_packages(), fn package ->
             package.path == "core/ops_control"
           end)

    refute Enum.any?(Mezzanine.Workspace.blocked_deprecated_packages(), fn package ->
             package.path == "core/ops_assurance"
           end)

    assert Enum.any?(Mezzanine.Workspace.deprecated_packages(), fn package ->
             package.path == "core/ops_model" and not package.delete_ready?
           end)

    assert Enum.any?(Mezzanine.Workspace.deprecated_packages(), fn package ->
             package.path == "core/ops_domain" and not package.delete_ready?
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

  test "removes the retired ops_policy and ops_planner packages from the workspace and owner deps" do
    root = Path.expand("../..", __DIR__)

    assert "core/ops_policy" not in Mezzanine.Workspace.package_paths()
    assert "core/ops_planner" not in Mezzanine.Workspace.package_paths()
    assert "core/ops_policy" not in WorkspaceContract.package_paths()
    assert "core/ops_planner" not in WorkspaceContract.package_paths()

    assert_refutes_file_patterns(
      root,
      ["core/ops_domain/mix.exs"],
      ":mezzanine_ops_policy"
    )

    assert_refutes_file_patterns(
      root,
      ["core/ops_domain/mix.exs"],
      ":mezzanine_ops_planner"
    )

    assert_refutes_file_patterns(root, ["docs/layout.md"], "core/ops_policy/")
    assert_refutes_file_patterns(root, ["docs/layout.md"], "core/ops_planner/")
  end

  test "removes the retired ops_audit, ops_control, and ops_assurance packages from the workspace" do
    root = Path.expand("../..", __DIR__)

    retired_paths = ["core/ops_audit", "core/ops_control", "core/ops_assurance"]

    assert Enum.all?(retired_paths, &(&1 not in Mezzanine.Workspace.package_paths()))
    assert Enum.all?(retired_paths, &(&1 not in WorkspaceContract.package_paths()))
    assert Enum.all?(retired_paths, &(&1 not in Mezzanine.Workspace.deprecated_package_paths()))

    assert_refutes_file_patterns(root, ["docs/layout.md"], "core/ops_audit/")
    assert_refutes_file_patterns(root, ["docs/layout.md"], "core/ops_control/")
    assert_refutes_file_patterns(root, ["docs/layout.md"], "core/ops_assurance/")
  end

  test "kept lower bridges stay limited to active lower seams" do
    assert "bridges/citadel_bridge" in Mezzanine.Workspace.kept_package_paths()
    assert "bridges/integration_bridge" in Mezzanine.Workspace.kept_package_paths()
    refute "bridges/execution_plane_bridge" in Mezzanine.Workspace.kept_package_paths()
  end

  test "kept lower bridges no longer depend on the deprecated ops_model seam" do
    root = Path.expand("../..", __DIR__)

    assert_refutes_file_patterns(
      root,
      ["bridges/citadel_bridge/mix.exs", "bridges/citadel_bridge/README.md"],
      ":mezzanine_ops_model"
    )

    assert_refutes_file_patterns(
      root,
      ["bridges/integration_bridge/mix.exs", "bridges/integration_bridge/README.md"],
      ":mezzanine_ops_model"
    )

    assert_refutes_file_patterns(
      root,
      [
        "bridges/citadel_bridge/README.md",
        "bridges/citadel_bridge/lib/**/*.ex",
        "bridges/citadel_bridge/test/**/*.exs"
      ],
      "MezzanineOpsModel"
    )

    assert_refutes_file_patterns(
      root,
      [
        "bridges/integration_bridge/README.md",
        "bridges/integration_bridge/lib/**/*.ex",
        "bridges/integration_bridge/test/**/*.exs"
      ],
      "MezzanineOpsModel"
    )
  end

  test "bounded app-kit and harness paths no longer depend on the deprecated ops_audit seam" do
    root = Path.expand("../..", __DIR__)

    assert_refutes_file_patterns(
      root,
      [
        "../app_kit/bridges/mezzanine_bridge/mix.exs",
        "../stack_lab/support/citadel_spine_harness/mix.exs"
      ],
      ":mezzanine_ops_audit"
    )

    assert_refutes_file_patterns(
      root,
      [
        "../app_kit/bridges/mezzanine_bridge/lib/**/*.ex",
        "../stack_lab/support/citadel_spine_harness/lib/**/*.ex"
      ],
      "Mezzanine.WorkAudit"
    )
  end

  test "bounded app-kit and harness paths no longer depend on the deprecated ops_control seam" do
    root = Path.expand("../..", __DIR__)

    assert_refutes_file_patterns(
      root,
      [
        "../app_kit/bridges/mezzanine_bridge/mix.exs",
        "../stack_lab/support/citadel_spine_harness/mix.exs"
      ],
      ":mezzanine_ops_control"
    )

    assert_refutes_file_patterns(
      root,
      [
        "../app_kit/bridges/mezzanine_bridge/lib/**/*.ex",
        "../stack_lab/support/citadel_spine_harness/lib/**/*.ex"
      ],
      "Mezzanine.Control"
    )
  end

  test "bounded app-kit and harness paths no longer depend on the deprecated ops_assurance seam" do
    root = Path.expand("../..", __DIR__)

    assert_refutes_file_patterns(
      root,
      [
        "../app_kit/bridges/mezzanine_bridge/mix.exs",
        "../stack_lab/support/citadel_spine_harness/mix.exs"
      ],
      ":mezzanine_ops_assurance"
    )

    assert_refutes_file_patterns(
      root,
      [
        "../app_kit/bridges/mezzanine_bridge/lib/**/*.ex",
        "../stack_lab/support/citadel_spine_harness/lib/**/*.ex"
      ],
      "Mezzanine.Assurance"
    )
  end

  test "engine apps that host audit-engine services still configure the ops domain repo" do
    root = Path.expand("../..", __DIR__)

    Enum.each(
      [
        "core/archival_engine",
        "core/decision_engine",
        "core/evidence_engine",
        "core/execution_engine",
        "core/object_engine",
        "core/projection_engine",
        "core/runtime_scheduler"
      ],
      fn app_path ->
        assert_file_patterns_include(
          root,
          ["#{app_path}/config/dev.exs", "#{app_path}/config/test.exs"],
          "config :mezzanine_ops_domain, Mezzanine.OpsDomain.Repo"
        )
      end
    )
  end

  test "neutral work-audit public specs do not leak hidden ops-domain types into docs" do
    path = Path.expand("../../core/audit_engine/lib/mezzanine/audit/work_audit.ex", __DIR__)
    contents = File.read!(path)

    refute contents =~ "AuditEvent.t()"
    refute contents =~ "EvidenceBundle.t()"
    refute contents =~ "TimelineProjection.t()"
  end

  test "exposes the workspace project globs" do
    assert "." in Mezzanine.Workspace.active_project_globs()
    assert "core/*" in Mezzanine.Workspace.active_project_globs()
    refute "surfaces/*" in Mezzanine.Workspace.active_project_globs()
  end

  defp assert_refutes_file_patterns(root, patterns, needle) do
    patterns
    |> Enum.flat_map(fn pattern ->
      wildcard_or_literal(root, pattern)
    end)
    |> Enum.uniq()
    |> Enum.each(fn path ->
      refute File.read!(path) =~ needle, "#{path} still references #{needle}"
    end)
  end

  defp assert_file_patterns_include(root, patterns, needle) do
    patterns
    |> Enum.flat_map(fn pattern ->
      wildcard_or_literal(root, pattern)
    end)
    |> Enum.uniq()
    |> Enum.each(fn path ->
      assert File.read!(path) =~ needle, "#{path} is missing #{needle}"
    end)
  end

  defp wildcard_or_literal(root, pattern) do
    absolute = Path.join(root, pattern)

    if String.contains?(pattern, "*") do
      Path.wildcard(absolute, match_dot: true)
    else
      [absolute]
    end
  end
end
