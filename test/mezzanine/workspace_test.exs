defmodule Mezzanine.WorkspaceTest do
  use ExUnit.Case, async: true

  alias Mezzanine.Build.WorkspaceContract

  test "declares the neutral package scaffold" do
    assert "core/pack_model" in Mezzanine.Workspace.neutral_package_paths()
    assert "core/pack_compiler" in Mezzanine.Workspace.neutral_package_paths()
    assert "core/barriers" in Mezzanine.Workspace.neutral_package_paths()
    assert "core/leasing" in Mezzanine.Workspace.neutral_package_paths()
    assert "core/lifecycle_engine" in Mezzanine.Workspace.neutral_package_paths()
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
    assert "core/workflow_runtime" in Mezzanine.Workspace.neutral_package_paths()
  end

  test "publishes the remaining ops packages as live semantic hosts under current repo posture" do
    assert Mezzanine.Workspace.semantic_host_packages() ==
             WorkspaceContract.semantic_host_packages()

    assert Mezzanine.Workspace.semantic_host_package_paths() ==
             WorkspaceContract.semantic_host_package_paths()

    assert "core/ops_model" in Mezzanine.Workspace.semantic_host_package_paths()
    assert "core/ops_domain" in Mezzanine.Workspace.semantic_host_package_paths()

    ops_domain =
      Enum.find(Mezzanine.Workspace.semantic_host_packages(), fn package ->
        package.path == "core/ops_domain"
      end)

    assert ops_domain.current_role =~ "live program, work, run, review, evidence, and control"
  end

  test "removes retired packages from the live workspace" do
    retired_paths = [
      "core/ops_policy",
      "core/ops_planner",
      "core/ops_audit",
      "core/ops_control",
      "core/ops_assurance",
      "bridges/app_kit_bridge",
      "surfaces/program_surface",
      "surfaces/work_surface",
      "surfaces/operator_surface",
      "surfaces/review_surface"
    ]

    assert Enum.all?(retired_paths, fn path ->
             path not in Mezzanine.Workspace.package_paths()
           end)

    assert Enum.all?(retired_paths, fn path ->
             path not in WorkspaceContract.package_paths()
           end)
  end

  test "kept lower bridges stay limited to active lower seams" do
    assert "bridges/citadel_bridge" in Mezzanine.Workspace.kept_package_paths()
    assert "bridges/integration_bridge" in Mezzanine.Workspace.kept_package_paths()
    refute "bridges/execution_plane_bridge" in Mezzanine.Workspace.kept_package_paths()
  end

  test "kept lower bridges no longer depend on the retired ops_model seam" do
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

  test "bounded app-kit and harness paths no longer depend on retired ops seams" do
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
        "../app_kit/bridges/mezzanine_bridge/mix.exs",
        "../stack_lab/support/citadel_spine_harness/mix.exs"
      ],
      ":mezzanine_ops_control"
    )

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
      "Mezzanine.WorkAudit"
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

  test "engine apps that still consume ops-domain semantics keep the repo config explicit" do
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
