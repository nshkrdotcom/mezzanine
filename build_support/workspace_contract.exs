defmodule Mezzanine.Build.WorkspaceContract do
  @moduledoc false

  @kept_package_paths [
    "core/mezzanine_core",
    "bridges/citadel_bridge",
    "bridges/integration_bridge"
  ]

  @neutral_package_paths [
    "core/pack_model",
    "core/pack_compiler",
    "core/config_registry",
    "core/object_engine",
    "core/execution_engine",
    "core/runtime_scheduler",
    "core/decision_engine",
    "core/evidence_engine",
    "core/projection_engine",
    "core/operator_engine",
    "core/audit_engine",
    "core/archival_engine"
  ]

  # Keep this inventory aligned with `Mezzanine.Workspace`; the root workspace
  # test suite asserts parity so the build contract and runtime helper cannot
  # drift while the deprecated ontology is being dismantled.
  @deprecated_packages [
    %{
      path: "core/ops_model",
      delete_ready?: false,
      blocking_consumers: ["core/ops_domain"],
      cutover_edge:
        "shared lower intent structs now live at Mezzanine.Intent.*; residual internal planning still uses MezzanineOpsModel through core/ops_domain"
    },
    %{
      path: "core/ops_domain",
      delete_ready?: false,
      blocking_consumers: ["core/execution_engine"],
      cutover_edge:
        "execution_engine still hosts the deprecated repo wiring and legacy resources that back the new neutral lower facades; residual policy/planner helpers now live inside core/ops_domain"
    }
  ]
  @deprecated_package_paths Enum.map(@deprecated_packages, & &1.path)
  @package_paths [@kept_package_paths, @neutral_package_paths, @deprecated_package_paths]
                 |> List.flatten()

  @active_project_globs [".", "core/*", "apps/*", "bridges/*"]

  def package_paths, do: @package_paths
  def deprecated_packages, do: @deprecated_packages
  def deprecated_package_paths, do: @deprecated_package_paths
  def active_project_globs, do: @active_project_globs
end
