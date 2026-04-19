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
    "core/barriers",
    "core/leasing",
    "core/lifecycle_engine",
    "core/config_registry",
    "core/object_engine",
    "core/execution_engine",
    "core/runtime_scheduler",
    "core/workflow_runtime",
    "core/decision_engine",
    "core/evidence_engine",
    "core/projection_engine",
    "core/operator_engine",
    "core/audit_engine",
    "core/archival_engine"
  ]

  @semantic_host_packages [
    %{
      path: "core/ops_model",
      current_role:
        "typed semantic structs still consumed by core/ops_domain while the later neutral migration is pending"
    },
    %{
      path: "core/ops_domain",
      current_role:
        "live program, work, run, review, evidence, and control domains still consumed by app_kit, extravaganza, and stack_lab"
    }
  ]
  @semantic_host_package_paths Enum.map(@semantic_host_packages, & &1.path)
  @package_paths [@kept_package_paths, @neutral_package_paths, @semantic_host_package_paths]
                 |> List.flatten()

  @active_project_globs [".", "core/*", "apps/*", "bridges/*"]

  def package_paths, do: @package_paths
  def semantic_host_packages, do: @semantic_host_packages
  def semantic_host_package_paths, do: @semantic_host_package_paths
  def active_project_globs, do: @active_project_globs
end
