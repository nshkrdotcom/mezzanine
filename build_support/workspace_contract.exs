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

  @deprecated_package_paths [
    "core/ops_model",
    "core/ops_policy",
    "core/ops_planner",
    "core/ops_domain",
    "core/ops_scheduler",
    "core/ops_audit",
    "core/ops_control",
    "core/ops_assurance",
    "bridges/app_kit_bridge",
    "surfaces/work_surface",
    "surfaces/operator_surface",
    "surfaces/review_surface",
    "surfaces/program_surface"
  ]
  @package_paths [@kept_package_paths, @neutral_package_paths, @deprecated_package_paths]
                 |> List.flatten()

  @active_project_globs [".", "core/*", "apps/*", "bridges/*", "surfaces/*"]

  def package_paths, do: @package_paths
  def active_project_globs, do: @active_project_globs
end
