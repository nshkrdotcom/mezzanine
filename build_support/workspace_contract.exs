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
      delete_ready?: true,
      blocking_consumers: [],
      cutover_edge:
        "shared lower intent structs now live at Mezzanine.Intent.*; package is retained only until the final delete pass"
    },
    %{
      path: "core/ops_policy",
      delete_ready?: false,
      blocking_consumers: ["core/ops_planner", "core/ops_domain"],
      cutover_edge: "legacy policy compilation still feeds the deprecated planning/domain path"
    },
    %{
      path: "core/ops_planner",
      delete_ready?: false,
      blocking_consumers: ["core/ops_control", "core/ops_domain"],
      cutover_edge:
        "legacy work-plan derivation still feeds the deprecated control and domain path"
    },
    %{
      path: "core/ops_domain",
      delete_ready?: false,
      blocking_consumers: [
        "app_kit/bridges/mezzanine_bridge",
        "stack_lab/support/citadel_spine_harness"
      ],
      cutover_edge:
        "the deprecated repo and program/work-class tables still back the bounded app-kit bridge and proof harness state"
    },
    %{
      path: "core/ops_audit",
      delete_ready?: true,
      blocking_consumers: [],
      cutover_edge:
        "bounded bridge and review-gating consumers now bind to neutral services; package is retained only until the final delete pass"
    },
    %{
      path: "core/ops_control",
      delete_ready?: true,
      blocking_consumers: [],
      cutover_edge:
        "operator command handling now lives in Mezzanine.WorkControl and Mezzanine.OperatorActions; package is retained only until the final delete pass"
    },
    %{
      path: "core/ops_assurance",
      delete_ready?: true,
      blocking_consumers: [],
      cutover_edge:
        "review gating now lives in Mezzanine.Reviews; package is retained only until the final delete pass"
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
