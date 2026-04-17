defmodule Mezzanine.Workspace do
  @moduledoc """
  Introspection helpers for the Mezzanine workspace root.
  """

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

  @deprecated_packages [
    %{
      path: "core/ops_model",
      delete_ready?: false,
      blocking_consumers: [
        "bridges/citadel_bridge",
        "bridges/integration_bridge",
        "app_kit/bridges/mezzanine_bridge",
        "stack_lab/support/citadel_spine_harness"
      ],
      cutover_edge: "shared lower intent structs still power the lower bridges and proof harness"
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
        "extravaganza/runtime_provisioner",
        "stack_lab/support/citadel_spine_harness"
      ],
      cutover_edge:
        "the deprecated repo and program/work-class tables still back product bootstrap and proof harness state"
    },
    %{
      path: "core/ops_audit",
      delete_ready?: false,
      blocking_consumers: [
        "app_kit/bridges/mezzanine_bridge",
        "core/ops_assurance",
        "core/ops_control",
        "stack_lab/support/citadel_spine_harness"
      ],
      cutover_edge: "legacy audit assemblers still feed the old app-kit bridge and proof harness"
    },
    %{
      path: "core/ops_control",
      delete_ready?: false,
      blocking_consumers: [
        "app_kit/bridges/mezzanine_bridge",
        "stack_lab/support/citadel_spine_harness"
      ],
      cutover_edge:
        "the deprecated operator command path still flows through the bounded AppKit bridge and proof harness"
    },
    %{
      path: "core/ops_assurance",
      delete_ready?: false,
      blocking_consumers: [
        "app_kit/bridges/mezzanine_bridge",
        "stack_lab/support/citadel_spine_harness"
      ],
      cutover_edge:
        "legacy assurance logic still feeds the bounded AppKit bridge and proof harness"
    },
    %{
      path: "surfaces/program_surface",
      delete_ready?: false,
      blocking_consumers: ["extravaganza/runtime_provisioner"],
      cutover_edge:
        "Extravaganza still provisions legacy program and work-class records through this surface"
    }
  ]
  @deprecated_package_paths Enum.map(@deprecated_packages, & &1.path)
  @package_paths [@kept_package_paths, @neutral_package_paths, @deprecated_package_paths]
                 |> List.flatten()

  @active_project_globs [".", "core/*", "apps/*", "bridges/*", "surfaces/*"]
  @coexistence_gates [
    "NO_NEW_PRODUCT_DEP_ON_OLD_MEZZANINE",
    "MEZZANINE_NEUTRAL_CORE_CUTOVER"
  ]

  @type deprecated_package :: %{
          path: String.t(),
          delete_ready?: boolean(),
          blocking_consumers: [String.t()],
          cutover_edge: String.t()
        }

  @spec package_paths() :: [String.t()]
  def package_paths, do: @package_paths

  @spec kept_package_paths() :: [String.t()]
  def kept_package_paths, do: @kept_package_paths

  @spec neutral_package_paths() :: [String.t()]
  def neutral_package_paths, do: @neutral_package_paths

  @spec deprecated_packages() :: [deprecated_package()]
  def deprecated_packages, do: @deprecated_packages

  @spec deprecated_package_paths() :: [String.t()]
  def deprecated_package_paths, do: @deprecated_package_paths

  @spec delete_ready_deprecated_package_paths() :: [String.t()]
  def delete_ready_deprecated_package_paths do
    @deprecated_packages
    |> Enum.filter(& &1.delete_ready?)
    |> Enum.map(& &1.path)
  end

  @spec blocked_deprecated_packages() :: [deprecated_package()]
  def blocked_deprecated_packages do
    Enum.reject(@deprecated_packages, & &1.delete_ready?)
  end

  @spec active_project_globs() :: [String.t()]
  def active_project_globs, do: @active_project_globs

  @spec coexistence_gates() :: [String.t()]
  def coexistence_gates, do: @coexistence_gates
end
