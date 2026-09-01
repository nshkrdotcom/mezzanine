Code.require_file("workspace_contract.exs", __DIR__)

defmodule Mezzanine.Build.WeldContract do
  @moduledoc false

  alias Mezzanine.Build.WorkspaceContract

  @manifest_dependencies [
    :citadel_authority_contract,
    :citadel_contract_core,
    :citadel_governance,
    :citadel_context_authority_contract,
    :execution_plane,
    :execution_plane_process,
    :outer_brain_context_abi,
    :outer_brain_context_budget,
    :outer_brain_memory_contracts,
    :outer_brain_prompting,
    :outer_brain_token_meter,
    :ai_trace_replay_contracts,
    :ground_plane_contracts,
    :ground_plane_persistence_policy,
    :jido_hive_coordination_patterns,
    :jido_hive_inter_agent_messaging,
    :jido_integration_provider_classification,
    :jido_integration_v2,
    :temporalex
  ]

  @dependency_coordinates %{
    citadel_authority_contract: [
      github: "nshkrdotcom/citadel",
      branch: "main",
      subdir: "core/authority_contract"
    ],
    citadel_contract_core: [
      github: "nshkrdotcom/citadel",
      branch: "main",
      subdir: "core/contract_core"
    ],
    citadel_governance: [
      github: "nshkrdotcom/citadel",
      branch: "main",
      subdir: "core/citadel_governance"
    ],
    citadel_context_authority_contract: [
      github: "nshkrdotcom/citadel",
      branch: "main",
      subdir: "core/context_authority_contract"
    ],
    execution_plane: [
      github: "nshkrdotcom/execution_plane",
      branch: "main",
      subdir: "core/execution_plane"
    ],
    execution_plane_process: [
      github: "nshkrdotcom/execution_plane",
      branch: "main",
      subdir: "runtimes/execution_plane_process"
    ],
    outer_brain_context_abi: [
      github: "nshkrdotcom/outer_brain",
      branch: "main",
      subdir: "core/context_abi"
    ],
    outer_brain_context_budget: [
      github: "nshkrdotcom/outer_brain",
      branch: "main",
      subdir: "core/context_budget"
    ],
    outer_brain_memory_contracts: [
      github: "nshkrdotcom/outer_brain",
      branch: "main",
      subdir: "core/memory_contracts"
    ],
    outer_brain_prompting: [
      github: "nshkrdotcom/outer_brain",
      branch: "main",
      subdir: "core/outer_brain_prompting"
    ],
    outer_brain_token_meter: [
      github: "nshkrdotcom/outer_brain",
      branch: "main",
      subdir: "core/token_meter"
    ],
    ai_trace_replay_contracts: [
      github: "nshkrdotcom/AITrace",
      branch: "main",
      subdir: "core/replay_contracts"
    ],
    ground_plane_contracts: [
      github: "nshkrdotcom/ground_plane",
      branch: "main",
      subdir: "core/ground_plane_contracts"
    ],
    ground_plane_persistence_policy: [
      github: "nshkrdotcom/ground_plane",
      branch: "main",
      subdir: "core/persistence_policy"
    ],
    jido_hive_coordination_patterns: [
      github: "nshkrdotcom/jido_hive",
      branch: "main",
      subdir: "core/coordination_patterns"
    ],
    jido_hive_inter_agent_messaging: [
      github: "nshkrdotcom/jido_hive",
      branch: "main",
      subdir: "core/inter_agent_messaging"
    ],
    jido_integration_provider_classification: [
      github: "agentjido/jido_integration",
      branch: "main",
      subdir: "core/provider_classification"
    ],
    jido_integration_v2: [
      github: "nshkrdotcom/jido_integration",
      branch: "main",
      subdir: "core/platform"
    ],
    temporalex: [github: "nshkrdotcom/temporalex", branch: "main"]
  }

  @manifest_dependency_opts %{
    citadel_authority_contract: [override: true],
    citadel_contract_core: [override: true],
    execution_plane: [override: true],
    execution_plane_process: [override: true],
    ground_plane_contracts: [override: true],
    ground_plane_persistence_policy: [override: true],
    jido_integration_provider_classification: [override: true],
    outer_brain_context_abi: [override: true]
  }

  @artifact_docs [
    "README.md",
    "core/substrate_model/README.md",
    "core/agent_turn_engine/README.md",
    "core/mezzanine_core/README.md",
    "core/pack_model/README.md",
    "core/pack_compiler/README.md",
    "core/config_registry/README.md",
    "core/object_engine/README.md",
    "core/execution_engine/README.md",
    "core/runtime_scheduler/README.md",
    "core/m1_m2_runtime/README.md",
    "core/headless_coding_ops/README.md",
    "core/workspace_build_model/README.md",
    "core/adaptive_control_engine/README.md",
    "core/ai_run_model/README.md",
    "core/context_packet_engine/README.md",
    "core/ai_execution_engine/README.md",
    "core/optimization_engine/README.md",
    "core/coordination_engine/README.md",
    "core/decision_engine/README.md",
    "core/evidence_engine/README.md",
    "core/projection_engine/README.md",
    "core/operator_engine/README.md",
    "core/audit_engine/README.md",
    "core/archival_engine/README.md",
    "docs/overview.md",
    "docs/layout.md",
    "docs/public_api.md",
    "docs/guides/index.md",
    "docs/guides/runtime_stack_overview.md",
    "docs/guides/work_control_run_lifecycle.md",
    "docs/guides/citadel_authority_compilation.md",
    "docs/guides/governed_lower_dispatch.md",
    "docs/guides/workflow_runtime_and_execution_lifecycle.md",
    "docs/guides/receipts_and_projections.md",
    "docs/guides/appkit_and_product_boundary.md",
    "docs/guides/local_acceptance_with_stacklab.md",
    "docs/publication.md",
    "docs/roadmap.md"
  ]

  def manifest do
    [
      workspace: [
        root: "..",
        project_globs: WorkspaceContract.active_project_globs()
      ],
      classify: [
        tooling: ["."]
      ],
      publication: [
        internal_only: [
          ".",
          "core/adaptive_control_engine",
          "core/ai_execution_engine",
          "core/context_budget_admission",
          "core/context_packet_engine",
          "core/ai_run_model",
          "core/cost_attribution_engine",
          "core/budget_enforcement_engine",
          "core/eval_engine",
          "core/optimization_engine",
          "core/coordination_engine"
        ]
      ],
      dependencies: dependencies(),
      artifacts: [
        mezzanine_core: artifact()
      ]
    ]
  end

  def artifact do
    [
      roots: [
        "core/substrate_model",
        "core/agent_turn_engine",
        "core/mezzanine_core",
        "core/pack_model",
        "core/pack_compiler",
        "core/config_registry",
        "core/object_engine",
        "core/execution_engine",
        "core/runtime_scheduler",
        "core/m1_m2_runtime",
        "core/headless_coding_ops",
        "core/workspace_build_model",
        "core/adaptive_control_engine",
        "core/ai_run_model",
        "core/context_packet_engine",
        "core/ai_execution_engine",
        "core/optimization_engine",
        "core/coordination_engine",
        "core/decision_engine",
        "core/evidence_engine",
        "core/projection_engine",
        "core/operator_engine",
        "core/audit_engine",
        "core/archival_engine"
      ],
      package: [
        name: "mezzanine_core",
        otp_app: :mezzanine_core,
        version: "0.1.0",
        docs_main: "MezzanineCore",
        description: "Projected reusable neutral core packages from the Mezzanine workspace"
      ],
      output: [
        docs: @artifact_docs,
        assets: ["CHANGELOG.md", "LICENSE"]
      ],
      verify: [
        artifact_tests: ["packaging/weld/mezzanine_core/test"],
        hex_build: false,
        hex_publish: false
      ]
    ]
  end

  defp dependencies do
    Enum.map(@manifest_dependencies, fn app ->
      {app, manifest_dependency(app)}
    end)
  end

  defp manifest_dependency(app) do
    coordinates = Map.fetch!(@dependency_coordinates, app)
    extra_opts = Map.get(@manifest_dependency_opts, app, [])

    [opts: Keyword.merge(coordinates, extra_opts)]
  end
end

Mezzanine.Build.WeldContract.manifest()
