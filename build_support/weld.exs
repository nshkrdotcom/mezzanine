Code.require_file("workspace_contract.exs", __DIR__)

defmodule Mezzanine.Build.WeldContract do
  @moduledoc false

  @repo_root Path.expand("..", __DIR__)
  @citadel_repo_path Path.expand("../citadel", @repo_root)
  @execution_plane_repo_path Path.expand("../execution_plane", @repo_root)
  @gepa_framework_repo_path Path.expand("../gepa_framework", @repo_root)
  @trinity_framework_repo_path Path.expand("../trinity_framework", @repo_root)
  @jido_hive_repo_path Path.expand("../jido_hive", @repo_root)
  @outer_brain_repo_path Path.expand("../outer_brain", @repo_root)
  @aitrace_repo_path Path.expand("../AITrace", @repo_root)
  @temporalex_repo_path Path.expand("../temporalex", @repo_root)

  @dependencies [
    citadel_governance: [
      opts:
        if File.dir?(@citadel_repo_path) do
          [git: @citadel_repo_path, subdir: "core/citadel_governance"]
        else
          [
            github: "nshkrdotcom/citadel",
            branch: "main",
            subdir: "core/citadel_governance"
          ]
        end
    ],
    execution_plane: [
      opts:
        if File.dir?(@execution_plane_repo_path) do
          [git: @execution_plane_repo_path, subdir: "core/execution_plane", override: true]
        else
          [
            github: "nshkrdotcom/execution_plane",
            branch: "main",
            subdir: "core/execution_plane",
            override: true
          ]
        end
    ],
    outer_brain_context_budget: [
      opts:
        if File.dir?(@outer_brain_repo_path) do
          [git: @outer_brain_repo_path, sparse: "core/context_budget"]
        else
          [
            github: "nshkrdotcom/outer_brain",
            branch: "main",
            sparse: "core/context_budget"
          ]
        end
    ],
    outer_brain_memory_contracts: [
      opts:
        if File.dir?(@outer_brain_repo_path) do
          [git: @outer_brain_repo_path, sparse: "core/memory_contracts"]
        else
          [
            github: "nshkrdotcom/outer_brain",
            branch: "main",
            sparse: "core/memory_contracts"
          ]
        end
    ],
    outer_brain_token_meter: [
      opts:
        if File.dir?(@outer_brain_repo_path) do
          [git: @outer_brain_repo_path, sparse: "core/token_meter"]
        else
          [
            github: "nshkrdotcom/outer_brain",
            branch: "main",
            sparse: "core/token_meter"
          ]
        end
    ],
    ai_trace_replay_contracts: [
      opts:
        if File.dir?(@aitrace_repo_path) do
          [git: @aitrace_repo_path, sparse: "core/replay_contracts"]
        else
          [
            github: "nshkrdotcom/AITrace",
            branch: "main",
            sparse: "core/replay_contracts"
          ]
        end
    ],
    gepa_framework: [
      opts:
        if File.dir?(@gepa_framework_repo_path) do
          [git: @gepa_framework_repo_path]
        else
          [github: "nshkrdotcom/gepa_framework", branch: "main"]
        end
    ],
    trinity_framework: [
      opts:
        if File.dir?(@trinity_framework_repo_path) do
          [git: @trinity_framework_repo_path]
        else
          [github: "nshkrdotcom/trinity_framework", branch: "main"]
        end
    ],
    jido_hive_coordination_patterns: [
      opts:
        if File.dir?(@jido_hive_repo_path) do
          [git: @jido_hive_repo_path, subdir: "core/coordination_patterns"]
        else
          [
            github: "nshkrdotcom/jido_hive",
            branch: "main",
            subdir: "core/coordination_patterns"
          ]
        end
    ],
    jido_hive_inter_agent_messaging: [
      opts:
        if File.dir?(@jido_hive_repo_path) do
          [git: @jido_hive_repo_path, subdir: "core/inter_agent_messaging"]
        else
          [
            github: "nshkrdotcom/jido_hive",
            branch: "main",
            subdir: "core/inter_agent_messaging"
          ]
        end
    ],
    temporalex: [
      opts:
        if File.dir?(@temporalex_repo_path) do
          [git: @temporalex_repo_path]
        else
          [github: "nshkrdotcom/temporalex", branch: "main"]
        end
    ]
  ]

  @artifact_docs [
    "README.md",
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
    "core/ai_run_model/README.md",
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
    "docs/publication.md",
    "docs/roadmap.md"
  ]

  def manifest do
    [
      workspace: [
        root: "..",
        project_globs: Mezzanine.Build.WorkspaceContract.active_project_globs()
      ],
      classify: [
        tooling: ["."]
      ],
      publication: [
        internal_only: [
          ".",
          "core/context_budget_admission",
          "core/ai_run_model",
          "core/cost_attribution_engine",
          "core/budget_enforcement_engine",
          "core/eval_engine",
          "core/optimization_engine",
          "core/coordination_engine"
        ]
      ],
      dependencies: @dependencies,
      artifacts: [
        mezzanine_core: artifact()
      ]
    ]
  end

  def artifact do
    [
      roots: [
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
        "core/ai_run_model",
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
end

Mezzanine.Build.WeldContract.manifest()
