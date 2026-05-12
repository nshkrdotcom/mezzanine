repo_root = Path.expand("..", __DIR__)
siblings_root = Path.expand("..", repo_root)

dep = fn repo, subdir, hex ->
  %{
    path: Path.join(siblings_root, "#{repo}/#{subdir}"),
    github: %{repo: "nshkrdotcom/#{repo}", branch: "main", subdir: subdir},
    hex: hex,
    default_order: [:path, :github, :hex],
    publish_order: [:hex]
  }
end

root_dep = fn repo, hex ->
  %{
    path: Path.join(siblings_root, repo),
    github: %{repo: "nshkrdotcom/#{repo}", branch: "main"},
    hex: hex,
    default_order: [:path, :github, :hex],
    publish_order: [:hex]
  }
end

%{
  deps: %{
    ai_trace_replay_contracts: dep.("AITrace", "core/replay_contracts", "~> 0.1.0"),
    citadel_governance: dep.("citadel", "core/citadel_governance", "~> 0.1.0"),
    execution_plane: %{
      path: Path.join(siblings_root, "execution_plane/core/execution_plane"),
      github: %{
        repo: "nshkrdotcom/execution_plane",
        branch: "main",
        subdir: "core/execution_plane"
      },
      hex: "~> 0.1.0",
      default_order: [:github, :hex, :path],
      publish_order: [:hex]
    },
    gepa_framework: root_dep.("gepa_framework", "~> 0.1.0"),
    ground_plane_persistence_policy: dep.("ground_plane", "core/persistence_policy", "~> 0.1.0"),
    jido_hive_coordination_patterns: dep.("jido_hive", "core/coordination_patterns", "~> 0.1.0"),
    jido_hive_inter_agent_messaging: dep.("jido_hive", "core/inter_agent_messaging", "~> 0.1.0"),
    jido_integration_v2: dep.("jido_integration", "core/platform", "~> 0.1.0"),
    jido_integration_v2_codex_cli: dep.("jido_integration", "connectors/codex_cli", "~> 0.1.0"),
    jido_integration_v2_github: dep.("jido_integration", "connectors/github", "~> 0.1.0"),
    jido_integration_v2_linear: dep.("jido_integration", "connectors/linear", "~> 0.1.0"),
    jido_integration_v2_runtime_router:
      dep.("jido_integration", "core/runtime_router", "~> 0.1.0"),
    outer_brain_context_budget: dep.("outer_brain", "core/context_budget", "~> 0.1.0"),
    outer_brain_memory_contracts: dep.("outer_brain", "core/memory_contracts", "~> 0.1.0"),
    outer_brain_token_meter: dep.("outer_brain", "core/token_meter", "~> 0.1.0"),
    temporalex: root_dep.("temporalex", "~> 0.1.0"),
    trinity_framework: root_dep.("trinity_framework", "~> 0.1.0")
  }
}
