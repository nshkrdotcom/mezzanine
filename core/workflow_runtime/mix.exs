unless Code.ensure_loaded?(DependencySources) do
  Code.require_file("../../build_support/dependency_sources.exs", __DIR__)
end

defmodule MezzanineWorkflowRuntime.MixProject do
  use Mix.Project

  @source_url "https://github.com/nshkrdotcom/mezzanine"
  @repo_root Path.expand("../..", __DIR__)

  def project do
    [
      app: :mezzanine_workflow_runtime,
      version: "0.1.0",
      elixir: "~> 1.19",
      start_permanent: Mix.env() == :prod,
      deps: deps(),
      aliases: aliases(),
      dialyzer: [plt_add_deps: :apps_tree],
      description: "Temporal workflow runtime boundary for Mezzanine",
      docs: [main: "readme", extras: ["README.md"]],
      name: "Mezzanine Workflow Runtime",
      source_url: @source_url,
      homepage_url: @source_url
    ]
  end

  def application do
    [
      extra_applications: [:logger],
      mod: {Mezzanine.WorkflowRuntime.Application, []}
    ]
  end

  def cli do
    [preferred_envs: [ci: :test]]
  end

  defp aliases do
    [
      ci: [
        "format --check-formatted",
        "compile --warnings-as-errors",
        "cmd env MIX_ENV=test mix test",
        "credo --strict",
        "cmd env MIX_ENV=dev mix dialyzer",
        "cmd env MIX_ENV=dev mix docs --warnings-as-errors"
      ]
    ]
  end

  defp deps do
    [
      {:mezzanine_core, path: "../mezzanine_core"},
      {:mezzanine_execution_engine, path: "../execution_engine"},
      {:mezzanine_runtime_profile, path: "../runtime_profile"},
      {:mezzanine_citadel_bridge, path: "../../bridges/citadel_bridge"},
      {:mezzanine_workspace_engine, path: "../workspace_engine"},
      DependencySources.dep(:ground_plane_persistence_policy, @repo_root, override: true),
      DependencySources.dep(:temporalex, @repo_root),
      {:ecto_sql, "~> 3.13"},
      {:oban, "~> 2.17"},
      {:opentelemetry, "~> 1.5"},
      {:opentelemetry_api, "~> 1.4"},
      {:opentelemetry_exporter, "~> 1.8"},
      {:opentelemetry_semantic_conventions, "~> 1.27"},
      {:credo, "~> 1.7", only: [:dev, :test], runtime: false},
      {:dialyxir, "~> 1.4", only: [:dev, :test], runtime: false},
      {:ex_doc, "~> 0.40.1", only: :dev, runtime: false}
    ]
  end
end
