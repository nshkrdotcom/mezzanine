unless Code.ensure_loaded?(DependencySources) do
  Code.require_file("../../build_support/dependency_sources.exs", __DIR__)
end

defmodule Mezzanine.CoordinationEngine.MixProject do
  use Mix.Project

  @repo_root Path.expand("../..", __DIR__)

  def project do
    [
      app: :mezzanine_coordination_engine,
      version: "0.1.0",
      elixir: "~> 1.18",
      start_permanent: Mix.env() == :prod,
      deps: deps(),
      aliases: aliases(),
      docs: [main: "readme", extras: ["README.md"]],
      dialyzer: [plt_add_deps: :apps_tree],
      name: "Mezzanine Coordination Engine",
      description: "Governed TRINITY coordination lifecycle and verifier orchestration"
    ]
  end

  def application do
    [extra_applications: [:logger]]
  end

  def cli do
    [preferred_envs: [ci: :test]]
  end

  defp deps do
    [
      {:mezzanine_ai_run_model, path: "../ai_run_model"},
      DependencySources.dep(:jido_hive_coordination_patterns, @repo_root),
      DependencySources.dep(:jido_hive_inter_agent_messaging, @repo_root),
      {:credo, "~> 1.7", only: [:dev, :test], runtime: false},
      {:dialyxir, "~> 1.4", only: [:dev, :test], runtime: false},
      {:ex_doc, "~> 0.40.1", only: [:dev, :test], runtime: false}
    ]
  end

  defp aliases do
    [
      ci: [
        "deps.get",
        "format --check-formatted",
        "compile --warnings-as-errors",
        "test",
        "credo --strict",
        "dialyzer --format short",
        "docs"
      ]
    ]
  end
end
