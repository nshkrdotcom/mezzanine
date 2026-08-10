unless Code.ensure_loaded?(DependencySources) do
  Code.require_file("../../build_support/dependency_sources.exs", __DIR__)
end

defmodule Mezzanine.ChassisBridge.MixProject do
  use Mix.Project

  @repo_root Path.expand("../..", __DIR__)

  def project do
    [
      app: :mezzanine_chassis_bridge,
      version: "0.1.0",
      elixir: "~> 1.19",
      deps: deps(),
      description: "Mezzanine workflows and read projections for Chassis"
    ]
  end

  def application, do: [extra_applications: [:logger]]

  def cli do
    [preferred_envs: [ci: :test]]
  end

  defp deps do
    [
      DependencySources.dep(:chassis_mezzanine_bridge, @repo_root),
      {:credo, "~> 1.7", only: [:dev, :test], runtime: false},
      {:dialyxir, "~> 1.4", only: [:dev, :test], runtime: false},
      {:ex_doc, "~> 0.40.1", only: :dev, runtime: false}
    ]
  end
end
