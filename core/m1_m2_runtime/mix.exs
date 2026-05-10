defmodule MezzanineM1M2Runtime.MixProject do
  use Mix.Project

  @source_url "https://github.com/nshkrdotcom/mezzanine"

  def project do
    [
      app: :mezzanine_m1_m2_runtime,
      version: "0.1.0",
      elixir: "~> 1.19",
      start_permanent: Mix.env() == :prod,
      deps: deps(),
      dialyzer: [
        plt_add_deps: :apps_tree,
        plt_add_apps: [:mezzanine_audit_engine, :mezzanine_execution_engine]
      ],
      docs: [main: "readme", extras: ["README.md"]],
      name: "Mezzanine M1/M2 Runtime",
      description: "Deterministic M1 readback and live M2 runtime separation contracts",
      source_url: @source_url,
      homepage_url: @source_url
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
      {:mezzanine_core, path: "../mezzanine_core"},
      {:mezzanine_execution_engine, path: "../execution_engine", runtime: false},
      {:mezzanine_workflow_runtime, path: "../workflow_runtime"},
      {:mezzanine_lifecycle_engine, path: "../lifecycle_engine", runtime: false},
      {:mezzanine_projection_engine, path: "../projection_engine", runtime: false},
      {:mezzanine_audit_engine, path: "../audit_engine", runtime: false},
      {:ecto_sql, "~> 3.13"},
      {:jason, "~> 1.4"},
      {:credo, "~> 1.7", only: [:dev, :test], runtime: false},
      {:dialyxir, "~> 1.4", only: [:dev, :test], runtime: false},
      {:ex_doc, "~> 0.40.1", only: :dev, runtime: false}
    ]
  end
end
