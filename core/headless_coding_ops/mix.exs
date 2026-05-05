defmodule MezzanineHeadlessCodingOps.MixProject do
  use Mix.Project

  @source_url "https://github.com/nshkrdotcom/mezzanine"

  def project do
    [
      app: :mezzanine_headless_coding_ops,
      version: "0.1.0",
      elixir: "~> 1.19",
      start_permanent: Mix.env() == :prod,
      deps: deps(),
      docs: [main: "readme", extras: ["README.md"]],
      name: "Mezzanine Headless Coding Ops",
      description: "Headless coding-ops intake, readback, operator controls, and receipts",
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
      {:mezzanine_core, path: "../mezzanine_core", runtime: false},
      {:mezzanine_ops_domain, path: "../ops_domain", runtime: false},
      {:mezzanine_ops_model, path: "../ops_model", runtime: false},
      {:mezzanine_runtime_scheduler, path: "../runtime_scheduler", runtime: false},
      {:mezzanine_execution_engine, path: "../execution_engine", runtime: false},
      {:mezzanine_workflow_runtime, path: "../workflow_runtime", runtime: false},
      {:mezzanine_lifecycle_engine, path: "../lifecycle_engine", runtime: false},
      {:mezzanine_m1_m2_runtime, path: "../m1_m2_runtime", runtime: false},
      {:mezzanine_audit_engine, path: "../audit_engine", runtime: false},
      {:mezzanine_leasing, path: "../leasing", runtime: false},
      {:credo, "~> 1.7", only: [:dev, :test], runtime: false},
      {:dialyxir, "~> 1.4", only: [:dev, :test], runtime: false},
      {:ex_doc, "~> 0.40.1", only: :dev, runtime: false}
    ]
  end
end
