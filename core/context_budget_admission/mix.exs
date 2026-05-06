defmodule Mezzanine.ContextBudgetAdmission.MixProject do
  use Mix.Project

  def project do
    [
      app: :mezzanine_context_budget_admission,
      version: "0.1.0",
      elixir: "~> 1.19",
      start_permanent: Mix.env() == :prod,
      deps: deps(),
      docs: [main: "readme", extras: ["README.md"]],
      dialyzer: [plt_add_deps: :apps_tree],
      name: "Mezzanine Context Budget Admission",
      description: "Fail-closed budget admission gates for governed AI effects"
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
      {:outer_brain_context_budget, path: "../../../outer_brain/core/context_budget"},
      {:outer_brain_memory_contracts, path: "../../../outer_brain/core/memory_contracts"},
      {:credo, "~> 1.7", only: [:dev, :test], runtime: false},
      {:dialyxir, "~> 1.4", only: :dev, runtime: false},
      {:ex_doc, "~> 0.40.1", only: :dev, runtime: false}
    ]
  end
end
