defmodule MezzanineOpsControl.MixProject do
  use Mix.Project

  def project do
    [
      app: :mezzanine_ops_control,
      version: "0.1.0",
      elixir: "~> 1.19",
      start_permanent: Mix.env() == :prod,
      deps: deps(),
      aliases: aliases(),
      description: "Operator control services for Mezzanine governed work",
      docs: [main: "readme", extras: ["README.md"]],
      name: "Mezzanine Ops Control",
      source_url: "https://github.com/nshkrdotcom/mezzanine",
      homepage_url: "https://github.com/nshkrdotcom/mezzanine",
      dialyzer: [plt_add_deps: :apps_tree, plt_add_apps: [:mezzanine_execution_engine]]
    ]
  end

  def application do
    [extra_applications: [:logger]]
  end

  def cli do
    [preferred_envs: [test: :test]]
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
      {:mezzanine_ops_model, path: "../ops_model"},
      {:mezzanine_ops_planner, path: "../ops_planner"},
      {:mezzanine_ops_domain, path: "../ops_domain"},
      {:mezzanine_ops_audit, path: "../ops_audit"},
      {:mezzanine_execution_engine, path: "../execution_engine", runtime: false},
      {:ash, "~> 3.24"},
      {:credo, "~> 1.7", only: [:dev, :test], runtime: false},
      {:dialyxir, "~> 1.4", only: [:dev, :test], runtime: false},
      {:ex_doc, "~> 0.40.1", only: :dev, runtime: false}
    ]
  end
end
