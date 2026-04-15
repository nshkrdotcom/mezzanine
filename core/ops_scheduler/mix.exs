defmodule MezzanineOpsScheduler.MixProject do
  use Mix.Project

  def project do
    [
      app: :mezzanine_ops_scheduler,
      version: "0.1.0",
      elixir: "~> 1.19",
      start_permanent: Mix.env() == :prod,
      elixirc_paths: elixirc_paths(Mix.env()),
      deps: deps(),
      aliases: aliases(),
      description: "Scheduler runtime owner for Mezzanine durable work dispatch",
      docs: [main: "readme", extras: ["README.md"]],
      name: "Mezzanine Ops Scheduler",
      source_url: "https://github.com/nshkrdotcom/mezzanine",
      homepage_url: "https://github.com/nshkrdotcom/mezzanine",
      dialyzer: [plt_add_deps: :apps_tree]
    ]
  end

  def application do
    [
      mod: {Mezzanine.OpsScheduler.Application, []},
      extra_applications: [:logger]
    ]
  end

  def cli do
    [preferred_envs: [test: :test]]
  end

  defp elixirc_paths(:test), do: ["lib", "test/support"]
  defp elixirc_paths(_env), do: ["lib"]

  defp aliases do
    [
      setup: ["deps.get"],
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
      {:ash, "~> 3.24"},
      {:ecto_sql, "~> 3.13"},
      {:jason, "~> 1.4"},
      {:credo, "~> 1.7", only: [:dev, :test], runtime: false},
      {:dialyxir, "~> 1.4", only: [:dev, :test], runtime: false},
      {:ex_doc, "~> 0.40.1", only: :dev, runtime: false}
    ]
  end
end
