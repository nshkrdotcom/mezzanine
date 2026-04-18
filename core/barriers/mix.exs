defmodule MezzanineBarriers.MixProject do
  use Mix.Project

  @source_url "https://github.com/nshkrdotcom/mezzanine"

  def project do
    [
      app: :mezzanine_barriers,
      version: "0.1.0",
      elixir: "~> 1.19",
      start_permanent: Mix.env() == :prod,
      elixirc_paths: elixirc_paths(Mix.env()),
      deps: deps(),
      aliases: aliases(),
      dialyzer: [plt_add_deps: :apps_tree],
      description: "Durable barrier ledger and exact-close primitives for Mezzanine",
      docs: [main: "readme", extras: ["README.md"]],
      name: "Mezzanine Barriers",
      source_url: @source_url,
      homepage_url: @source_url
    ]
  end

  def application do
    [
      extra_applications: [:logger]
    ]
  end

  def cli do
    [preferred_envs: [ci: :test]]
  end

  defp elixirc_paths(:test), do: ["lib", "test/support"]
  defp elixirc_paths(_env), do: ["lib"]

  defp aliases do
    [
      setup: [
        "deps.get",
        "ecto.create -r Mezzanine.Execution.Repo",
        "execution.migrate",
        "barrier.migrate"
      ],
      "ecto.setup": [
        "ecto.create -r Mezzanine.Execution.Repo",
        "execution.migrate",
        "barrier.migrate"
      ],
      "ecto.reset": [
        "ecto.drop -r Mezzanine.Execution.Repo",
        "ecto.setup"
      ],
      "execution.migrate": [
        "ecto.migrate -r Mezzanine.Execution.Repo --migrations-path ../execution_engine/priv/repo/migrations"
      ],
      "barrier.migrate": [
        "ecto.migrate -r Mezzanine.Execution.Repo --migrations-path priv/repo/migrations"
      ],
      test: [
        "ecto.create -r Mezzanine.Execution.Repo --quiet",
        "execution.migrate",
        "barrier.migrate",
        "test"
      ],
      ci: [
        "format --check-formatted",
        "compile --warnings-as-errors",
        "test",
        "credo --strict",
        "cmd env MIX_ENV=dev mix dialyzer --force-check",
        "cmd env MIX_ENV=dev mix docs --warnings-as-errors"
      ]
    ]
  end

  defp deps do
    [
      {:mezzanine_core, path: "../mezzanine_core"},
      {:mezzanine_execution_engine, path: "../execution_engine"},
      {:telemetry, "~> 1.3"},
      {:ecto_sql, "~> 3.13"},
      {:postgrex, ">= 0.0.0"},
      {:credo, "~> 1.7", only: [:dev, :test], runtime: false},
      {:dialyxir, "~> 1.4", only: [:dev, :test], runtime: false},
      {:ex_doc, "~> 0.40.1", only: :dev, runtime: false}
    ]
  end
end
