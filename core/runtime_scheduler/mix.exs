defmodule MezzanineRuntimeScheduler.MixProject do
  use Mix.Project

  @source_url "https://github.com/nshkrdotcom/mezzanine"

  def project do
    [
      app: :mezzanine_runtime_scheduler,
      version: "0.1.0",
      elixir: "~> 1.19",
      start_permanent: Mix.env() == :prod,
      elixirc_paths: elixirc_paths(Mix.env()),
      deps: deps(),
      aliases: aliases(),
      dialyzer: [plt_add_deps: :apps_tree],
      docs: [main: "readme", extras: ["README.md"]],
      name: "Mezzanine Runtime Scheduler",
      description: "Installation-scoped retry timing and restart recovery for Mezzanine",
      source_url: @source_url,
      homepage_url: @source_url
    ]
  end

  def application do
    [
      extra_applications: [:logger, :runtime_tools],
      mod: {Mezzanine.RuntimeScheduler.Application, []}
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
        "ash.setup",
        "runtime_scheduler.migrate",
        "audit.migrate",
        "object.migrate",
        "execution.migrate"
      ],
      "ecto.setup": ["setup"],
      "ecto.reset": ["ecto.drop", "setup"],
      "runtime_scheduler.migrate": [
        "ecto.migrate -r Mezzanine.RuntimeScheduler.Repo --migrations-path priv/repo/migrations"
      ],
      "execution.migrate": [
        "ecto.migrate -r Mezzanine.Execution.Repo --migrations-path ../execution_engine/priv/repo/migrations"
      ],
      "object.migrate": [
        "ecto.migrate -r Mezzanine.Objects.Repo --migrations-path ../object_engine/priv/repo/migrations"
      ],
      "audit.migrate": [
        "ecto.migrate -r Mezzanine.Audit.Repo --migrations-path ../audit_engine/priv/repo/migrations"
      ],
      test: [
        "ash.setup --quiet",
        "runtime_scheduler.migrate",
        "audit.migrate",
        "object.migrate",
        "execution.migrate",
        "test"
      ],
      ci: [
        "format --check-formatted",
        "compile --warnings-as-errors",
        "cmd env MIX_ENV=test mix test",
        "credo --strict",
        "cmd env MIX_ENV=dev mix dialyzer --force-check",
        "cmd env MIX_ENV=dev mix docs --warnings-as-errors"
      ]
    ]
  end

  defp deps do
    [
      {:mezzanine_audit_engine, path: "../audit_engine"},
      {:mezzanine_core, path: "../mezzanine_core"},
      {:mezzanine_object_engine, path: "../object_engine"},
      {:mezzanine_execution_engine, path: "../execution_engine"},
      {:mezzanine_lifecycle_engine, path: "../lifecycle_engine"},
      {:ash, "~> 3.24"},
      {:ash_postgres, "~> 2.6"},
      {:telemetry, "~> 1.3"},
      {:ecto_sql, "~> 3.13"},
      {:postgrex, ">= 0.0.0"},
      {:jason, "~> 1.4"},
      {:credo, "~> 1.7", only: [:dev, :test], runtime: false},
      {:dialyxir, "~> 1.4", only: [:dev, :test], runtime: false},
      {:ex_doc, "~> 0.40.1", only: :dev, runtime: false}
    ]
  end
end
