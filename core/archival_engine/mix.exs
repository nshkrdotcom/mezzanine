defmodule MezzanineArchivalEngine.MixProject do
  use Mix.Project

  @source_url "https://github.com/nshkrdotcom/mezzanine"

  def project do
    [
      app: :mezzanine_archival_engine,
      version: "0.1.0",
      elixir: "~> 1.19",
      start_permanent: Mix.env() == :prod,
      elixirc_paths: elixirc_paths(Mix.env()),
      deps: deps(),
      aliases: aliases(),
      dialyzer: [plt_add_deps: :apps_tree],
      description: "Durable archival manifests and offload contracts for Mezzanine",
      docs: [main: "readme", extras: ["README.md"]],
      name: "Mezzanine Archival Engine",
      source_url: @source_url,
      homepage_url: @source_url
    ]
  end

  def application do
    [
      mod: {Mezzanine.Archival.Application, []},
      extra_applications: [:logger, :runtime_tools]
    ]
  end

  def cli do
    [preferred_envs: [ci: :test]]
  end

  defp elixirc_paths(:test), do: ["lib", "test/support"]
  defp elixirc_paths(_env), do: ["lib"]

  defp aliases do
    [
      setup: ["deps.get", "ecto.setup"],
      "ecto.setup": [
        "ecto.create",
        "config_registry.migrate",
        "audit.migrate",
        "object.migrate",
        "execution.migrate",
        "barriers.migrate",
        "decision.migrate",
        "evidence.migrate",
        "ecto.migrate"
      ],
      "ecto.reset": ["ecto.drop", "ecto.setup"],
      "evidence.migrate": [
        "ecto.migrate -r Mezzanine.EvidenceLedger.Repo --migrations-path ../evidence_engine/priv/repo/migrations"
      ],
      "decision.migrate": [
        "ecto.migrate -r Mezzanine.Decisions.Repo --migrations-path ../decision_engine/priv/repo/migrations"
      ],
      "execution.migrate": [
        "ecto.migrate -r Mezzanine.Execution.Repo --migrations-path ../execution_engine/priv/repo/migrations"
      ],
      "barriers.migrate": [
        "ecto.migrate -r Mezzanine.Execution.Repo --migrations-path ../barriers/priv/repo/migrations"
      ],
      "config_registry.migrate": [
        "ecto.migrate -r Mezzanine.ConfigRegistry.Repo --migrations-path ../config_registry/priv/repo/migrations"
      ],
      "object.migrate": [
        "ecto.migrate -r Mezzanine.Objects.Repo --migrations-path ../object_engine/priv/repo/migrations"
      ],
      "audit.migrate": [
        "ecto.migrate -r Mezzanine.Audit.Repo --migrations-path ../audit_engine/priv/repo/migrations"
      ],
      test: [
        "ecto.create --quiet",
        "audit.migrate",
        "object.migrate",
        "execution.migrate",
        "decision.migrate",
        "evidence.migrate",
        "ecto.migrate",
        "test"
      ],
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
      {:mezzanine_config_registry, path: "../config_registry"},
      {:mezzanine_audit_engine, path: "../audit_engine"},
      {:mezzanine_object_engine, path: "../object_engine"},
      {:mezzanine_execution_engine, path: "../execution_engine"},
      {:mezzanine_decision_engine, path: "../decision_engine"},
      {:mezzanine_evidence_engine, path: "../evidence_engine"},
      {:ash, "~> 3.24"},
      {:ash_postgres, "~> 2.6"},
      {:ecto_sql, "~> 3.13"},
      {:postgrex, ">= 0.0.0"},
      {:telemetry, "~> 1.3"},
      {:jason, "~> 1.4"},
      {:credo, "~> 1.7", only: [:dev, :test], runtime: false},
      {:dialyxir, "~> 1.4", only: [:dev, :test], runtime: false},
      {:ex_doc, "~> 0.40.1", only: :dev, runtime: false}
    ]
  end
end
