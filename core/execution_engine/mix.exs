defmodule MezzanineExecutionEngine.MixProject do
  use Mix.Project

  @source_url "https://github.com/nshkrdotcom/mezzanine"

  def project do
    [
      app: :mezzanine_execution_engine,
      version: "0.1.0",
      elixir: "~> 1.19",
      start_permanent: Mix.env() == :prod,
      elixirc_paths: elixirc_paths(Mix.env()),
      deps: deps(),
      aliases: aliases(),
      dialyzer: [plt_add_deps: :apps_tree],
      description: "Durable execution ledger and JobOutbox-backed dispatch workers for Mezzanine",
      docs: [main: "readme", extras: ["README.md"]],
      name: "Mezzanine Execution Engine",
      source_url: @source_url,
      homepage_url: @source_url
    ]
  end

  def application do
    [
      mod: {Mezzanine.Execution.Application, []},
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
      setup: [
        "deps.get",
        "ash.setup",
        "audit.migrate",
        "object.migrate",
        "ops_domain.migrate",
        "leasing.migrate",
        "ecto.migrate"
      ],
      "ecto.setup": [
        "ecto.create",
        "audit.migrate",
        "object.migrate",
        "ops_domain.migrate",
        "leasing.migrate",
        "ecto.migrate"
      ],
      "ecto.reset": ["ecto.drop", "ecto.setup"],
      "object.migrate": [
        "ecto.migrate -r Mezzanine.Objects.Repo --migrations-path ../object_engine/priv/repo/migrations"
      ],
      "audit.migrate": [
        "ecto.migrate -r Mezzanine.Audit.Repo --migrations-path ../audit_engine/priv/repo/migrations"
      ],
      "ops_domain.migrate": [
        "ecto.migrate -r Mezzanine.OpsDomain.Repo --migrations-path ../ops_domain/priv/repo/migrations"
      ],
      "leasing.migrate": [
        "ecto.migrate -r Mezzanine.Execution.Repo --migrations-path ../leasing/priv/repo/migrations"
      ],
      test: [
        "ash.setup --quiet",
        "audit.migrate",
        "object.migrate",
        "ops_domain.migrate",
        "leasing.migrate",
        "ecto.migrate",
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
      {:mezzanine_leasing, path: "../leasing"},
      {:mezzanine_core, path: "../mezzanine_core"},
      {:mezzanine_object_engine, path: "../object_engine"},
      {:mezzanine_ops_domain, path: "../ops_domain"},
      {:ash, "~> 3.24"},
      {:ash_postgres, "~> 2.6"},
      {:telemetry, "~> 1.3"},
      {:oban, "~> 2.17"},
      {:ecto_sql, "~> 3.13"},
      {:postgrex, ">= 0.0.0"},
      {:jason, "~> 1.4"},
      {:credo, "~> 1.7", only: [:dev, :test], runtime: false},
      {:dialyxir, "~> 1.4", only: [:dev, :test], runtime: false},
      {:ex_doc, "~> 0.40.1", only: :dev, runtime: false}
    ]
  end
end
