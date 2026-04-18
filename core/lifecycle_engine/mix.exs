defmodule MezzanineLifecycleEngine.MixProject do
  use Mix.Project

  @source_url "https://github.com/nshkrdotcom/mezzanine"

  def project do
    [
      app: :mezzanine_lifecycle_engine,
      version: "0.1.0",
      elixir: "~> 1.19",
      start_permanent: Mix.env() == :prod,
      elixirc_paths: elixirc_paths(Mix.env()),
      deps: deps(),
      aliases: aliases(),
      dialyzer: [plt_add_deps: :apps_tree],
      description: "Durable lifecycle coordinator for explicit mezzanine execution requests",
      docs: [main: "readme", extras: ["README.md"]],
      name: "Mezzanine Lifecycle Engine",
      source_url: @source_url,
      homepage_url: @source_url
    ]
  end

  def application do
    [
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
        "ecto.create",
        "ash.setup",
        "audit.migrate",
        "object.migrate",
        "config_registry.migrate",
        "execution.migrate"
      ],
      "ecto.setup": [
        "ecto.create",
        "audit.migrate",
        "object.migrate",
        "config_registry.migrate",
        "execution.migrate"
      ],
      "ecto.reset": ["ecto.drop", "ecto.setup"],
      "audit.migrate": [
        "ecto.migrate -r Mezzanine.Audit.Repo --migrations-path ../audit_engine/priv/repo/migrations"
      ],
      "object.migrate": [
        "ecto.migrate -r Mezzanine.Objects.Repo --migrations-path ../object_engine/priv/repo/migrations"
      ],
      "config_registry.migrate": [
        "ecto.migrate -r Mezzanine.ConfigRegistry.Repo --migrations-path ../config_registry/priv/repo/migrations"
      ],
      "barrier.migrate": [
        "ecto.migrate -r Mezzanine.Execution.Repo --migrations-path ../barriers/priv/repo/migrations"
      ],
      "execution.migrate": [
        "ecto.migrate -r Mezzanine.Execution.Repo --migrations-path ../execution_engine/priv/repo/migrations"
      ],
      test: [
        "ecto.create --quiet",
        "ash.setup --quiet",
        "audit.migrate",
        "object.migrate",
        "config_registry.migrate",
        "barrier.migrate",
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
      {:mezzanine_barriers, path: "../barriers"},
      {:mezzanine_config_registry, path: "../config_registry"},
      {:mezzanine_pack_compiler, path: "../pack_compiler"},
      {:ash, "~> 3.24"},
      {:telemetry, "~> 1.3"},
      {:ecto_sql, "~> 3.13"},
      {:postgrex, ">= 0.0.0"},
      {:oban, "~> 2.17"},
      {:jason, "~> 1.4"},
      {:credo, "~> 1.7", only: [:dev, :test], runtime: false},
      {:dialyxir, "~> 1.4", only: [:dev, :test], runtime: false},
      {:ex_doc, "~> 0.40.1", only: :dev, runtime: false}
    ]
  end
end
