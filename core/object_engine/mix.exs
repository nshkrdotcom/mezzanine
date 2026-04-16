defmodule MezzanineObjectEngine.MixProject do
  use Mix.Project

  @source_url "https://github.com/nshkrdotcom/mezzanine"

  def project do
    [
      app: :mezzanine_object_engine,
      version: "0.1.0",
      elixir: "~> 1.19",
      start_permanent: Mix.env() == :prod,
      elixirc_paths: elixirc_paths(Mix.env()),
      deps: deps(),
      aliases: aliases(),
      dialyzer: [plt_add_deps: :apps_tree],
      description: "Durable neutral subject ledger and lifecycle ownership for Mezzanine",
      docs: [main: "readme", extras: ["README.md"]],
      name: "Mezzanine Object Engine",
      source_url: @source_url,
      homepage_url: @source_url
    ]
  end

  def application do
    [
      mod: {Mezzanine.Objects.Application, []},
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
      setup: ["deps.get", "ash.setup", "audit.migrate"],
      "ecto.setup": ["ecto.create", "ecto.migrate", "audit.migrate"],
      "ecto.reset": ["ecto.drop", "ecto.setup"],
      "audit.migrate": [
        "ecto.migrate -r Mezzanine.Audit.Repo --migrations-path ../audit_engine/priv/repo/migrations"
      ],
      test: ["ash.setup --quiet", "audit.migrate", "test"],
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
      {:mezzanine_audit_engine, path: "../audit_engine"},
      {:ash, "~> 3.24"},
      {:ash_postgres, "~> 2.6"},
      {:ecto_sql, "~> 3.13"},
      {:postgrex, ">= 0.0.0"},
      {:jason, "~> 1.4"},
      {:credo, "~> 1.7", only: [:dev, :test], runtime: false},
      {:dialyxir, "~> 1.4", only: [:dev, :test], runtime: false},
      {:ex_doc, "~> 0.40.1", only: :dev, runtime: false}
    ]
  end
end
