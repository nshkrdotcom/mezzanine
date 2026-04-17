defmodule MezzanineIntegrationBridge.MixProject do
  use Mix.Project

  def project do
    [
      app: :mezzanine_integration_bridge,
      version: "0.1.0",
      elixir: "~> 1.19",
      start_permanent: Mix.env() == :prod,
      deps: deps(),
      aliases: aliases(),
      description: "Direct jido_integration bridge for Mezzanine intents",
      docs: [main: "readme", extras: ["README.md"]],
      name: "Mezzanine Integration Bridge",
      source_url: "https://github.com/nshkrdotcom/mezzanine",
      homepage_url: "https://github.com/nshkrdotcom/mezzanine",
      dialyzer: [plt_add_deps: :apps_tree]
    ]
  end

  def application do
    [extra_applications: [:logger]]
  end

  def cli do
    [preferred_envs: [test: :test, ci: :test]]
  end

  defp aliases do
    [
      setup: ["deps.get", "ash.setup"],
      test: ["ash.setup --quiet", "test"],
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
      {:mezzanine_core, path: "../../core/mezzanine_core"},
      {:mezzanine_audit_engine, path: "../../core/audit_engine"},
      {:jido_integration_v2, path: "../../../jido_integration/core/platform"},
      {:credo, "~> 1.7", only: [:dev, :test], runtime: false},
      {:dialyxir, "~> 1.4", only: [:dev, :test], runtime: false},
      {:ex_doc, "~> 0.40.1", only: :dev, runtime: false}
    ]
  end
end
