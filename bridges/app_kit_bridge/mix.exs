defmodule MezzanineAppKitBridge.MixProject do
  use Mix.Project

  def project do
    [
      app: :mezzanine_app_kit_bridge,
      version: "0.1.0",
      elixir: "~> 1.19",
      start_permanent: Mix.env() == :prod,
      deps: deps(),
      aliases: aliases(),
      description: "AppKit backend adapters backed by Mezzanine service seams",
      docs: [main: "readme", extras: ["README.md"]],
      name: "Mezzanine AppKit Bridge",
      source_url: "https://github.com/nshkrdotcom/mezzanine",
      homepage_url: "https://github.com/nshkrdotcom/mezzanine",
      dialyzer: [plt_add_deps: :apps_tree]
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
      {:mezzanine_ops_domain, path: "../../core/ops_domain"},
      {:mezzanine_ops_audit, path: "../../core/ops_audit"},
      {:mezzanine_ops_assurance, path: "../../core/ops_assurance"},
      {:mezzanine_ops_control, path: "../../core/ops_control"},
      {:mezzanine_audit_engine, path: "../../core/audit_engine"},
      {:mezzanine_execution_engine, path: "../../core/execution_engine"},
      {:mezzanine_decision_engine, path: "../../core/decision_engine"},
      {:mezzanine_evidence_engine, path: "../../core/evidence_engine"},
      {:mezzanine_ops_model, path: "../../core/ops_model"},
      {:mezzanine_integration_bridge, path: "../integration_bridge"},
      {:app_kit_core, path: "../../../app_kit/core/app_kit_core"},
      {:app_kit_run_governance, path: "../../../app_kit/core/run_governance"},
      {:ash, "~> 3.24"},
      {:credo, "~> 1.7", only: [:dev, :test], runtime: false},
      {:dialyxir, "~> 1.4", only: [:dev, :test], runtime: false},
      {:ex_doc, "~> 0.40.1", only: :dev, runtime: false}
    ]
  end
end
