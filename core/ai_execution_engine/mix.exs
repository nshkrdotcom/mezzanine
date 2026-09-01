if bootstrap = System.get_env("MIX_WORKSPACE_OPS_BOOTSTRAP"), do: Code.require_file(bootstrap)

defmodule Mezzanine.AIExecutionEngine.MixProject do
  use Mix.Project

  def project do
    [
      app: :mezzanine_ai_execution_engine,
      version: "0.1.0",
      elixir: "~> 1.19",
      start_permanent: Mix.env() == :prod,
      deps: deps(),
      docs: [main: "readme", extras: ["README.md"]],
      dialyzer: [plt_add_deps: :apps_tree],
      name: "Mezzanine AI Execution Engine",
      description:
        "Router, optimizer, and rendered prompt handoff contracts for governed AI execution"
    ]
  end

  def application do
    [extra_applications: [:crypto, :logger]]
  end

  def cli do
    [preferred_envs: [ci: :test]]
  end

  defp deps do
    [
      {:mezzanine_context_packet_engine, path: "../context_packet_engine"},
      workspace_dep({:outer_brain_context_abi, "~> 0.1.0"}),
      workspace_dep({:outer_brain_prompting, "~> 0.1.0"}),
      workspace_dep({:jido_integration_provider_classification, "~> 0.1.0", override: true}),
      workspace_dep({:ground_plane_contracts, "~> 0.1.0"}),
      {:credo, "~> 1.7", only: [:dev, :test], runtime: false},
      {:dialyxir, "~> 1.4", only: :dev, runtime: false},
      {:ex_doc, "~> 0.40.1", only: :dev, runtime: false}
    ]
  end

  defp workspace_dep(committed) do
    if function_exported?(MixWorkspaceOpsBootstrap, :dep, 2),
      do: apply(MixWorkspaceOpsBootstrap, :dep, [committed, __DIR__]),
      else: committed
  end
end
