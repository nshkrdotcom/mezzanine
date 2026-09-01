if bootstrap = System.get_env("MIX_WORKSPACE_OPS_BOOTSTRAP"), do: Code.require_file(bootstrap)

defmodule Mezzanine.ChassisBridge.MixProject do
  use Mix.Project

  def project do
    [
      app: :mezzanine_chassis_bridge,
      version: "0.1.0",
      elixir: "~> 1.19",
      deps: deps(),
      description: "Mezzanine workflows and read projections for Chassis"
    ]
  end

  def application, do: [extra_applications: [:logger]]

  def cli do
    [preferred_envs: [ci: :test]]
  end

  defp deps do
    [
      workspace_dep({:chassis_mezzanine_bridge, "~> 0.1.0"}),
      {:credo, "~> 1.7", only: [:dev, :test], runtime: false},
      {:dialyxir, "~> 1.4", only: [:dev, :test], runtime: false},
      {:ex_doc, "~> 0.40.1", only: :dev, runtime: false}
    ]
  end

  defp workspace_dep(committed) do
    if function_exported?(MixWorkspaceOpsBootstrap, :dep, 2),
      do: apply(MixWorkspaceOpsBootstrap, :dep, [committed, __DIR__]),
      else: committed
  end
end
