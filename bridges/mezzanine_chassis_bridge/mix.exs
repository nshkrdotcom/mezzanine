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
      {:chassis_mezzanine_bridge, path: "../../../chassis/governance/chassis_mezzanine_bridge"},
      {:credo, "~> 1.7", only: [:dev, :test], runtime: false},
      {:dialyxir, "~> 1.4", only: [:dev, :test], runtime: false},
      {:ex_doc, "~> 0.40.1", only: :dev, runtime: false}
    ]
  end
end
