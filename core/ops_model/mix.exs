defmodule MezzanineOpsModel.MixProject do
  use Mix.Project

  def project do
    [
      app: :mezzanine_ops_model,
      version: "0.1.0",
      elixir: "~> 1.19",
      start_permanent: Mix.env() == :prod,
      deps: deps(),
      description: "Pure operational vocabulary for the Mezzanine workspace",
      docs: [main: "readme", extras: ["README.md"]],
      name: "Mezzanine Ops Model"
    ]
  end

  def application do
    [extra_applications: [:logger]]
  end

  def cli do
    [preferred_envs: [ci: :test]]
  end

  defp deps do
    [
      {:credo, "~> 1.7", only: [:dev, :test], runtime: false},
      {:dialyxir, "~> 1.4", only: [:dev, :test], runtime: false},
      {:ex_doc, "~> 0.40.1", only: :dev, runtime: false}
    ]
  end
end
