defmodule MezzanineWorkspaceBuildModel.MixProject do
  use Mix.Project

  @source_url "https://github.com/nshkrdotcom/mezzanine"

  def project do
    [
      app: :mezzanine_workspace_build_model,
      version: "0.1.0",
      elixir: "~> 1.19",
      start_permanent: Mix.env() == :prod,
      deps: deps(),
      docs: [main: "readme", extras: ["README.md"]],
      name: "Mezzanine Workspace Build Model",
      description: "Workspace-to-runtime manifests and no-secret build contracts",
      source_url: @source_url,
      homepage_url: @source_url
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
      {:mezzanine_core, path: "../mezzanine_core", runtime: false},
      {:mezzanine_ops_model, path: "../ops_model", runtime: false},
      {:mezzanine_ops_domain, path: "../ops_domain", runtime: false},
      {:mezzanine_pack_model, path: "../pack_model", runtime: false},
      {:mezzanine_audit_engine, path: "../audit_engine", runtime: false},
      {:credo, "~> 1.7", only: [:dev, :test], runtime: false},
      {:dialyxir, "~> 1.4", only: [:dev, :test], runtime: false},
      {:ex_doc, "~> 0.40.1", only: :dev, runtime: false}
    ]
  end
end
