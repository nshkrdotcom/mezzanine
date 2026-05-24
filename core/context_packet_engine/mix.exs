unless Code.ensure_loaded?(DependencySources) do
  Code.require_file("../../build_support/dependency_sources.exs", __DIR__)
end

defmodule Mezzanine.ContextPacketEngine.MixProject do
  use Mix.Project

  @repo_root Path.expand("../..", __DIR__)

  def project do
    [
      app: :mezzanine_context_packet_engine,
      version: "0.1.0",
      elixir: "~> 1.19",
      start_permanent: Mix.env() == :prod,
      deps: deps(),
      docs: [main: "readme", extras: ["README.md"]],
      dialyzer: [plt_add_deps: :apps_tree],
      name: "Mezzanine Context Packet Engine",
      description: "Ref-only admission receipts for OuterBrain Context ABI packets"
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
      DependencySources.dep(:outer_brain_context_abi, @repo_root),
      DependencySources.dep(:citadel_authority_contract, @repo_root, override: true),
      DependencySources.dep(:citadel_contract_core, @repo_root, override: true),
      DependencySources.dep(:citadel_context_authority_contract, @repo_root),
      DependencySources.dep(:ground_plane_contracts, @repo_root),
      {:credo, "~> 1.7", only: [:dev, :test], runtime: false},
      {:dialyxir, "~> 1.4", only: :dev, runtime: false},
      {:ex_doc, "~> 0.40.1", only: :dev, runtime: false}
    ]
  end
end
