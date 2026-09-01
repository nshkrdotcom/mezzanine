if bootstrap = System.get_env("MIX_WORKSPACE_OPS_BOOTSTRAP"), do: Code.require_file(bootstrap)

defmodule MezzanineCore.MixProject do
  use Mix.Project

  @source_url "https://github.com/nshkrdotcom/mezzanine"
  def project do
    [
      app: :mezzanine_core,
      version: "0.1.0",
      elixir: "~> 1.19",
      start_permanent: Mix.env() == :prod,
      deps: deps(),
      aliases: aliases(),
      dialyzer: [plt_add_deps: :apps_tree],
      docs: docs(),
      description: "Reusable business-semantics substrate for Mezzanine",
      name: "Mezzanine Core",
      source_url: @source_url,
      homepage_url: @source_url
    ]
  end

  def application do
    [
      extra_applications: [:crypto, :logger, :ecto_sql]
    ]
  end

  def cli do
    [preferred_envs: [ci: :test]]
  end

  defp aliases do
    [
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
      {:mezzanine_runtime_profile, path: "../runtime_profile"},
      workspace_dep({:ground_plane_persistence_policy, "~> 0.1.0", override: true}),
      {:ecto_sql, "~> 3.13"},
      {:jason, "~> 1.4"},
      {:postgrex, "~> 0.21"},
      {:telemetry, "~> 1.3"},
      {:credo, "~> 1.7", only: [:dev, :test], runtime: false},
      {:dialyxir, "~> 1.4", only: [:dev, :test], runtime: false},
      {:ex_doc, "~> 0.40.1", only: :dev, runtime: false}
    ]
  end

  defp docs do
    [
      main: "readme",
      extras: ["README.md"]
    ]
  end

  defp workspace_dep(committed) do
    if function_exported?(MixWorkspaceOpsBootstrap, :dep, 2),
      do: apply(MixWorkspaceOpsBootstrap, :dep, [committed, __DIR__]),
      else: committed
  end
end
