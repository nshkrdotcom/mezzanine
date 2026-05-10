unless Code.ensure_loaded?(DependencySources) do
  Code.require_file("build_support/dependency_sources.exs", __DIR__)
end

Code.require_file("build_support/workspace_contract.exs", __DIR__)
Code.require_file("build_support/internal_modularity_contract.exs", __DIR__)

defmodule Mezzanine.Workspace.MixProject do
  use Mix.Project

  alias Mezzanine.Build.WorkspaceContract

  @repo_root __DIR__
  @version "0.1.0"
  @source_url "https://github.com/nshkrdotcom/mezzanine"

  def project do
    [
      app: :mezzanine_workspace,
      version: @version,
      elixir: "~> 1.19",
      start_permanent: Mix.env() == :prod,
      consolidate_protocols: false,
      deps: deps(),
      aliases: aliases(),
      blitz_workspace: blitz_workspace(),
      dialyzer: dialyzer(),
      docs: docs(),
      source_url: @source_url,
      homepage_url: @source_url,
      name: "Mezzanine Workspace",
      description: "Tooling root for the Mezzanine reusable business-semantics monorepo"
    ]
  end

  def application do
    [
      extra_applications: [:logger]
    ]
  end

  def cli do
    [
      preferred_envs: [
        "boundary.check": :test,
        ci: :test,
        "monorepo.test": :test,
        "monorepo.credo": :test,
        "monorepo.dialyzer": :dev,
        "monorepo.docs": :dev
      ]
    ]
  end

  defp deps do
    [
      {:blitz, "~> 0.3.0", runtime: false},
      {:weld, "~> 0.8.1", only: [:dev, :test], runtime: false},
      DependencySources.dep(:temporalex, @repo_root),
      {:opentelemetry, "~> 1.5"},
      {:opentelemetry_api, "~> 1.4"},
      {:opentelemetry_exporter, "~> 1.8"},
      {:opentelemetry_semantic_conventions, "~> 1.27"},
      {:credo, "~> 1.7", only: [:dev, :test], runtime: false},
      {:dialyxir, "~> 1.4", only: [:dev, :test], runtime: false},
      {:ex_doc, "~> 0.40.1", only: :dev, runtime: false}
    ]
  end

  defp aliases do
    monorepo_aliases = [
      "monorepo.deps.get": ["blitz.workspace.impact deps_get --"],
      "monorepo.format": ["blitz.workspace.impact format --"],
      "monorepo.compile": ["blitz.workspace.impact compile --"],
      "monorepo.test": ["blitz.workspace.impact test --"],
      "monorepo.credo": ["blitz.workspace.impact credo --"],
      "monorepo.dialyzer": ["blitz.workspace.impact dialyzer --"],
      "monorepo.docs": ["blitz.workspace.impact docs --"],
      "mr.deps.get": ["monorepo.deps.get"],
      "mr.format": ["monorepo.format"],
      "mr.compile": ["monorepo.compile"],
      "mr.test": ["monorepo.test"],
      "mr.credo": ["monorepo.credo"],
      "mr.dialyzer": ["monorepo.dialyzer"],
      "mr.docs": ["monorepo.docs"]
    ]

    [
      "boundary.check": ["cmd elixir build_support/internal_modularity_check.exs"],
      "artifact.fidelity.check": [
        "cmd elixir build_support/projected_artifact_fidelity_check.exs"
      ],
      ci: [
        "deps.get",
        "monorepo.deps.get",
        "monorepo.format --check-formatted",
        "monorepo.compile",
        "boundary.check",
        "monorepo.test",
        "monorepo.credo --strict",
        "monorepo.dialyzer",
        "monorepo.docs",
        "weld.verify",
        "artifact.fidelity.check"
      ],
      "docs.root": ["docs"]
    ] ++ monorepo_aliases
  end

  defp dialyzer do
    [
      plt_add_deps: :apps_direct,
      plt_add_apps: [:mix, :blitz, :weld]
    ]
  end

  defp blitz_workspace do
    [
      root: __DIR__,
      projects: WorkspaceContract.active_project_globs(),
      isolation: [
        deps_path: true,
        build_path: true,
        lockfile: true,
        hex_home: "_build/hex"
      ],
      parallelism: [
        max_concurrency: nil,
        multiplier: :auto,
        base: [
          deps_get: 4,
          format: 4,
          compile: 4,
          test: 4,
          credo: 2,
          dialyzer: 1,
          docs: 4
        ],
        overrides: []
      ],
      tasks: [
        deps_get: [args: ["deps.get"], preflight?: false],
        format: [args: ["format"]],
        compile: [args: ["compile", "--warnings-as-errors"]],
        test: [args: ["test"], mix_env: "test", color: true],
        credo: [args: ["credo"]],
        dialyzer: [args: ["dialyzer"], mix_env: "dev"],
        docs: [args: ["docs"]]
      ]
    ]
  end

  defp docs do
    [
      main: "workspace_readme",
      name: "Mezzanine Workspace",
      logo: "assets/mezzanine.svg",
      assets: %{"assets" => "assets"},
      source_ref: "main",
      source_url: @source_url,
      homepage_url: @source_url,
      extras: [
        {"README.md", filename: "workspace_readme"},
        "docs/overview.md",
        "docs/layout.md",
        "docs/public_api.md",
        "docs/guides/index.md",
        "docs/guides/runtime_stack_overview.md",
        "docs/guides/work_control_run_lifecycle.md",
        "docs/guides/citadel_authority_compilation.md",
        "docs/guides/governed_lower_dispatch.md",
        "docs/guides/workflow_runtime_and_execution_lifecycle.md",
        "docs/guides/receipts_and_projections.md",
        "docs/guides/appkit_and_product_boundary.md",
        "docs/guides/local_acceptance_with_stacklab.md",
        "docs/publication.md",
        "docs/roadmap.md",
        "CHANGELOG.md",
        "LICENSE"
      ],
      groups_for_extras: [
        Overview: ["README.md", "docs/overview.md"],
        Architecture: ["docs/layout.md", "docs/roadmap.md", "docs/public_api.md"],
        Guides: [
          "docs/guides/index.md",
          "docs/guides/runtime_stack_overview.md",
          "docs/guides/work_control_run_lifecycle.md",
          "docs/guides/citadel_authority_compilation.md",
          "docs/guides/governed_lower_dispatch.md",
          "docs/guides/workflow_runtime_and_execution_lifecycle.md",
          "docs/guides/receipts_and_projections.md",
          "docs/guides/appkit_and_product_boundary.md",
          "docs/guides/local_acceptance_with_stacklab.md"
        ],
        Publication: ["docs/publication.md"],
        Project: ["CHANGELOG.md", "LICENSE"]
      ]
    ]
  end
end
