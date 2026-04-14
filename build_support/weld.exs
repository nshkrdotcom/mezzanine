Code.require_file("workspace_contract.exs", __DIR__)

defmodule Mezzanine.Build.WeldContract do
  @moduledoc false

  @artifact_docs [
    "README.md",
    "docs/overview.md",
    "docs/layout.md",
    "docs/publication.md",
    "docs/roadmap.md"
  ]

  def manifest do
    [
      workspace: [
        root: "..",
        project_globs: Mezzanine.Build.WorkspaceContract.active_project_globs()
      ],
      classify: [
        tooling: ["."]
      ],
      publication: [
        internal_only: ["."]
      ],
      artifacts: [
        mezzanine_core: artifact()
      ]
    ]
  end

  def artifact do
    [
      roots: ["core/mezzanine_core"],
      package: [
        name: "mezzanine_core",
        otp_app: :mezzanine_core,
        version: "0.1.0",
        description:
          "Projected reusable business-semantics core package from the Mezzanine workspace"
      ],
      output: [
        docs: @artifact_docs,
        assets: ["CHANGELOG.md", "LICENSE"]
      ],
      verify: [
        artifact_tests: ["packaging/weld/mezzanine_core/test"],
        hex_build: false,
        hex_publish: false
      ]
    ]
  end
end

Mezzanine.Build.WeldContract.manifest()
