Code.require_file("workspace_contract.exs", __DIR__)

defmodule Mezzanine.Build.WeldContract do
  @moduledoc false

  @artifact_docs [
    "README.md",
    "core/mezzanine_core/README.md",
    "core/pack_model/README.md",
    "core/pack_compiler/README.md",
    "core/config_registry/README.md",
    "core/object_engine/README.md",
    "core/execution_engine/README.md",
    "core/runtime_scheduler/README.md",
    "core/decision_engine/README.md",
    "core/evidence_engine/README.md",
    "core/projection_engine/README.md",
    "core/operator_engine/README.md",
    "core/audit_engine/README.md",
    "core/archival_engine/README.md",
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
      roots: [
        "core/mezzanine_core",
        "core/pack_model",
        "core/pack_compiler",
        "core/config_registry",
        "core/object_engine",
        "core/execution_engine",
        "core/runtime_scheduler",
        "core/decision_engine",
        "core/evidence_engine",
        "core/projection_engine",
        "core/operator_engine",
        "core/audit_engine",
        "core/archival_engine"
      ],
      package: [
        name: "mezzanine_core",
        otp_app: :mezzanine_core,
        version: "0.1.0",
        docs_main: "MezzanineCore",
        description: "Projected reusable neutral core packages from the Mezzanine workspace"
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
