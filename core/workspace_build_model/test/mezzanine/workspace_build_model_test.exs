defmodule Mezzanine.WorkspaceBuildModelTest do
  use ExUnit.Case, async: true

  alias Mezzanine.WorkspaceBuildModel

  test "builds no-secret runtime manifests with provider and target refs" do
    assert {:ok, manifest} = WorkspaceBuildModel.build_manifest(valid_workspace())

    assert manifest.workspace_ref == "workspace://tenant-1/product/main"
    assert manifest.provider_account_refs == ["provider-account://tenant-1/claude/main"]
    assert manifest.connector_binding_refs == ["connector-binding://tenant-1/claude/default"]
    assert manifest.target_posture_refs == ["target-posture://tenant-1/local-process"]
    assert manifest.plugin_boundary_refs == ["plugin-boundary://tenant-1/local-runner"]
    assert manifest.raw_material_present? == false
  end

  test "rejects manifests that bundle unmanaged auth material" do
    assert {:error, {:forbidden_workspace_build_material, forbidden}} =
             valid_workspace()
             |> Map.put(:token_file, "/home/operator/.token")
             |> Map.put(:raw_secret, "secret")
             |> WorkspaceBuildModel.build_manifest()

    assert forbidden == [:raw_secret, :token_file]
  end

  test "requires runtime invocation refs for configured plugins" do
    assert {:error, {:missing_workspace_build_refs, missing}} =
             valid_workspace()
             |> Map.delete(:runtime_invocation_ref)
             |> Map.delete(:projection_ref)
             |> WorkspaceBuildModel.build_manifest()

    assert missing == [:runtime_invocation_ref, :projection_ref]
  end

  defp valid_workspace do
    %{
      workspace_ref: "workspace://tenant-1/product/main",
      agent_refs: ["agent://tenant-1/coder"],
      role_refs: ["role://tenant-1/coder"],
      trigger_refs: ["trigger://tenant-1/manual"],
      provider_account_refs: ["provider-account://tenant-1/claude/main"],
      connector_binding_refs: ["connector-binding://tenant-1/claude/default"],
      target_posture_refs: ["target-posture://tenant-1/local-process"],
      env_contract_refs: ["env-contract://tenant-1/headless"],
      secret_contract_refs: ["secret-contract://tenant-1/provider"],
      plugin_boundary_refs: ["plugin-boundary://tenant-1/local-runner"],
      runtime_invocation_ref: "runtime-invocation://tenant-1/headless/1",
      projection_ref: "projection://tenant-1/headless/state",
      manifest_ref: "workspace-build-manifest://tenant-1/product/main"
    }
  end
end
