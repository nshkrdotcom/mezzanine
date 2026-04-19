defmodule Mezzanine.Authoring.SupplyChainContractsTest do
  use ExUnit.Case, async: true

  alias Mezzanine.Authoring.{ExtensionPackBundle, ExtensionPackSignature}

  @hash "sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"

  test "accepts scoped extension pack signature evidence" do
    assert {:ok, signature} =
             signature_attrs()
             |> ExtensionPackSignature.new()

    assert signature.contract_name == "Platform.ExtensionPackSignature.v1"
    assert signature.signature_algorithm == "hmac-sha256"
    assert signature.verification_hash == @hash
  end

  test "rejects extension pack signature evidence without signature scope" do
    assert {:error, {:missing_required_fields, fields}} =
             signature_attrs()
             |> Map.delete(:signature_ref)
             |> ExtensionPackSignature.new()

    assert :signature_ref in fields
  end

  test "rejects extension pack signature evidence with unsupported algorithms" do
    assert {:error, :invalid_extension_pack_signature} =
             signature_attrs()
             |> Map.put(:signature_algorithm, "rsa-legacy")
             |> ExtensionPackSignature.new()
  end

  test "accepts scoped extension pack bundle evidence" do
    assert {:ok, bundle} =
             bundle_attrs()
             |> ExtensionPackBundle.new()

    assert bundle.contract_name == "Platform.ExtensionPackBundle.v1"
    assert bundle.declared_resources == ["connector:github.issue", "schema:expense_request"]
    assert bundle.schema_hash == @hash
  end

  test "rejects extension pack bundle evidence without declared resources" do
    assert {:error, {:missing_required_fields, fields}} =
             bundle_attrs()
             |> Map.put(:declared_resources, [])
             |> ExtensionPackBundle.new()

    assert :declared_resources in fields
  end

  test "rejects extension pack bundle evidence with an invalid schema hash" do
    assert {:error, :invalid_extension_pack_bundle} =
             bundle_attrs()
             |> Map.put(:schema_hash, "sha256:not-a-hash")
             |> ExtensionPackBundle.new()
  end

  defp signature_attrs do
    base_attrs()
    |> Map.merge(%{
      signature_ref: "sig:phase4-expense-approval",
      signing_key_ref: "signing-key:tenant-a:2026-04",
      signature_algorithm: "hmac-sha256",
      verification_hash: @hash,
      rejection_ref: "rejection:none"
    })
  end

  defp bundle_attrs do
    base_attrs()
    |> Map.merge(%{
      bundle_schema_version: "phase4.extension_bundle.v1",
      declared_resources: ["connector:github.issue", "schema:expense_request"],
      schema_hash: @hash,
      validation_error_ref: "validation:none"
    })
  end

  defp base_attrs do
    %{
      tenant_ref: "tenant:alpha",
      installation_ref: "installation:alpha-prod",
      workspace_ref: "workspace:alpha",
      project_ref: "project:phase4",
      environment_ref: "env:prod",
      system_actor_ref: "system:pack-authoring",
      resource_ref: "pack:expense-approval",
      authority_packet_ref: "authority:pack-import",
      permission_decision_ref: "decision:allow-import",
      idempotency_key: "idem:pack-import:1",
      trace_id: "trace:pack-import:1",
      correlation_id: "corr:pack-import:1",
      release_manifest_ref: "phase4-v6-milestone14-extension-authoring-supply-chain",
      pack_ref: "pack:expense-approval@1.0.0"
    }
  end
end
