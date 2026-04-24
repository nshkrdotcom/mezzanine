defmodule Mezzanine.Execution.PayloadBoundaryTest do
  use ExUnit.Case, async: true

  alias Mezzanine.Execution.PayloadBoundary

  @sha256 "sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
  @schema_hash "sha256:bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"

  test "compact execution column maps stay inline" do
    assert {:ok, :small_inline} =
             PayloadBoundary.classify_execution_column(:dispatch_envelope, %{
               "recipe_ref" => "triage_ticket",
               "runtime_class" => "session",
               "placement_ref" => "local_runner"
             })
  end

  test "oversized inline execution maps require artifact references before persistence" do
    payload = %{"capability" => "sandbox.exec", "padding" => String.duplicate("x", 70_000)}

    assert {:error, {:execution_payload_boundary, :dispatch_envelope, details}} =
             PayloadBoundary.classify_execution_column(:dispatch_envelope, payload)

    assert details.classification == :ref_required
    assert details.safe_action == :reject_before_durable_write
  end

  test "raw provider bodies are rejected even when small" do
    payload = %{"raw_provider_body" => %{"text" => "provider-native body"}}

    assert {:error, {:execution_payload_boundary, :last_dispatch_error_payload, details}} =
             PayloadBoundary.classify_execution_column(:last_dispatch_error_payload, payload)

    assert details.reason == :raw_payload_forbidden
  end

  test "valid artifact references carry primary content and schema hashes" do
    artifact_ref = valid_artifact_ref(%{"byte_size" => 120_000})

    assert {:ok, :ref_required} =
             PayloadBoundary.classify_execution_column(:lower_receipt, %{
               "state" => "completed",
               "normalized_outcome_ref" => artifact_ref
             })
  end

  test "artifact references reject bad primary hash algorithms and lifecycle fields" do
    bad_hash = valid_artifact_ref(%{"content_hash" => "sha1:abc"})

    assert {:error, {:execution_payload_boundary, :lower_receipt, hash_details}} =
             PayloadBoundary.classify_execution_column(:lower_receipt, %{
               "artifact_ref" => bad_hash
             })

    assert hash_details.reason == :invalid_primary_hash

    lifecycle_field = valid_artifact_ref(%{"storage_tier" => "cold"})

    assert {:error, {:execution_payload_boundary, :lower_receipt, lifecycle_details}} =
             PayloadBoundary.classify_execution_column(:lower_receipt, %{
               "artifact_ref" => lifecycle_field
             })

    assert lifecycle_details.reason == :phase5_lifecycle_field_forbidden
  end

  test "tenant-sensitive artifact references fail closed without posture evidence" do
    artifact_ref =
      valid_artifact_ref(%{
        "sensitivity_class" => "tenant_sensitive",
        "store_security_posture_ref" => nil
      })

    assert {:error, {:execution_payload_boundary, :lower_receipt, details}} =
             PayloadBoundary.classify_execution_column(:lower_receipt, %{
               "artifact_ref" => artifact_ref
             })

    assert details.reason == :missing_sensitive_posture
    assert details.safe_action == :unavailable_fail_closed
  end

  defp valid_artifact_ref(overrides) do
    Map.merge(
      %{
        "artifact_id" => "artifact:execution:1",
        "content_hash" => @sha256,
        "content_hash_alg" => "sha256",
        "byte_size" => 70_000,
        "schema_name" => "Mezzanine.Execution.NormalizedOutcomeRef.v1",
        "schema_hash" => @schema_hash,
        "schema_hash_alg" => "sha256",
        "media_type" => "application/json",
        "producer_repo" => "jido_integration",
        "tenant_scope" => "tenant-1",
        "sensitivity_class" => "tenant_sensitive",
        "store_security_posture_ref" => "jido_integration.claim_check_hot.security.v1",
        "encryption_posture_ref" => "unavailable_fail_closed",
        "retrieval_owner" => "jido_integration",
        "existing_fetch_or_restore_path" => "existing_claim_check_fetch",
        "safe_actions" => ["quarantine", "operator_review"],
        "queue_key" => "tenant-1:inst-1:triage_ticket",
        "oversize_action" => "ref_required",
        "release_manifest_ref" => "phase5-v7-artifact-boundary"
      },
      overrides
    )
  end
end
