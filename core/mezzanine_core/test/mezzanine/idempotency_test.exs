defmodule Mezzanine.IdempotencyTest do
  use ExUnit.Case, async: true

  alias Mezzanine.Idempotency

  test "derives canonical root key from deterministic JSON bytes" do
    attrs = [
      operation_ref: "expense:run",
      tenant_id: "tenant-1",
      payload_hash: "sha256:payload",
      subject_ref: %{kind: :work, id: "work-1"},
      source_event_position: "event:17",
      causation_id: "cause:abc",
      installation_id: "inst-1",
      operation_family: "workflow.start",
      authority_decision_ref: "authz:decision:42",
      payload: %{"ignored" => "raw payload is not part of identity"}
    ]

    expected_json =
      ~s({"authority_decision_ref":"authz:decision:42","causation_id":"cause:abc","installation_id":"inst-1","operation_family":"workflow.start","operation_ref":"expense:run","payload_hash":"sha256:payload","source_event_position":"event:17","subject_ref":{"id":"work-1","kind":"work"},"tenant_id":"tenant-1"})

    expected_digest =
      :sha256
      |> :crypto.hash(expected_json)
      |> Base.encode16(case: :lower)

    assert {:ok, "idem:v1:" <> digest} = Idempotency.canonical_key(attrs)
    assert digest == expected_digest
  end

  test "normalizes string keys and equivalent subject and authority aliases" do
    atom_key =
      Idempotency.canonical_key!(%{
        tenant_id: "tenant-1",
        installation_id: nil,
        operation_family: "operator.action",
        operation_ref: "retry",
        causation_id: "cause-1",
        authority_decision_hash: "sha256:authz",
        resource_id: "resource-1",
        payload_hash: "sha256:payload"
      })

    string_key =
      Idempotency.canonical_key!(%{
        "tenant_id" => "tenant-1",
        "operation_family" => "operator.action",
        "operation_ref" => "retry",
        "causation_id" => "cause-1",
        "authority_decision_hash" => "sha256:authz",
        "resource_id" => "resource-1",
        "payload_hash" => "sha256:payload"
      })

    assert atom_key == string_key
  end

  test "reports missing required canonical root fields" do
    assert {:error,
            {:missing_canonical_idempotency_fields,
             [
               :operation_ref,
               :causation_id,
               :authority_decision_ref_or_hash,
               :subject_ref_or_resource_ref,
               :payload_hash
             ]}} =
             Idempotency.canonical_key(%{
               tenant_id: "tenant-1",
               operation_family: "workflow.start"
             })
  end

  test "exposes canonical payload for evidence records" do
    assert {:ok, payload} =
             Idempotency.canonical_payload(%{
               tenant_id: "tenant-1",
               operation_family: "audit.append",
               operation_ref: "fact-1",
               causation_id: "cause-1",
               authority_decision_ref: "decision-1",
               subject_id: "subject-1",
               payload_hash: "sha256:payload",
               source_event_position: "ledger:10"
             })

    assert payload == %{
             "authority_decision_ref" => "decision-1",
             "causation_id" => "cause-1",
             "installation_id" => nil,
             "operation_family" => "audit.append",
             "operation_ref" => "fact-1",
             "payload_hash" => "sha256:payload",
             "source_event_position" => "ledger:10",
             "subject_ref" => "subject-1",
             "tenant_id" => "tenant-1"
           }
  end

  test "excludes volatile runtime evidence from the canonical root key" do
    base = canonical_root_attrs()
    root_key = Idempotency.canonical_key!(base)

    volatile_first =
      Map.merge(base, %{
        payload: %{"ignored" => "raw payload is not identity"},
        raw_payload_bytes: "payload-bytes-1",
        wall_clock_timestamp: "2026-04-21T23:40:00Z",
        temporal_workflow_run_id: "run-001",
        temporal_activity_attempt_number: 1,
        provider_response: %{"request_id" => "provider-1", "body" => "raw"},
        random_retry_counter: 1
      })

    volatile_replay =
      Map.merge(base, %{
        payload: %{"ignored" => "different raw payload"},
        raw_payload_bytes: "payload-bytes-2",
        wall_clock_timestamp: "2026-04-21T23:41:00Z",
        temporal_workflow_run_id: "run-002",
        temporal_activity_attempt_number: 3,
        provider_response: %{"request_id" => "provider-2", "body" => "changed"},
        random_retry_counter: 7
      })

    assert Idempotency.canonical_key!(volatile_first) == root_key
    assert Idempotency.canonical_key!(volatile_replay) == root_key

    assert {:ok, payload} = Idempotency.canonical_payload(volatile_replay)
    refute Map.has_key?(payload, "payload")
    refute Map.has_key?(payload, "raw_payload_bytes")
    refute Map.has_key?(payload, "wall_clock_timestamp")
    refute Map.has_key?(payload, "temporal_workflow_run_id")
    refute Map.has_key?(payload, "temporal_activity_attempt_number")
    refute Map.has_key?(payload, "provider_response")
    refute Map.has_key?(payload, "random_retry_counter")
  end

  test "derives domain separated child keys from a canonical root" do
    canonical_key = canonical_root_key()

    expected_json =
      "{\"canonical_idempotency_key\":\"" <>
        canonical_key <> "\",\"scope\":\"activity\",\"stable_ref\":\"activity-call-1\"}"

    expected_digest =
      :sha256
      |> :crypto.hash(expected_json)
      |> Base.encode16(case: :lower)

    assert {:ok, "idem:v1:activity:" <> digest} =
             Idempotency.child_key(canonical_key, :activity, "activity-call-1")

    assert digest == expected_digest
  end

  test "normalizes child scope and structured stable refs" do
    canonical_key = canonical_root_key()

    atom_key =
      Idempotency.child_key!(
        canonical_key,
        :lower_submission,
        %{tenant_ref: "tenant-1", submission_key: "submission-1"}
      )

    string_key =
      Idempotency.child_key!(
        canonical_key,
        "lower_submission",
        %{"submission_key" => "submission-1", "tenant_ref" => "tenant-1"}
      )

    assert atom_key == string_key
    assert String.starts_with?(atom_key, "idem:v1:lower_submission:")
  end

  test "defines the required child idempotency scopes" do
    assert Idempotency.known_child_scopes() == [
             "activity",
             "lower_side_effect",
             "lower_submission",
             "provider_retry"
           ]

    canonical_key = canonical_root_key()

    for scope <- Idempotency.known_child_scopes() do
      assert {:ok, "idem:v1:" <> _key} = Idempotency.child_key(canonical_key, scope, "#{scope}:1")
    end
  end

  test "rejects child keys without a canonical root, safe scope, and stable ref" do
    assert {:error, {:invalid_canonical_idempotency_key, "idem:v1:activity:abc"}} =
             Idempotency.child_key("idem:v1:activity:abc", :activity, "call-1")

    assert {:error, {:invalid_child_idempotency_scope, "activity:attempt"}} =
             Idempotency.child_key(canonical_root_key(), "activity:attempt", "call-1")

    assert {:error, :missing_child_idempotency_stable_ref} =
             Idempotency.child_key(canonical_root_key(), :activity, nil)
  end

  test "builds idempotency correlation evidence across workflow and lower refs" do
    canonical_key = canonical_root_key()
    activity_key = Idempotency.child_key!(canonical_key, :activity, "activity-call-1")
    lower_key = Idempotency.child_key!(canonical_key, :lower_submission, "lower-submission-1")
    retry_key = Idempotency.child_key!(canonical_key, :provider_retry, "provider-retry-1")

    assert {:ok, evidence} =
             Idempotency.correlation_evidence(%{
               canonical_idempotency_key: canonical_key,
               tenant_id: "tenant-1",
               trace_id: "trace-1",
               causation_id: "cause-1",
               client_retry_key: "client-retry-1",
               platform_envelope_idempotency_key: canonical_key,
               lower_submission_ref: "lower-submission-1",
               temporal_workflow_id: "workflow-1",
               temporal_workflow_run_id: "run-1",
               temporal_start_idempotency_key: canonical_key,
               temporal_activity_call_ref: "activity-call-1",
               temporal_activity_attempt_number: 3,
               jido_lower_activity_idempotency_key: canonical_key,
               lower_provider_retry_stable_ref: "provider-retry-1",
               execution_plane_intent_id: "intent-1",
               execution_plane_route_id: "route-1",
               execution_plane_envelope_idempotency_key: canonical_key,
               execution_plane_route_idempotency_key: canonical_key,
               release_manifest_ref: "release-1"
             })

    assert evidence["contract_name"] == "Mezzanine.IdempotencyCorrelationEvidence.v1"
    assert evidence["derivation_algorithm"] == "idem:v1:sha256_jcs"
    assert evidence["canonical_idempotency_key"] == canonical_key
    assert evidence["client_retry_key"] == "client-retry-1"
    assert evidence["platform_envelope_idempotency_key"] == canonical_key
    assert evidence["mezzanine_submission_dedupe_key"] == lower_key
    assert evidence["temporal_workflow_id"] == "workflow-1"
    assert evidence["temporal_workflow_run_id"] == "run-1"
    assert evidence["temporal_start_idempotency_key"] == canonical_key
    assert evidence["temporal_activity_call_ref"] == "activity-call-1"
    assert evidence["temporal_activity_side_effect_key"] == activity_key
    assert evidence["temporal_activity_attempt_number"] == 3
    assert evidence["jido_lower_activity_idempotency_key"] == canonical_key
    assert evidence["jido_lower_submission_dedupe_key"] == lower_key
    assert evidence["lower_provider_retry_key"] == retry_key
    assert evidence["execution_plane_intent_id"] == "intent-1"
    assert evidence["execution_plane_route_id"] == "route-1"
    assert evidence["execution_plane_envelope_idempotency_key"] == canonical_key
    assert evidence["execution_plane_route_idempotency_key"] == canonical_key
    assert evidence["trace_id"] == "trace-1"
    assert evidence["causation_id"] == "cause-1"
    assert evidence["tenant_id"] == "tenant-1"
    assert evidence["release_manifest_ref"] == "release-1"
  end

  test "retry replay changes attempt evidence without changing root or side effect keys" do
    canonical_key = canonical_root_key()
    attrs = retry_replay_correlation_attrs(canonical_key)

    assert Idempotency.canonical_key!(
             Map.put(canonical_root_attrs(), :temporal_activity_attempt_number, 1)
           ) ==
             canonical_key

    assert Idempotency.canonical_key!(
             Map.put(canonical_root_attrs(), :temporal_activity_attempt_number, 4)
           ) ==
             canonical_key

    assert {:ok, first_attempt} =
             attrs
             |> Map.put(:temporal_activity_attempt_number, 1)
             |> Idempotency.correlation_evidence()

    assert {:ok, replay_attempt} =
             attrs
             |> Map.put(:temporal_activity_attempt_number, 4)
             |> Idempotency.correlation_evidence()

    assert first_attempt["canonical_idempotency_key"] == canonical_key
    assert replay_attempt["canonical_idempotency_key"] == canonical_key

    assert first_attempt["temporal_activity_side_effect_key"] ==
             replay_attempt["temporal_activity_side_effect_key"]

    assert first_attempt["jido_lower_submission_dedupe_key"] ==
             replay_attempt["jido_lower_submission_dedupe_key"]

    assert first_attempt["lower_provider_retry_key"] == replay_attempt["lower_provider_retry_key"]
    assert first_attempt["temporal_activity_attempt_number"] == 1
    assert replay_attempt["temporal_activity_attempt_number"] == 4

    assert Map.delete(first_attempt, "temporal_activity_attempt_number") ==
             Map.delete(replay_attempt, "temporal_activity_attempt_number")
  end

  test "rejects idempotency correlation fields that no longer join to the root" do
    canonical_key = canonical_root_key()

    assert {:error,
            {:idempotency_correlation_mismatch, :platform_envelope_idempotency_key,
             ^canonical_key, "idem:v1:bad"}} =
             Idempotency.correlation_evidence(%{
               canonical_idempotency_key: canonical_key,
               tenant_id: "tenant-1",
               trace_id: "trace-1",
               causation_id: "cause-1",
               platform_envelope_idempotency_key: "idem:v1:bad"
             })

    lower_key = Idempotency.child_key!(canonical_key, :lower_submission, "lower-submission-1")

    assert {:error,
            {:idempotency_correlation_mismatch, :mezzanine_submission_dedupe_key, ^lower_key,
             "different-lower-key"}} =
             Idempotency.correlation_evidence(%{
               canonical_idempotency_key: canonical_key,
               tenant_id: "tenant-1",
               trace_id: "trace-1",
               causation_id: "cause-1",
               lower_submission_ref: "lower-submission-1",
               mezzanine_submission_dedupe_key: lower_key,
               jido_lower_submission_dedupe_key: "different-lower-key"
             })
  end

  defp canonical_root_key do
    Idempotency.canonical_key!(canonical_root_attrs())
  end

  defp canonical_root_attrs do
    %{
      tenant_id: "tenant-1",
      installation_id: "inst-1",
      operation_family: "workflow.start",
      operation_ref: "expense:run",
      causation_id: "cause:abc",
      authority_decision_ref: "authz:decision:42",
      subject_ref: %{kind: :work, id: "work-1"},
      payload_hash: "sha256:payload",
      source_event_position: "event:17"
    }
  end

  defp retry_replay_correlation_attrs(canonical_key) do
    %{
      canonical_idempotency_key: canonical_key,
      tenant_id: "tenant-1",
      trace_id: "trace-retry",
      causation_id: "cause:abc",
      platform_envelope_idempotency_key: canonical_key,
      temporal_start_idempotency_key: canonical_key,
      temporal_workflow_id: "workflow-retry",
      temporal_workflow_run_id: "run-retry",
      temporal_activity_call_ref: "activity-call-retry",
      jido_lower_activity_idempotency_key: canonical_key,
      lower_submission_ref: "lower-submission-retry",
      lower_provider_retry_stable_ref: "provider-retry-stable",
      execution_plane_envelope_idempotency_key: canonical_key,
      execution_plane_route_idempotency_key: canonical_key,
      execution_plane_route_id: "route-retry",
      release_manifest_ref: "release-retry"
    }
  end
end
