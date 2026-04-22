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

  test "derives domain separated child keys from a canonical root" do
    canonical_key = canonical_root_key()

    expected_json =
      ~s({"canonical_idempotency_key":"#{canonical_key}","scope":"activity","stable_ref":"activity-call-1"})

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

  defp canonical_root_key do
    Idempotency.canonical_key!(%{
      tenant_id: "tenant-1",
      installation_id: "inst-1",
      operation_family: "workflow.start",
      operation_ref: "expense:run",
      causation_id: "cause:abc",
      authority_decision_ref: "authz:decision:42",
      subject_ref: %{kind: :work, id: "work-1"},
      payload_hash: "sha256:payload",
      source_event_position: "event:17"
    })
  end
end
