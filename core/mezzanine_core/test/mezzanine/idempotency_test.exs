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
end
