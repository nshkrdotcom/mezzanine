defmodule Mezzanine.ContextPacketEngineTest do
  use ExUnit.Case, async: true

  alias Mezzanine.ContextPacketEngine
  alias Mezzanine.ContextPacketEngine.{AdmissionReceipt, DefaultAdmitter}
  alias OuterBrain.ContextABI
  alias OuterBrain.ContextABI.Failure

  test "admits a compiled packet with Citadel authority and packet joins" do
    assert {:ok, packet, _compile_receipt} = ContextABI.compile(compile_request())

    assert {:ok, receipt} =
             ContextPacketEngine.admit(packet, admission_request(packet),
               authority_grant: authority_grant(packet),
               budget_decision: :allow
             )

    assert %AdmissionReceipt{} = receipt
    assert receipt.status == :admitted
    assert receipt.context_packet_ref == packet.context_packet_ref
    assert receipt.packet_hash == packet.packet_hash
    assert receipt.joins.workflow_ref == "workflow://tenant-a/run-a"
    assert receipt.joins.budget_ref == packet.budget_ref
    assert receipt.joins.cost_ref == "cost://tenant-a/run-a"
    assert receipt.joins.eval_ref == "eval://tenant-a/default"
    refute Map.has_key?(ContextPacketEngine.redacted_projection(receipt), :raw_prompt)
  end

  test "fails closed when budget admission denies execution" do
    assert {:ok, packet, _compile_receipt} = ContextABI.compile(compile_request())

    assert {:error, %Failure{} = failure} =
             ContextPacketEngine.admit(packet, admission_request(packet),
               authority_grant: authority_grant(packet),
               budget_decision: :deny_hard_exhausted
             )

    assert failure.reason_code == "mezzanine.packet_admission.budget_exhausted.v1"
  end

  test "fails closed without Citadel authority" do
    assert {:ok, packet, _compile_receipt} = ContextABI.compile(compile_request())

    assert {:error, %Failure{} = failure} =
             ContextPacketEngine.admit(packet, admission_request(packet), budget_decision: :allow)

    assert failure.reason_code == "mezzanine.packet_admission.authority_required.v1"
    assert failure.retryable?
  end

  test "returns duplicate receipt for replayed idempotency keys" do
    assert {:ok, packet, _compile_receipt} = ContextABI.compile(compile_request())
    request = admission_request(packet)

    assert {:ok, first_receipt} =
             DefaultAdmitter.admit(packet, request,
               authority_grant: authority_grant(packet),
               budget_decision: :allow
             )

    assert {:ok, duplicate} =
             DefaultAdmitter.admit(packet, request,
               admitted_receipts: %{request.idempotency_key => first_receipt},
               authority_grant: authority_grant(packet),
               budget_decision: :allow
             )

    assert duplicate.status == :duplicate
    assert duplicate.receipt_ref == first_receipt.receipt_ref
  end

  test "rejects stale packet hashes" do
    assert {:ok, packet, _compile_receipt} = ContextABI.compile(compile_request())
    stale_packet = %{packet | packet_hash: "sha256:" <> String.duplicate("0", 64)}

    assert {:error, %Failure{} = failure} =
             ContextPacketEngine.admit(stale_packet, admission_request(packet),
               authority_grant: authority_grant(packet),
               budget_decision: :allow
             )

    assert failure.reason_code == "mezzanine.packet_admission.stale_packet_hash.v1"
  end

  test "rejects raw payload projection fields in admission request metadata" do
    assert {:ok, packet, _compile_receipt} = ContextABI.compile(compile_request())
    request = Map.put(admission_request(packet), :metadata, %{raw_prompt: "do not admit"})

    assert {:error, %Failure{} = failure} =
             ContextPacketEngine.admit(packet, request,
               authority_grant: authority_grant(packet),
               budget_decision: :allow
             )

    assert failure.reason_code == "mezzanine.packet_admission.raw_payload_rejected.v1"
  end

  defp compile_request do
    %{
      tenant_ref: "tenant://tenant-a",
      user_request_ref: "artifact://tenant-a/request/a",
      system_instruction_ref: "artifact://tenant-a/system/a",
      memory_refs: ["memory://tenant-a/promoted/a"],
      budget_ref: "budget://tenant-a/run-a",
      model_class_allowlist: ["class://coding-small"],
      route_policy_ref: "route-policy://tenant-a/default",
      trace_ref: "trace://tenant-a/run-a"
    }
  end

  defp admission_request(packet) do
    %{
      tenant_ref: packet.tenant_ref,
      workflow_ref: "workflow://tenant-a/run-a",
      authority_ref: "authority://tenant-a/context/a",
      context_packet_ref: packet.context_packet_ref,
      idempotency_key: "idem://tenant-a/context/a",
      trace_ref: packet.trace_ref,
      budget_ref: packet.budget_ref,
      cost_ref: "cost://tenant-a/run-a",
      eval_ref: "eval://tenant-a/default",
      route_decision_ref: "route-decision://tenant-a/pending",
      model_call_ref: "model-call://tenant-a/pending"
    }
  end

  defp authority_grant(packet) do
    %{
      authority_ref: "authority://tenant-a/context/a",
      tenant_ref: packet.tenant_ref,
      allowed_model_classes: packet.model_class_allowlist,
      route_policy_ref: packet.route_policy_ref,
      expires_at: nil,
      trace_ref: packet.trace_ref,
      payload_mode: :refs_only,
      redaction_class: :tenant_sensitive,
      operation: :context_access
    }
  end
end
