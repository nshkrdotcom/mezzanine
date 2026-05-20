defmodule Mezzanine.Core.GovernedEffects.CoreStructsTest do
  use ExUnit.Case, async: true

  alias GroundPlane.Boundary.Codec
  alias Mezzanine.Core.GovernedEffects.AuthorityPacket
  alias Mezzanine.Core.GovernedEffects.EffectReceipt
  alias Mezzanine.Core.GovernedEffects.GovernedEffect

  test "GovernedEffect enforces required fields and bounded statuses" do
    assert {:ok, effect} = GovernedEffect.new(governed_effect_attrs())

    assert effect.status == :proposed
    assert effect.effect_ref == "effect://tenant-a/diagnostic/001"
    assert effect.command_ref == "command://tenant-a/diagnostic/001"
    assert effect.tenant_ref == "tenant-a"
    assert effect.actor_ref == "actor://user/operator-a"
    assert effect.trace_ref == "trace-tenant-a-diagnostic-001"

    assert GovernedEffect.statuses() == [
             :proposed,
             :authorized,
             :dispatched,
             :receipt_received,
             :reduced,
             :projected,
             :completed,
             :denied,
             :compensating
           ]
  end

  test "GovernedEffect rejects missing required fields and invalid statuses" do
    assert {:error, {:missing_field, :effect_ref}} =
             GovernedEffect.new(Map.delete(governed_effect_attrs(), :effect_ref))

    assert {:error, {:invalid_status, :unknown}} =
             GovernedEffect.new(%{governed_effect_attrs() | status: :unknown})

    invalid = struct!(GovernedEffect, %{governed_effect_attrs() | status: :unknown})

    assert {:error, {:invalid_status, :unknown}} = GovernedEffect.new(invalid)
  end

  test "GovernedEffect accepts bounded string statuses" do
    assert {:ok, effect} = GovernedEffect.new(%{governed_effect_attrs() | status: "authorized"})
    assert effect.status == :authorized
  end

  test "GovernedEffect round-trips through the boundary codec" do
    effect = GovernedEffect.new!(governed_effect_attrs())

    assert %{
             "effect_ref" => "effect://tenant-a/diagnostic/001",
             "effect_type" => "diagnostic",
             "status" => "proposed",
             "tenant_ref" => "tenant-a",
             "trace_ref" => "trace-tenant-a-diagnostic-001"
           } = effect |> GovernedEffect.encode!() |> Codec.decode!()
  end

  test "EffectReceipt enforces required fields and bounded statuses" do
    assert {:ok, receipt} = EffectReceipt.new(effect_receipt_attrs())

    assert receipt.status == :success
    assert receipt.receipt_ref == "receipt://tenant-a/diagnostic/001"
    assert receipt.effect_ref == "effect://tenant-a/diagnostic/001"

    assert EffectReceipt.statuses() == [
             :success,
             :failure,
             :partial,
             :timeout,
             :compensated,
             :denied,
             :cancelled
           ]
  end

  test "EffectReceipt rejects missing required fields and invalid statuses" do
    assert {:error, {:missing_field, :receipt_ref}} =
             EffectReceipt.new(Map.delete(effect_receipt_attrs(), :receipt_ref))

    assert {:error, {:invalid_status, :unknown}} =
             EffectReceipt.new(%{effect_receipt_attrs() | status: :unknown})

    invalid = struct!(EffectReceipt, %{effect_receipt_attrs() | status: :unknown})

    assert {:error, {:invalid_status, :unknown}} = EffectReceipt.new(invalid)
  end

  test "EffectReceipt accepts bounded string statuses" do
    assert {:ok, receipt} = EffectReceipt.new(%{effect_receipt_attrs() | status: "partial"})
    assert receipt.status == :partial
  end

  test "EffectReceipt round-trips through the boundary codec" do
    receipt = EffectReceipt.new!(effect_receipt_attrs())

    assert %{
             "receipt_ref" => "receipt://tenant-a/diagnostic/001",
             "effect_ref" => "effect://tenant-a/diagnostic/001",
             "status" => "success",
             "trace_ref" => "trace-tenant-a-diagnostic-001"
           } = receipt |> EffectReceipt.encode!() |> Codec.decode!()
  end

  test "AuthorityPacket enforces required fields and bounded decisions" do
    assert {:ok, packet} = AuthorityPacket.new(authority_packet_attrs())

    assert packet.decision == :review
    assert packet.authority_ref == "authority://tenant-a/diagnostic/review"
    assert packet.tenant_ref == "tenant-a"
    assert packet.actor_ref == "actor://user/operator-a"

    assert AuthorityPacket.decisions() == [:allow, :deny, :review, :downgrade, :revoke]
  end

  test "AuthorityPacket rejects missing required fields and invalid decisions" do
    assert {:error, {:missing_field, :authority_ref}} =
             AuthorityPacket.new(Map.delete(authority_packet_attrs(), :authority_ref))

    assert {:error, {:invalid_decision, :review_required}} =
             AuthorityPacket.new(%{authority_packet_attrs() | decision: :review_required})

    invalid = struct!(AuthorityPacket, %{authority_packet_attrs() | decision: :review_required})

    assert {:error, {:invalid_decision, :review_required}} = AuthorityPacket.new(invalid)
  end

  test "AuthorityPacket accepts bounded string decisions" do
    assert {:ok, packet} = AuthorityPacket.new(%{authority_packet_attrs() | decision: "allow"})
    assert packet.decision == :allow
  end

  test "AuthorityPacket round-trips through the boundary codec" do
    packet = AuthorityPacket.new!(authority_packet_attrs())

    assert %{
             "authority_ref" => "authority://tenant-a/diagnostic/review",
             "decision" => "review",
             "tenant_ref" => "tenant-a",
             "actor_ref" => "actor://user/operator-a",
             "trace_ref" => "trace-tenant-a-diagnostic-001"
           } = packet |> AuthorityPacket.encode!() |> Codec.decode!()
  end

  defp governed_effect_attrs do
    %{
      effect_ref: "effect://tenant-a/diagnostic/001",
      effect_type: "diagnostic",
      command_ref: "command://tenant-a/diagnostic/001",
      tenant_ref: "tenant-a",
      actor_ref: "actor://user/operator-a",
      installation_ref: "installation://tenant-a/synapse",
      authority_ref: "authority://tenant-a/diagnostic/review",
      status: :proposed,
      risk_class: "low",
      preconditions: [%{"kind" => "tenant_active"}],
      dispatch_ref: "dispatch://tenant-a/diagnostic/001",
      receipt_ref: "receipt://tenant-a/diagnostic/001",
      compensation_posture: "not_required",
      expected_version: 1,
      trace_ref: "trace-tenant-a-diagnostic-001",
      created_at: "2026-05-20T08:00:00Z",
      updated_at: "2026-05-20T08:00:01Z"
    }
  end

  defp effect_receipt_attrs do
    %{
      receipt_ref: "receipt://tenant-a/diagnostic/001",
      effect_ref: "effect://tenant-a/diagnostic/001",
      status: :success,
      lower_receipt_ref: "lower-receipt://tenant-a/diagnostic/001",
      lower_facts: %{"operation" => "diagnostic.echo"},
      projection_updates: [%{"projection_ref" => "projection://tenant-a/run/001"}],
      evidence_refs: ["evidence://tenant-a/diagnostic/001"],
      trace_ref: "trace-tenant-a-diagnostic-001",
      completed_at: "2026-05-20T08:00:02Z"
    }
  end

  defp authority_packet_attrs do
    %{
      authority_ref: "authority://tenant-a/diagnostic/review",
      decision: :review,
      tenant_ref: "tenant-a",
      actor_ref: "actor://user/operator-a",
      command_ref: "command://tenant-a/diagnostic/001",
      trace_ref: "trace-tenant-a-diagnostic-001",
      policy_refs: ["policy://tenant-a/diagnostic"],
      risk_class: "low",
      budget_refs: ["budget://tenant-a/default"],
      residency_refs: ["residency://tenant-a/hst"],
      reason: "operator review required",
      expiry: "2026-05-20T09:00:00Z"
    }
  end
end
