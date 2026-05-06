defmodule Mezzanine.CoordinationEngine.PrimitiveReuseTest do
  use ExUnit.Case, async: true

  alias Mezzanine.CoordinationEngine

  test "delegates bounded coordination pattern planning to Jido Hive primitives" do
    assert {:ok, plan} = CoordinationEngine.plan_pattern(pattern_attrs())
    assert plan.pattern_name == :orchestrator_worker
    assert plan.provider_effect_status == :suppressed_for_replay
  end

  test "delegates bounded inter-agent message routing to Jido Hive primitives" do
    assert {:ok, routed} =
             CoordinationEngine.route_message(message_attrs(), %{
               declared_recipient_refs: ["agent://reviewer"],
               fanout_count: 1
             })

    projection = JidoHive.InterAgentMessaging.projection(routed)
    assert projection.delivery_status == :accepted_no_provider_effect
    refute Map.has_key?(projection, :message_body)
  end

  defp pattern_attrs do
    %{
      pattern_ref: "coordination-pattern://orchestrator-worker",
      pattern_name: :orchestrator_worker,
      tenant_ref: "tenant-a",
      installation_ref: "installation://main",
      authority_ref: "authority://coordination",
      budget_profile_ref: "budget-profile://coordination",
      trace_ref: "trace://pattern",
      max_agents: 4,
      max_turns: 8,
      max_messages: 16,
      max_tokens: 4_000,
      cancellation_policy_ref: "cancel-policy://bounded",
      memory_policy_ref: "memory-policy://shared-grants",
      replay_policy: :suppress_provider_effects,
      connector_policy_ref: "connector-policy://approved",
      approved_connector_refs: ["connector://search"],
      redaction_posture: "refs_only"
    }
  end

  defp message_attrs do
    %{
      message_ref: "message://1",
      sender_agent_ref: "agent://writer",
      recipient_agent_ref: "agent://reviewer",
      tenant_ref: "tenant-a",
      installation_ref: "installation://main",
      authority_ref: "authority://coordination",
      memory_scope_ref: "memory-scope://tenant-a/run-1/shared",
      context_budget_ref: "context-budget://run-1",
      budget_decision_ref: "budget-decision://allow",
      idempotency_key: "idem-message-1",
      trace_ref: "trace://message-1",
      message_body_ref: "message-body-ref://hash-1",
      redaction_posture: "hash_only",
      token_budget: 50,
      byte_budget: 2048,
      turn_budget: 4,
      wall_clock_budget_ms: 1_000,
      max_fanout: 1
    }
  end
end
