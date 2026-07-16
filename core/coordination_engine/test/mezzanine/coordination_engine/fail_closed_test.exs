defmodule Mezzanine.CoordinationEngine.FailClosedTest do
  use ExUnit.Case, async: true

  alias Mezzanine.CoordinationEngine

  test "fails closed when authority or tenant refs are dropped" do
    assert {:error, {:missing_required_ref, :authority_ref}} =
             base_run_attrs()
             |> Map.delete(:authority_ref)
             |> CoordinationEngine.admit()

    assert {:error, {:missing_required_ref, :tenant_ref}} =
             base_run_attrs()
             |> Map.delete(:tenant_ref)
             |> CoordinationEngine.admit()
  end

  test "fails closed when verifier, handoff, or turn refs are dropped" do
    {:ok, run} = CoordinationEngine.admit(base_run_attrs())

    assert {:error, {:missing_required_ref, :verifier_result_ref}} =
             CoordinationEngine.complete_verifier(run, %{
               score_schema_ref: "score-schema://binary",
               score_band: :pass,
               replay_ref: "replay://verifier/1",
               trace_ref: "trace://verifier/1"
             })

    assert {:error, {:missing_required_ref, :handoff_ref}} =
             CoordinationEngine.request_handoff(run, %{
               from_role_ref: "role://worker",
               to_role_ref: "role://reviewer",
               memory_summary_ref: "memory-summary://handoff",
               trace_ref: "trace://handoff/request",
               replay_ref: "replay://handoff/request"
             })

    assert {:error, {:missing_required_ref, :memory_ref_set}} =
             CoordinationEngine.start_turn(run, %{
               turn_ref: "turn://1",
               agent_ref: "agent://worker",
               role_ref: "role://worker",
               router_decision_ref: "router-decision://1",
               provider_pool_ref: "provider-pool://1",
               model_profile_ref: "model://mock",
               endpoint_profile_ref: "endpoint://mock",
               inference_call_ref: "inference-call://1",
               context_budget_ref: "context-budget://worker",
               cost_budget_ref: "cost-budget://worker",
               trace_ref: "trace://turn/1",
               replay_ref: "replay://turn/1",
               operator_action_ref: "operator-action://observe",
               handoff_policy_ref: "handoff-policy://bounded"
             })
  end

  defp base_run_attrs do
    %{
      coordination_run_ref: "coordination-run/fail-closed",
      tenant_ref: "tenant://adaptive",
      authority_ref: "authority://coordination",
      actor_ref: "actor://operator",
      subject_ref: "subject://ticket",
      persistence_profile_ref: "persistence://mezzanine/ops-durable",
      router_session_ref: "trinity-session://fail-closed",
      router_config_ref: "trinity-config://fail-closed",
      provider_pool_ref: "provider-pool://fail-closed",
      role_registry_ref: "role-registry://fail-closed",
      memory_ref_set: ["memory://shared"],
      prompt_ref_set: ["prompt://worker"],
      guardrail_ref_set: ["guardrail://input", "guardrail://output"],
      eval_ref_set: ["eval-suite://coordination-repair"],
      model_profile_ref_set: ["model://mock"],
      target_ref_set: ["target://mock"],
      trace_ref_set: ["trace://coordination"],
      replay_ref: "replay://coordination",
      cost_budget_ref: "cost-budget://coordination",
      context_budget_ref: "context-budget://coordination",
      operation_policy_ref: "operation-policy://route"
    }
  end
end
