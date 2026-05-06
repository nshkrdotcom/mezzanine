defmodule Mezzanine.CoordinationEngine.StateMachineTest do
  use ExUnit.Case, async: true

  alias Mezzanine.CoordinationEngine

  test "tracks governed TRINITY coordination lifecycle with required refs" do
    assert {:ok, run} = CoordinationEngine.admit(run_attrs())
    assert run.state == :created
    assert run.ai_run_envelope.run_class == :coordination_run

    assert {:ok, run} =
             CoordinationEngine.router_ready(run, %{
               router_artifact_ref: "router://phase10",
               extractor_ref: "extractor://phase10",
               head_ref: "head://phase10",
               trace_ref: "trace://router-ready"
             })

    assert run.state == :router_ready

    assert {:ok, run} = CoordinationEngine.provider_pool_ready(run, provider_slots())
    assert run.state == :provider_pool_ready
    assert run.provider_pool_ref == "provider-pool://coordination-run/phase10"

    assert {:ok, run, decision} =
             CoordinationEngine.route(run, trinity_config(), %{
               coordination_run_ref: "coordination-run/phase10",
               preferred_role_ref: "role://worker",
               trace_ref: "trace://router-decision",
               replay_ref: "replay://router-decision"
             })

    assert run.state == :routing
    assert decision.selected_role_ref == "role://worker"

    assert {:ok, run} =
             CoordinationEngine.inject_role(run, %{
               role_ref: "role://worker",
               prompt_ref: "prompt://worker",
               memory_ref_set: ["memory://shared"],
               context_budget_ref: "context-budget://worker",
               handoff_policy_ref: "handoff-policy://bounded",
               appkit_projection_ref: "appkit://coordination/worker"
             })

    assert run.state == :role_injected

    assert {:ok, run} = CoordinationEngine.start_turn(run, turn_refs())
    assert run.state == :agent_turn_started

    assert {:ok, run} =
             CoordinationEngine.complete_turn(run, %{
               turn_ref: "turn://1",
               inference_call_ref: "inference-call://1",
               memory_ref_set: ["memory://shared"],
               context_budget_ref: "context-budget://worker",
               cost_budget_ref: "cost-budget://worker",
               trace_ref: "trace://turn/1",
               replay_ref: "replay://turn/1"
             })

    assert run.state == :agent_turn_completed

    assert {:ok, run, _policy} = CoordinationEngine.run_verifier(run, verifier_policy_attrs())
    assert run.state == :verifier_running

    assert {:ok, run, verifier_decision} =
             CoordinationEngine.complete_verifier(run, %{
               verifier_result_ref: "verifier-result://1",
               score_schema_ref: "score-schema://binary",
               score_band: :repair,
               replay_ref: "replay://verifier/1",
               trace_ref: "trace://verifier/1"
             })

    assert run.state == :agent_turn_completed
    assert verifier_decision.termination_decision == :repair

    assert {:ok, run} =
             CoordinationEngine.request_handoff(run, %{
               handoff_ref: "handoff://worker-to-reviewer",
               from_role_ref: "role://worker",
               to_role_ref: "role://reviewer",
               memory_summary_ref: "memory-summary://handoff",
               trace_ref: "trace://handoff/request",
               replay_ref: "replay://handoff/request"
             })

    assert run.state == :handoff_requested

    assert {:ok, run} =
             CoordinationEngine.accept_handoff(run, %{
               handoff_ref: "handoff://worker-to-reviewer",
               accepting_agent_ref: "agent://reviewer",
               authority_ref: "authority://coordination",
               trace_ref: "trace://handoff/accept"
             })

    assert run.state == :handoff_accepted

    assert {:ok, run} =
             CoordinationEngine.terminate(run, %{
               termination_ref: "termination://complete",
               verifier_result_ref: "verifier-result://1",
               replay_ref: "replay://termination",
               trace_ref: "trace://termination"
             })

    assert run.state == :terminated
    assert :provider_pool_ready in run.state_history
  end

  defp run_attrs do
    %{
      coordination_run_ref: "coordination-run/phase10",
      tenant_ref: "tenant://adaptive",
      authority_ref: "authority://coordination",
      actor_ref: "actor://operator",
      subject_ref: "subject://ticket",
      persistence_profile_ref: "persistence://memory",
      router_session_ref: "trinity-session://phase10",
      router_config_ref: "trinity-config://phase10",
      provider_pool_ref: "provider-pool://coordination-run/phase10",
      role_registry_ref: "role-registry://phase10",
      memory_ref_set: ["memory://shared"],
      prompt_ref_set: ["prompt://worker"],
      model_profile_ref_set: ["model://mock"],
      target_ref_set: ["target://mock"],
      trace_ref_set: ["trace://coordination"],
      replay_ref: "replay://coordination",
      cost_budget_ref: "cost-budget://coordination",
      context_budget_ref: "context-budget://coordination",
      operation_policy_ref: "operation-policy://route",
      cancellation_ref: "cancel://coordination",
      retry_ref: "retry://coordination"
    }
  end

  defp trinity_config do
    Trinity.Config.compile!(%{
      router_artifact: %{
        router_artifact_ref: "router://phase10",
        extractor_ref: "extractor://phase10",
        head_ref: "head://phase10",
        compatibility_ref: "compat://phase10",
        hash_ref: "sha256:phase10"
      },
      role_packs: [
        %{
          role_ref: "role://worker",
          prompt_ref: "prompt://worker",
          capability_refs: ["capability://compose"],
          allowed_model_profile_refs: ["model://mock"],
          tool_policy_ref: "tool-policy://worker",
          memory_profile_ref: "memory-profile://worker",
          guardrail_profile_ref: "guardrail://worker",
          verifier_profile_ref: "verifier://worker",
          budget_ref: "budget://worker",
          context_budget_ref: "context-budget://worker",
          handoff_policy_ref: "handoff-policy://bounded",
          appkit_projection_ref: "appkit://coordination/worker",
          gepa_target_refs: ["gepa-target://role/worker"]
        }
      ],
      provider_pool: []
    })
  end

  defp provider_slots do
    [
      %{
        slot_ref: "slot://mock",
        slot_kind: :mock,
        role_refs: ["role://worker"],
        model_profile_ref: "model://mock",
        endpoint_profile_ref: "endpoint://mock",
        operation_policy_ref: "operation-policy://route",
        target_ref: "target://mock",
        credential_ref: "credential://mock"
      }
    ]
  end

  defp turn_refs do
    %{
      turn_ref: "turn://1",
      agent_ref: "agent://worker",
      role_ref: "role://worker",
      router_decision_ref: "router_decision:coordination-run/phase10:role://worker",
      provider_pool_ref: "provider-pool://coordination-run/phase10",
      model_profile_ref: "model://mock",
      endpoint_profile_ref: "endpoint://mock",
      inference_call_ref: "inference-call://1",
      memory_ref_set: ["memory://shared"],
      context_budget_ref: "context-budget://worker",
      cost_budget_ref: "cost-budget://worker",
      trace_ref: "trace://turn/1",
      replay_ref: "replay://turn/1",
      operator_action_ref: "operator-action://observe",
      handoff_policy_ref: "handoff-policy://bounded"
    }
  end

  defp verifier_policy_attrs do
    %{
      verifier_policy_ref: "verifier-policy://worker",
      verifier_prompt_ref: "prompt://verifier",
      verifier_model_profile_ref: "model://verifier",
      operation_policy_ref: "operation-policy://verify",
      score_schema_ref: "score-schema://binary",
      termination_threshold_ref: "threshold://terminate",
      retry_policy_ref: "retry://verifier",
      repair_policy_ref: "repair://verifier",
      escalation_policy_ref: "escalation://human",
      replay_ref: "replay://verifier",
      trace_ref: "trace://verifier",
      gepa_target_refs: ["gepa-target://verifier/prompt"]
    }
  end
end
