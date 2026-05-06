defmodule Mezzanine.CoordinationEngine.TraceDatasetTest do
  use ExUnit.Case, async: true

  alias Mezzanine.CoordinationEngine

  test "converts coordination traces to eval and replay dataset refs without raw payloads" do
    assert {:ok, run} = CoordinationEngine.admit(run_attrs())

    assert {:ok, receipt} =
             CoordinationEngine.trace_dataset(run, %{
               eval_dataset_ref: "eval-dataset://trinity/repair",
               replay_dataset_ref: "replay-dataset://trinity/repair",
               drift_run_ref: "drift-run://router/worker",
               memory_evidence_refs: ["memory-evidence://turn-summary"],
               context_budget_refs: ["context-budget://coordination"],
               guardrail_evidence_refs: ["guardrail-evidence://output"],
               cost_ledger_refs: ["cost-ledger://coordination"],
               appkit_projection_refs: ["appkit://coordination/trace-dataset"],
               aitrace_span_refs: ["aitrace-span://coordination/1"],
               store_tier_refs: ["store-tier://memory-ephemeral"],
               persistence_profile_ref: "persistence-profile://memory-default",
               local_restart_safe_profile_ref: "restart-safe://local-selected-categories",
               retention_ref: "retention://ephemeral",
               debug_tap_posture_ref: "debug-tap://redacted-only"
             })

    assert receipt.fixture_refs == ["AOC-027", "AOC-032", "PERSIST-AOC-004", "PERSIST-AOC-005"]
    assert receipt.eval_dataset_ref == "eval-dataset://trinity/repair"
    assert receipt.replay_dataset_ref == "replay-dataset://trinity/repair"
    assert receipt.trace_refs == ["trace://coordination"]
    assert receipt.redaction_posture == :refs_only
    assert receipt.status == :pass
  end

  test "rejects raw prompt and provider payload fields" do
    assert {:ok, run} = CoordinationEngine.admit(run_attrs())

    assert {:error, {:forbidden_raw_field, :provider_payload}} =
             CoordinationEngine.trace_dataset(run, %{
               eval_dataset_ref: "eval-dataset://trinity/repair",
               replay_dataset_ref: "replay-dataset://trinity/repair",
               provider_payload: %{body: "not allowed"}
             })
  end

  defp run_attrs do
    %{
      coordination_run_ref: "coordination-run/phase12",
      tenant_ref: "tenant://adaptive",
      authority_ref: "authority://coordination",
      actor_ref: "actor://operator",
      subject_ref: "subject://ticket",
      persistence_profile_ref: "persistence://memory",
      router_session_ref: "trinity-session://phase12",
      router_config_ref: "trinity-config://phase12",
      provider_pool_ref: "provider-pool://coordination-run/phase12",
      role_registry_ref: "role-registry://phase12",
      memory_ref_set: ["memory://shared"],
      prompt_ref_set: ["prompt://worker", "prompt://verifier"],
      guardrail_ref_set: ["guardrail://input", "guardrail://output"],
      eval_ref_set: ["eval-suite://coordination-repair"],
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
end
