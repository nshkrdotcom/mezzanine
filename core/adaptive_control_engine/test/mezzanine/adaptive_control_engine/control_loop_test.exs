defmodule Mezzanine.AdaptiveControlEngine.ControlLoopTest do
  use ExUnit.Case, async: true

  alias Mezzanine.AdaptiveControlEngine

  test "creates a closed-loop promotion receipt over governed refs only" do
    assert {:ok, receipt} = AdaptiveControlEngine.evaluate(control_attrs())

    assert receipt.fixture_refs == [
             "AOC-028",
             "AOC-029",
             "AOC-030",
             "PERSIST-AOC-008"
           ]

    assert receipt.status == :ready_for_promotion
    assert receipt.trace_dataset_ref == "trace-dataset://trinity/repair"
    assert receipt.optimization_target_refs == ["gepa-target://role-prompt"]
    assert receipt.role_prompt_refs == ["prompt://role/worker/v2"]
    assert receipt.verifier_prompt_refs == ["prompt://verifier/worker/v2"]
    assert receipt.context_budget_refs == ["context-budget://worker/v2"]
    assert receipt.memory_policy_refs == ["memory-policy://worker/retrieval/v2"]
    assert receipt.tool_policy_refs == ["tool-policy://worker/v2"]
    assert receipt.fallback_policy_refs == ["fallback-policy://router/v2"]
    assert receipt.termination_threshold_refs == ["termination-threshold://worker/v2"]
    assert receipt.shadow_ref == "shadow://candidate/worker/v2"
    assert receipt.canary_ref == "canary://candidate/worker/v2"
    assert receipt.approval_ref == "approval://operator/worker/v2"
    assert receipt.promotion_ref == "promotion://candidate/worker/v2"
    assert receipt.rollback_ref == "rollback://candidate/worker/v1"
    assert receipt.redaction_posture == :refs_only
  end

  test "fails closed when adaptive gates are missing" do
    assert {:error, receipt} =
             control_attrs()
             |> Map.delete(:operator_approval_refs)
             |> AdaptiveControlEngine.evaluate()

    assert receipt.status == :blocked
    assert "gate:approval" in receipt.blocked_gate_refs
  end

  test "fails closed when persistence, replay, eval, checkpoint, or promotion epochs diverge" do
    assert {:error, receipt} =
             control_attrs()
             |> Map.put(:checkpoint_epoch_consistency_ref, "diverged")
             |> AdaptiveControlEngine.evaluate()

    assert receipt.status == :blocked
    assert "gate:checkpoint_epoch" in receipt.blocked_gate_refs

    assert {:error, eval_receipt} =
             control_attrs()
             |> Map.put(:eval_consistency_ref, "diverged")
             |> AdaptiveControlEngine.evaluate()

    assert "gate:eval" in eval_receipt.blocked_gate_refs
  end

  test "rejects raw prompt, provider payload, model output, memory body, and credential fields" do
    assert {:error, {:forbidden_raw_field, :raw_prompt}} =
             control_attrs()
             |> Map.put(:raw_prompt, "do not project")
             |> AdaptiveControlEngine.evaluate()
  end

  test "records promotion truth only after eval and Citadel gates are present" do
    assert {:error, receipt} =
             promotion_attrs()
             |> Map.put(:eval_refs, [])
             |> AdaptiveControlEngine.record_promotion()

    assert receipt.status == :denied
    assert "gate:eval" in receipt.blocked_gate_refs

    assert {:ok, receipt} = AdaptiveControlEngine.record_promotion(promotion_attrs())

    assert receipt.status == :promoted
    assert receipt.citadel_authority_ref == "authority://citadel/promotion/a"
    assert receipt.candidate_ref == "memory-candidate://tenant-a/a"
    assert receipt.promotion_ref == "memory-promotion://tenant-a/a"
  end

  test "records rollback truth and rejects rollback without Citadel authority" do
    assert {:error, receipt} =
             rollback_attrs()
             |> Map.delete(:citadel_authority_ref)
             |> AdaptiveControlEngine.record_rollback()

    assert receipt.status == :denied
    assert "gate:citadel_authority" in receipt.blocked_gate_refs

    assert {:ok, receipt} = AdaptiveControlEngine.record_rollback(rollback_attrs())

    assert receipt.status == :rolled_back
    assert receipt.rollback_ref == "memory-rollback://tenant-a/a"
    assert receipt.restored_ref == "memory://tenant-a/promoted/previous"
  end

  defp control_attrs do
    %{
      control_run_ref: "adaptive-control://phase-13/worker",
      tenant_ref: "tenant://adaptive",
      source_coordination_run_ref: "coordination-run://trinity/repair",
      trace_dataset_ref: "trace-dataset://trinity/repair",
      trace_refs: ["trace://trinity/repair"],
      replay_dataset_refs: ["replay-dataset://trinity/repair"],
      eval_dataset_refs: ["eval-dataset://trinity/repair"],
      optimization_run_ref: "optimization-run://gepa/role-worker",
      candidate_ref: "candidate://role-worker/v2",
      optimization_target_refs: ["gepa-target://role-prompt"],
      role_prompt_refs: ["prompt://role/worker/v2"],
      verifier_prompt_refs: ["prompt://verifier/worker/v2"],
      context_budget_refs: ["context-budget://worker/v2"],
      memory_policy_refs: ["memory-policy://worker/retrieval/v2"],
      tool_policy_refs: ["tool-policy://worker/v2"],
      fallback_policy_refs: ["fallback-policy://router/v2"],
      termination_threshold_refs: ["termination-threshold://worker/v2"],
      eval_refs: ["eval://candidate/worker/v2"],
      replay_refs: ["replay://candidate/worker/v2"],
      guardrail_refs: ["guardrail://candidate/worker/v2"],
      budget_refs: ["budget://candidate/worker/v2"],
      gate_evidence_refs: [
        "gate-evidence://shadow",
        "gate-evidence://canary",
        "gate-evidence://eval",
        "gate-evidence://replay",
        "gate-evidence://guardrail",
        "gate-evidence://budget",
        "gate-evidence://approval"
      ],
      threshold_refs: [
        "threshold://improvement",
        "threshold://regression",
        "threshold://budget",
        "threshold://approval"
      ],
      shadow_ref: "shadow://candidate/worker/v2",
      canary_ref: "canary://candidate/worker/v2",
      operator_approval_refs: ["approval://operator/worker/v2"],
      promotion_ref: "promotion://candidate/worker/v2",
      rollback_ref: "rollback://candidate/worker/v1",
      stale_artifact_fence_refs: ["fence://candidate/stale/v1"],
      artifact_lock_refs: ["artifact-lock://role-worker"],
      persistence_profile_ref: "persistence-profile://memory-default",
      replay_bundle_ref: "replay-bundle://candidate/worker/v2",
      checkpoint_epoch_ref: "checkpoint-epoch://candidate/worker/v2",
      promotion_epoch_ref: "promotion-epoch://candidate/worker/v2",
      persistence_consistency_ref: "consistent",
      replay_consistency_ref: "consistent",
      eval_consistency_ref: "consistent",
      checkpoint_epoch_consistency_ref: "consistent",
      promotion_epoch_consistency_ref: "consistent",
      appkit_projection_refs: ["appkit://adaptive-control/worker"],
      ground_plane_fence_refs: ["ground-plane://stale-candidate/fence"],
      audit_refs: ["audit://adaptive-control/worker"]
    }
  end

  defp promotion_attrs do
    %{
      candidate_ref: "memory-candidate://tenant-a/a",
      promotion_ref: "memory-promotion://tenant-a/a",
      rollback_ref: "memory-rollback://tenant-a/a",
      tenant_ref: "tenant://adaptive",
      citadel_authority_ref: "authority://citadel/promotion/a",
      eval_refs: ["eval://memory/a"],
      trace_ref: "trace://adaptive/promotion/a",
      appkit_projection_ref: "appkit://memory/promotion/a"
    }
  end

  defp rollback_attrs do
    %{
      candidate_ref: "memory-candidate://tenant-a/a",
      rollback_ref: "memory-rollback://tenant-a/a",
      restored_ref: "memory://tenant-a/promoted/previous",
      tenant_ref: "tenant://adaptive",
      citadel_authority_ref: "authority://citadel/rollback/a",
      trace_ref: "trace://adaptive/rollback/a",
      appkit_projection_ref: "appkit://memory/rollback/a"
    }
  end
end
