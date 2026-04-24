defmodule Mezzanine.ConfigRegistry.PolicyContractsTest do
  use ExUnit.Case, async: true

  alias Mezzanine.ConfigRegistry.{
    InvalidatePolicy,
    PolicyContracts,
    PromotePolicy,
    ReadPolicy,
    ShareUpPolicy,
    TransformPolicy,
    WritePolicy
  }

  test "builds all six Phase 7 policy contracts" do
    assert {:ok, %ReadPolicy{contract_name: "Platform.ReadPolicy.V1"}} =
             ReadPolicy.new(read_policy_attrs())

    assert {:ok, %WritePolicy{contract_name: "Platform.WritePolicy.V1"}} =
             WritePolicy.new(write_policy_attrs())

    assert {:ok, %TransformPolicy{contract_name: "Platform.TransformPolicy.V1"}} =
             TransformPolicy.new(transform_policy_attrs())

    assert {:ok, %ShareUpPolicy{contract_name: "Platform.ShareUpPolicy.V1"}} =
             ShareUpPolicy.new(share_up_policy_attrs())

    assert {:ok, %PromotePolicy{contract_name: "Platform.PromotePolicy.V1"}} =
             PromotePolicy.new(promote_policy_attrs())

    assert {:ok, %InvalidatePolicy{contract_name: "Platform.InvalidatePolicy.V1"}} =
             InvalidatePolicy.new(invalidate_policy_attrs())
  end

  test "rejects invalid granularity and identity share-up" do
    assert {:error, {:invalid_granularity_scope, :project}} =
             read_policy_attrs()
             |> Map.put(:granularity_scope, :project)
             |> ReadPolicy.new()

    assert {:error, :identity_share_up_forbidden} =
             share_up_policy_attrs()
             |> Map.put(:transform_ref, "identity")
             |> ShareUpPolicy.new()
  end

  test "composes transform pipelines and preserves stochastic provenance" do
    assert {:ok, deterministic} =
             transform_policy_attrs(
               policy_id: "policy-transform-redact",
               pipeline: [%{kind: :redact, patterns: ["secret"]}],
               determinism: :deterministic
             )
             |> TransformPolicy.new()

    assert {:ok, stochastic} =
             transform_policy_attrs(
               policy_id: "policy-transform-summarize",
               pipeline: [%{kind: :summarize, model_ref: "model://summarizer", budget: 120}],
               determinism: :stochastic
             )
             |> TransformPolicy.new()

    assert {:ok, composed} = TransformPolicy.compose(deterministic, stochastic)

    assert Enum.map(composed.pipeline, & &1.kind) == [:redact, :summarize]
    assert composed.determinism == :stochastic

    assert composed.output_hash_anchor ==
             "composed:policy-transform-redact+policy-transform-summarize"
  end

  test "dumps policy contracts into registry specs" do
    assert {:ok, policy} = ReadPolicy.new(read_policy_attrs())

    dumped = PolicyContracts.dump(policy)

    assert dumped.contract_name == "Platform.ReadPolicy.V1"
    assert dumped.policy_id == "policy-read-global"
    assert dumped.granularity_scope == :global
  end

  defp read_policy_attrs(overrides \\ []) do
    %{
      policy_id: "policy-read-global",
      version: 1,
      granularity_scope: :global,
      candidate_filter: %{tiers: [:private, :shared]},
      ranking_fn: "recency_then_similarity",
      top_k_private: 5,
      top_k_shared: 3,
      top_k_governed: 1,
      transform_ref: "transform://identity",
      degraded_behavior: :fail_empty,
      audit_level: :standard
    }
    |> Map.merge(Map.new(overrides))
  end

  defp write_policy_attrs(overrides \\ []) do
    %{
      policy_id: "policy-write-private",
      version: 1,
      granularity_scope: :tenant,
      target_tier: :private,
      retention: %{class: "standard"},
      transform_ref: "transform://identity",
      dedupe_fn: "content_hash",
      share_up_eligibility: %{eligible?: false},
      audit_level: :standard
    }
    |> Map.merge(Map.new(overrides))
  end

  defp transform_policy_attrs(overrides \\ []) do
    %{
      policy_id: "policy-transform-default",
      version: 1,
      granularity_scope: :tenant,
      pipeline: [%{kind: :redact, patterns: ["secret"]}],
      determinism: :deterministic,
      output_hash_anchor:
        "sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
      access_projection_rule: %{mode: "same_access"}
    }
    |> Map.merge(Map.new(overrides))
  end

  defp share_up_policy_attrs(overrides \\ []) do
    %{
      policy_id: "policy-share-up",
      version: 1,
      granularity_scope: :installation,
      transform_ref: "transform://redact-before-share",
      eligibility_fn: "eligible_for_workspace_memory",
      target_scope_predicate: %{scope_kind: "workspace"},
      access_projection_rule: %{mode: "workspace_scope"},
      audit_level: :strict
    }
    |> Map.merge(Map.new(overrides))
  end

  defp promote_policy_attrs(overrides \\ []) do
    %{
      policy_id: "policy-promote",
      version: 1,
      granularity_scope: :installation,
      review_required: true,
      quorum_ref: "quorum://memory-review/default",
      auto_decide: false,
      evidence_requirements: [%{kind: "review_packet"}],
      audit_level: :strict
    }
    |> Map.merge(Map.new(overrides))
  end

  defp invalidate_policy_attrs(overrides \\ []) do
    %{
      policy_id: "policy-invalidate",
      version: 1,
      granularity_scope: :tenant,
      cascade_rule: %{private: true, shared: true, governed: true},
      retention_override: %{mode: "delete"},
      audit_level: :strict
    }
    |> Map.merge(Map.new(overrides))
  end
end
