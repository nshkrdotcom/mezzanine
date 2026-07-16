defmodule Mezzanine.AIRun.EnvelopeTest do
  use ExUnit.Case, async: true

  alias Mezzanine.AIRun.Envelope

  test "carries required adaptive refs without raw payloads" do
    assert {:ok, envelope} =
             Envelope.new(%{
               ai_run_ref: "ai_run://optimization/1",
               run_class: :optimization_run,
               tenant_ref: "tenant://demo",
               subject_ref: "subject://role-pack/demo",
               authority_ref: "authority://decision/1",
               actor_ref: "actor://operator/1",
               persistence_profile_ref: durable_profile(),
               memory_ref_set: ["memory://working/1"],
               prompt_ref_set: ["prompt://role/1"],
               model_profile_ref_set: ["model://mock/evaluator"],
               operation_policy_ref: "policy://model/evaluate",
               target_ref_set: ["target://mock"],
               trace_ref_set: ["trace://optimization/1"],
               eval_ref_set: ["eval://suite/1"],
               budget_ref_set: ["budget://optimization/1"],
               promotion_ref_set: ["promotion://candidate/1"],
               parent_run_ref: "ai_run://adaptive/parent",
               idempotency_ref: "idem://optimization/1"
             })

    assert envelope.run_class == :optimization_run
    assert envelope.lifecycle_state == :created
    assert envelope.parent_run_ref == "ai_run://adaptive/parent"
    assert Envelope.redacted_projection(envelope).prompt_ref_set == ["prompt://role/1"]
  end

  test "rejects raw payload shaped fields anywhere in refs" do
    assert {:error, {:raw_projection_forbidden, [:prompt_ref_set, :raw_prompt]}} =
             Envelope.new(%{
               ai_run_ref: "ai_run://bad",
               run_class: :inference_call,
               tenant_ref: "tenant://demo",
               authority_ref: "authority://decision/1",
               actor_ref: "actor://operator/1",
               persistence_profile_ref: durable_profile(),
               prompt_ref_set: [%{raw_prompt: "do not store this"}]
             })
  end

  test "rejects missing tenant scope" do
    assert {:error, {:missing_ref, :tenant_ref}} =
             Envelope.new(%{
               ai_run_ref: "ai_run://bad",
               run_class: :coordination_run,
               authority_ref: "authority://decision/1",
               actor_ref: "actor://operator/1",
               persistence_profile_ref: durable_profile()
             })
  end

  test "rejects an omitted persistence profile" do
    assert {:error, :persistence_profile_required} =
             Envelope.new(%{
               ai_run_ref: "ai_run://missing-persistence",
               run_class: :inference_call,
               tenant_ref: "tenant://demo",
               authority_ref: "authority://decision/1",
               actor_ref: "actor://operator/1"
             })
  end

  defp durable_profile do
    %{id: :ops_durable, selected_tier: :postgres_shared}
  end
end
