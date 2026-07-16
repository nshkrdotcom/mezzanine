defmodule Mezzanine.AIRun.RunGraphTest do
  use ExUnit.Case, async: true

  alias Mezzanine.AIRun.{Envelope, RunGraph}

  test "links child runs with idempotency cancellation retry supersession and rollback refs" do
    parent = envelope!("ai_run://optimization/parent", :optimization_run)
    child = envelope!("ai_run://optimization/candidate/1", :optimization_candidate_eval)

    assert {:ok, edge, linked_child} =
             RunGraph.link_child(parent, child,
               idempotency_ref: "idem://candidate/1",
               cancellation_ref: "cancel://candidate/1",
               retry_ref: "retry://candidate/1",
               supersession_ref: "supersede://candidate/1",
               rollback_ref: "rollback://candidate/1"
             )

    assert edge.parent_run_ref == parent.ai_run_ref
    assert edge.child_run_ref == child.ai_run_ref
    assert edge.rollback_ref == "rollback://candidate/1"
    assert linked_child.parent_run_ref == parent.ai_run_ref
    assert linked_child.idempotency_ref == "idem://candidate/1"
  end

  test "rejects cross-tenant parent child linkage" do
    parent = envelope!("ai_run://parent", :coordination_run, tenant_ref: "tenant://a")
    child = envelope!("ai_run://child", :router_decision, tenant_ref: "tenant://b")

    assert {:error, :tenant_mismatch} = RunGraph.link_child(parent, child)
  end

  defp envelope!(ref, class, overrides \\ []) do
    attrs =
      Map.merge(
        %{
          ai_run_ref: ref,
          run_class: class,
          tenant_ref: "tenant://demo",
          authority_ref: "authority://decision/1",
          actor_ref: "actor://operator/1",
          persistence_profile_ref: %{id: :ops_durable, selected_tier: :postgres_shared}
        },
        Map.new(overrides)
      )

    assert {:ok, envelope} = Envelope.new(attrs)
    envelope
  end
end
