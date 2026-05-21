defmodule Mezzanine.Decisions.AgentTurnReviewTest do
  use ExUnit.Case, async: true

  alias Mezzanine.AgentTurnEngine.{AgentPendingInteraction, PendingDecision}
  alias Mezzanine.Decisions.{AgentTurnReview, DecisionRecord}

  @now ~U[2026-05-21 12:00:00Z]

  test "maps resolved decision records into pending decision bindings" do
    decision_record = %DecisionRecord{
      id: "11111111-1111-1111-1111-111111111111",
      decision_value: "approved",
      row_version: 7,
      resolved_at: @now
    }

    assert {:ok, %PendingDecision{} = decision} =
             AgentTurnReview.to_pending_decision(decision_record, pending(), %{})

    assert decision.decision_ref == "decision://11111111-1111-1111-1111-111111111111"
    assert decision.pending_ref == "agent-pending://pending-1"
    assert decision.decision == :approved

    assert decision.authority_revision_ref ==
             "authority-revision://decision/11111111-1111-1111-1111-111111111111/7"
  end

  test "rejects unsupported terminal decision values" do
    decision_record = %DecisionRecord{
      id: "11111111-1111-1111-1111-111111111111",
      decision_value: "maybe",
      row_version: 1,
      resolved_at: @now
    }

    assert {:error, {:invalid, :decision_value, :unsupported}} =
             AgentTurnReview.to_pending_decision(decision_record, pending(), %{})
  end

  test "maps pending decision values to DecisionEngine terminal actions" do
    assert :accept = AgentTurnReview.terminal_action(pending_decision(:approved))
    assert :reject = AgentTurnReview.terminal_action(pending_decision(:denied))
    assert :expire = AgentTurnReview.terminal_action(pending_decision(:expired))
    assert :reject = AgentTurnReview.terminal_action(pending_decision(:cancelled))
  end

  defp pending do
    {:ok, pending} =
      AgentPendingInteraction.new(%{
        pending_ref: "agent-pending://pending-1",
        ledger_ref: "agent-ledger://run-1",
        decision_ref: "decision://review-1",
        tenant_ref: "tenant://tenant-1",
        actor_ref: "actor://operator-1",
        kind: :approval_required,
        prompt_summary: "Approve file write?",
        requested_action_ref: "action://file-write-1",
        authority_ref: "authority://policy-1",
        opened_seq: 1,
        status: :open,
        expires_at: DateTime.add(@now, 3600, :second),
        resolved_at: nil
      })

    pending
  end

  defp pending_decision(decision) do
    {:ok, pending_decision} =
      PendingDecision.new(%{
        decision_ref: "decision://review-1",
        pending_ref: "agent-pending://pending-1",
        tenant_ref: "tenant://tenant-1",
        actor_ref: "actor://operator-1",
        authority_ref: "authority://policy-1",
        authority_revision_ref: "authority-revision://policy-1/1",
        decision: decision,
        idempotency_key: "decision-idem-#{decision}",
        decided_at: @now
      })

    pending_decision
  end
end
