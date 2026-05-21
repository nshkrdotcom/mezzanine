defmodule Mezzanine.AgentTurnEngine.StoreAndReplayTest do
  use ExUnit.Case, async: true

  alias Mezzanine.AgentTurnEngine.{
    AgentConversationEvent,
    AgentPendingInteraction,
    AgentRunCursor,
    AgentTurnLedger,
    ExecutionReplay,
    PendingDecision,
    Store.Memory
  }

  @now ~U[2026-05-20 12:00:00Z]

  test "memory store appends events and catch-up never dispatches lower work" do
    {:ok, store} =
      Memory.new()
      |> Memory.put_ledger(ledger())
      |> Memory.append_event(conversation_event(seq: 1, summary: "First"))
      |> Memory.append_event(
        conversation_event(seq: 2, event_ref: "agent-conv-event://event-2", summary: "Second")
      )

    cursor = cursor(last_seq_seen: 1, visibility: :product)

    assert {:ok, store, page} = Memory.catch_up(store, cursor)
    assert Enum.map(page.events, & &1.summary) == ["Second"]
    assert page.cursor.last_seq_seen == 2
    assert store.lower_dispatch_count == 0
  end

  test "pending resume requires decision binding and current authority" do
    {:ok, store} =
      Memory.new()
      |> Memory.put_ledger(ledger())
      |> Memory.open_pending(pending(opened_seq: 1))

    assert {:error, {:invalid, :decision_binding, :required}} =
             Memory.resolve_pending(store, "agent-pending://pending-1", nil)

    stale_decision = decision(authority_ref: "authority://stale")

    assert {:error, {:invalid, :authority_ref, :stale}} =
             Memory.resolve_pending(store, "agent-pending://pending-1", stale_decision)

    assert {:ok, store, resolved} =
             Memory.resolve_pending(store, "agent-pending://pending-1", decision())

    assert resolved.status == :approved
    assert store.lower_dispatch_count == 0

    assert {:duplicate, _store, ^resolved} =
             Memory.resolve_pending(store, "agent-pending://pending-1", decision())
  end

  test "replay modes read existing facts without accidental lower reexecution" do
    {:ok, store} =
      Memory.new()
      |> Memory.put_ledger(ledger())
      |> Memory.append_event(conversation_event(seq: 1, summary: "First"))
      |> Memory.append_event(
        conversation_event(seq: 2, event_ref: "agent-conv-event://event-2", summary: "Second")
      )

    assert {:ok, store, replay_page} =
             Memory.replay(store, replay(replay_kind: :catchup, from_seq: 0, to_seq: 2))

    assert length(replay_page.events) == 2
    assert store.lower_dispatch_count == 0

    assert {:ok, _store, projection} =
             Memory.replay(
               store,
               replay(replay_kind: :reconstruct_projection, from_seq: 0, to_seq: 2)
             )

    assert Enum.map(projection.rows, & &1.summary) == ["First", "Second"]

    assert {:error, {:invalid, :retry_lower_effect, :evidence_required}} =
             Memory.replay(
               store,
               replay(
                 replay_kind: :retry_lower_effect,
                 lower_reexecution_allowed?: true,
                 evidence_refs: []
               )
             )
  end

  test "projection rows are product-safe conversation summaries" do
    {:ok, store} =
      Memory.new()
      |> Memory.put_ledger(ledger())
      |> Memory.append_event(conversation_event(seq: 1, payload_ref: "payload://safe-summary"))

    assert [row] = Memory.projection_rows(store, "agent-ledger://run-1")
    assert row.row_ref == "agent-projection-row://run-1/1"
    assert row.payload_ref == "payload://safe-summary"
    assert row.summary == "Assistant response is available."
  end

  defp ledger(overrides \\ []) do
    attrs =
      %{
        ledger_ref: "agent-ledger://run-1",
        tenant_ref: "tenant://tenant-1",
        installation_ref: "installation://installation-1",
        subject_ref: "subject://subject-1",
        platform_run_ref: "run://platform-run-1",
        platform_execution_ref: "execution://execution-1",
        actor_ref: "actor://operator-1",
        authority_ref: "authority://policy-1",
        idempotency_key: "idem-ledger-1",
        status: :initialized,
        next_seq: 1,
        last_reduced_seq: 0,
        last_conversation_seq: 0,
        last_execution_seq: 0,
        cursor_ref: nil,
        replay_ref: nil,
        pending_interaction_ref: nil,
        created_at: @now,
        updated_at: @now
      }
      |> Map.merge(Map.new(overrides))

    {:ok, ledger} = AgentTurnLedger.new(attrs)
    ledger
  end

  defp conversation_event(overrides) do
    attrs =
      %{
        event_ref: "agent-conv-event://event-1",
        ledger_ref: "agent-ledger://run-1",
        seq: 1,
        event_type: :assistant_message_available,
        visibility: :product,
        summary: "Assistant response is available.",
        payload_ref: "payload://conversation-1",
        redaction_class: :safe,
        authority_ref: "authority://policy-1",
        evidence_refs: ["evidence://summary-1"],
        occurred_at: @now
      }
      |> Map.merge(Map.new(overrides))

    {:ok, event} = AgentConversationEvent.new(attrs)
    event
  end

  defp cursor(overrides) do
    attrs =
      %{
        cursor_ref: "agent-cursor://cursor-1",
        ledger_ref: "agent-ledger://run-1",
        tenant_ref: "tenant://tenant-1",
        actor_ref: "actor://operator-1",
        last_seq_seen: 0,
        visibility: :product,
        issued_at: @now,
        expires_at: DateTime.add(@now, 3600, :second)
      }
      |> Map.merge(Map.new(overrides))

    {:ok, cursor} = AgentRunCursor.new(attrs)
    cursor
  end

  defp pending(overrides) do
    attrs =
      %{
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
      }
      |> Map.merge(Map.new(overrides))

    {:ok, pending} = AgentPendingInteraction.new(attrs)
    pending
  end

  defp decision(overrides \\ []) do
    attrs =
      %{
        decision_ref: "decision://review-1",
        pending_ref: "agent-pending://pending-1",
        tenant_ref: "tenant://tenant-1",
        actor_ref: "actor://operator-1",
        authority_ref: "authority://policy-1",
        authority_revision_ref: "authority-revision://policy-1/1",
        decision: :approved,
        idempotency_key: "decision-idem-1",
        decided_at: @now
      }
      |> Map.merge(Map.new(overrides))

    {:ok, decision} = PendingDecision.new(attrs)
    decision
  end

  defp replay(overrides) do
    attrs =
      %{
        replay_ref: "agent-replay://replay-1",
        ledger_ref: "agent-ledger://run-1",
        replay_kind: :catchup,
        from_seq: 0,
        to_seq: 2,
        lower_reexecution_allowed?: false,
        idempotency_key: "idem-replay-1",
        authority_ref: "authority://policy-1",
        evidence_refs: ["evidence://summary-1"],
        status: :planned,
        created_at: @now
      }
      |> Map.merge(Map.new(overrides))

    {:ok, replay} = ExecutionReplay.new(attrs)
    replay
  end
end
