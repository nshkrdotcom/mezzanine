defmodule Mezzanine.AgentTurnEngine.ReducerTest do
  use ExUnit.Case, async: true

  alias Mezzanine.AgentTurnEngine.{
    AgentConversationEvent,
    AgentPendingInteraction,
    AgentTurnLedger,
    Reducer
  }

  @now ~U[2026-05-20 12:00:00Z]

  test "append enforces monotonic sequence and records conversation progress" do
    state = new_state()

    assert {:ok, state} =
             state
             |> Reducer.append!(conversation_event(seq: 1))

    assert state.ledger.next_seq == 2
    assert state.ledger.last_conversation_seq == 1

    assert {:error, {:invalid, :seq, {:expected, 2}}} =
             Reducer.append(
               state,
               conversation_event(event_ref: "agent-conv-event://event-4", seq: 4)
             )
  end

  test "duplicate event refs and idempotency keys are no-ops" do
    state = new_state()
    event = conversation_event(seq: 1)

    assert {:ok, state} = Reducer.append(state, event)
    assert {:duplicate, duplicate_state} = Reducer.append(state, %{event | seq: 2})

    assert duplicate_state == state
  end

  test "terminal completion requires receipt or evidence posture" do
    state = new_state()

    assert {:error, {:invalid, :terminal_event, :receipt_or_evidence_required}} =
             Reducer.append(
               state,
               conversation_event(seq: 1, event_type: :run_completed, evidence_refs: [])
             )

    assert {:ok, state} =
             Reducer.append(
               state,
               conversation_event(
                 seq: 1,
                 event_type: :run_completed,
                 evidence_refs: ["receipt://terminal-1"]
               )
             )

    assert state.ledger.status == :completed
  end

  test "pending interactions open, resolve, and issue replay-safe cursors" do
    state = new_state()
    pending = pending_interaction(opened_seq: 1)

    assert {:ok, state} = Reducer.open_pending(state, pending)
    assert state.ledger.status == :pending
    assert state.ledger.pending_interaction_ref == pending.pending_ref

    assert {:ok, state, cursor} =
             Reducer.issue_cursor(state, %{
               cursor_ref: "agent-cursor://cursor-open",
               actor_ref: "actor://operator-1",
               visibility: :operator,
               issued_at: @now,
               expires_at: DateTime.add(@now, 3600, :second)
             })

    assert cursor.last_seq_seen == state.ledger.next_seq - 1
    assert state.ledger.cursor_ref == cursor.cursor_ref

    assert {:ok, state, resolved} =
             Reducer.resolve_pending(state, pending.pending_ref, :approved, @now)

    assert resolved.status == :approved
    assert state.ledger.status == :running
    assert state.ledger.pending_interaction_ref == nil
  end

  defp new_state do
    {:ok, ledger} = AgentTurnLedger.new(valid_ledger_attrs())
    Reducer.new!(ledger)
  end

  defp conversation_event(overrides) do
    attrs = Map.merge(valid_conversation_attrs(), Map.new(overrides))
    {:ok, event} = AgentConversationEvent.new(attrs)
    event
  end

  defp pending_interaction(overrides) do
    attrs = Map.merge(valid_pending_attrs(), Map.new(overrides))
    {:ok, pending} = AgentPendingInteraction.new(attrs)
    pending
  end

  defp valid_ledger_attrs do
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
  end

  defp valid_conversation_attrs do
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
  end

  defp valid_pending_attrs do
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
      opened_seq: 3,
      status: :open,
      expires_at: DateTime.add(@now, 3600, :second),
      resolved_at: nil
    }
  end
end
