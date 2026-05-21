defmodule Mezzanine.AgentTurnEngine.PropertyTest do
  use ExUnit.Case, async: true
  use ExUnitProperties

  alias Mezzanine.AgentTurnEngine.{
    AgentConversationEvent,
    AgentTurnLedger,
    Store.Memory
  }

  @now ~U[2026-05-20 12:00:00Z]

  property "monotonic append advances next sequence by event count" do
    check all(event_count <- integer(1..20)) do
      {:ok, store} =
        Enum.reduce(1..event_count, Memory.put_ledger(Memory.new(), ledger()), fn seq,
                                                                                  {:ok, store} ->
          Memory.append_event(store, event(seq))
        end)

      assert store.ledgers["agent-ledger://run-1"].next_seq == event_count + 1
    end
  end

  property "duplicate event refs remain idempotent" do
    check all(seq <- integer(1..20)) do
      {:ok, store} =
        Memory.new()
        |> Memory.put_ledger(ledger(next_seq: seq))
        |> Memory.append_event(event(seq))

      assert {:duplicate, duplicate_store} =
               Memory.append_event(store, %{event(seq) | seq: seq + 1})

      assert duplicate_store == store
    end
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

  defp event(seq) do
    attrs = %{
      event_ref: "agent-conv-event://event-#{seq}",
      ledger_ref: "agent-ledger://run-1",
      seq: seq,
      event_type: :assistant_message_available,
      visibility: :product,
      summary: "Assistant response #{seq}",
      payload_ref: "payload://conversation-#{seq}",
      redaction_class: :safe,
      authority_ref: "authority://policy-1",
      evidence_refs: ["evidence://summary-#{seq}"],
      occurred_at: DateTime.add(@now, seq, :second)
    }

    {:ok, event} = AgentConversationEvent.new(attrs)
    event
  end
end
