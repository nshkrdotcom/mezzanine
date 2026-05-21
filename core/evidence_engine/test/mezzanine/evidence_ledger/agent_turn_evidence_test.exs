defmodule Mezzanine.EvidenceLedger.AgentTurnEvidenceTest do
  use ExUnit.Case, async: true

  alias Mezzanine.AgentTurnEngine.AgentConversationEvent
  alias Mezzanine.EvidenceLedger.AgentTurnEvidence

  @now ~U[2026-05-21 12:00:00Z]

  test "maps conversation events into durable evidence collection attrs" do
    attrs =
      AgentTurnEvidence.to_collect_attrs(event(), %{
        installation_id: "inst-1",
        subject_id: "11111111-1111-1111-1111-111111111111",
        execution_id: "22222222-2222-2222-2222-222222222222",
        trace_id: "trace-1",
        actor_ref: %{"actor_ref" => "actor://operator-1"}
      })

    assert attrs.collector_ref == "mezzanine_agent_turn_engine"
    assert attrs.evidence_kind == "agent_conversation_event"
    assert attrs.content_ref == "payload://conversation-1"
    assert attrs.status == "verified"
    assert attrs.metadata["event_ref"] == "agent-conv-event://event-1"
    assert attrs.metadata["evidence_refs"] == ["evidence://summary-1"]
  end

  defp event do
    {:ok, event} =
      AgentConversationEvent.new(%{
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
      })

    event
  end
end
