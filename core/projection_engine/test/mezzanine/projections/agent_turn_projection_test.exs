defmodule Mezzanine.Projections.AgentTurnProjectionTest do
  use ExUnit.Case, async: true

  alias Mezzanine.AgentTurnEngine.Projection.Row
  alias Mezzanine.Projections.AgentTurnProjection

  @now ~U[2026-05-21 12:00:00Z]

  test "maps product-safe agent rows into durable projection attrs" do
    attrs =
      AgentTurnProjection.to_upsert_attrs(row(), %{
        installation_id: "inst-1",
        subject_id: "11111111-1111-1111-1111-111111111111",
        execution_id: "22222222-2222-2222-2222-222222222222",
        trace_id: "trace-1"
      })

    assert attrs.projection_name == "agent_turn_timeline"
    assert attrs.row_key == "agent-projection-row://run-1/1"
    assert attrs.projection_kind == "agent_turn"
    assert attrs.sort_key == 1
    assert attrs.payload["summary"] == "Assistant response is available."
    assert attrs.payload["evidence_refs"] == ["evidence://summary-1"]
  end

  defp row do
    %Row{
      row_ref: "agent-projection-row://run-1/1",
      ledger_ref: "agent-ledger://run-1",
      seq: 1,
      event_ref: "agent-conv-event://event-1",
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
end
