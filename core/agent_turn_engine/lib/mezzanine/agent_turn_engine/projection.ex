defmodule Mezzanine.AgentTurnEngine.Projection do
  @moduledoc """
  Product-safe projection rows derived from agent conversation events.
  """

  alias Mezzanine.AgentTurnEngine.AgentConversationEvent

  defmodule Row do
    @moduledoc """
    Product-safe row for AppKit timelines and reconnect catch-up views.
    """

    @enforce_keys [
      :row_ref,
      :ledger_ref,
      :seq,
      :event_ref,
      :event_type,
      :visibility,
      :summary,
      :payload_ref,
      :redaction_class,
      :authority_ref,
      :evidence_refs,
      :occurred_at
    ]
    defstruct @enforce_keys

    @type t :: %__MODULE__{}
  end

  @spec reduce(AgentConversationEvent.t()) :: {:ok, Row.t()}
  def reduce(%AgentConversationEvent{} = event) do
    {:ok,
     %Row{
       row_ref: row_ref(event.ledger_ref, event.seq),
       ledger_ref: event.ledger_ref,
       seq: event.seq,
       event_ref: event.event_ref,
       event_type: event.event_type,
       visibility: event.visibility,
       summary: event.summary,
       payload_ref: event.payload_ref,
       redaction_class: event.redaction_class,
       authority_ref: event.authority_ref,
       evidence_refs: event.evidence_refs,
       occurred_at: event.occurred_at
     }}
  end

  defp row_ref(ledger_ref, seq) do
    "agent-projection-row://" <> ledger_suffix(ledger_ref) <> "/" <> Integer.to_string(seq)
  end

  defp ledger_suffix("agent-ledger://" <> suffix), do: suffix
  defp ledger_suffix(other), do: other
end
