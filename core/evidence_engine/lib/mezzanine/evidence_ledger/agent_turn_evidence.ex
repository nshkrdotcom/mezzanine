defmodule Mezzanine.EvidenceLedger.AgentTurnEvidence do
  @moduledoc """
  EvidenceEngine adapter for agent-turn evidence refs.

  The adapter keeps agent events provider-neutral while allowing the durable
  evidence ledger to collect or verify rows for event summaries.
  """

  alias Mezzanine.AgentTurnEngine.AgentConversationEvent
  alias Mezzanine.EvidenceLedger.EvidenceRecord

  @collector_ref "mezzanine_agent_turn_engine"
  @evidence_kind "agent_conversation_event"

  @spec collect_conversation_event(AgentConversationEvent.t(), map()) ::
          {:ok, EvidenceRecord.t()} | {:error, term()}
  def collect_conversation_event(%AgentConversationEvent{} = event, attrs) when is_map(attrs) do
    attrs = to_collect_attrs(event, attrs)

    EvidenceRecord.verify_or_update(
      Map.fetch!(attrs, :subject_id),
      Map.fetch!(attrs, :execution_id),
      Map.fetch!(attrs, :evidence_kind),
      attrs
    )
  end

  @spec to_collect_attrs(AgentConversationEvent.t(), map()) :: map()
  def to_collect_attrs(%AgentConversationEvent{} = event, attrs) when is_map(attrs) do
    %{
      installation_id: fetch_required!(attrs, :installation_id),
      subject_id: fetch_required!(attrs, :subject_id),
      execution_id: fetch_required!(attrs, :execution_id),
      evidence_kind: map_value(attrs, :evidence_kind) || @evidence_kind,
      collector_ref: map_value(attrs, :collector_ref) || @collector_ref,
      content_ref: event.payload_ref,
      status: map_value(attrs, :status) || "verified",
      metadata: metadata(event, attrs),
      trace_id: fetch_required!(attrs, :trace_id),
      causation_id: map_value(attrs, :causation_id) || event.event_ref,
      actor_ref: map_value(attrs, :actor_ref) || %{"authority_ref" => event.authority_ref}
    }
  end

  @spec evidence_ref(EvidenceRecord.t()) :: String.t()
  def evidence_ref(%EvidenceRecord{id: id}), do: "evidence://" <> to_string(id)

  defp metadata(%AgentConversationEvent{} = event, attrs) do
    Map.merge(
      %{
        "event_ref" => event.event_ref,
        "ledger_ref" => event.ledger_ref,
        "seq" => event.seq,
        "event_type" => Atom.to_string(event.event_type),
        "visibility" => Atom.to_string(event.visibility),
        "redaction_class" => Atom.to_string(event.redaction_class),
        "authority_ref" => event.authority_ref,
        "evidence_refs" => event.evidence_refs
      },
      map_value(attrs, :metadata) || %{}
    )
  end

  defp fetch_required!(attrs, key) do
    case map_value(attrs, key) do
      nil -> raise ArgumentError, "missing required #{key}"
      value -> value
    end
  end

  defp map_value(attrs, key) when is_atom(key),
    do: Map.get(attrs, key, Map.get(attrs, Atom.to_string(key)))
end
