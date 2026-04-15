defmodule Mezzanine.Audit.EvidenceManifest do
  @moduledoc """
  Pure evidence-manifest assembly from audit events and evidence items.
  """

  @type manifest :: %{
          summary: String.t(),
          evidence_manifest: map(),
          completeness_status: map()
        }

  @spec build([struct()], [struct()]) :: manifest()
  def build(events, evidence_items) when is_list(events) and is_list(evidence_items) do
    event_counts =
      events
      |> Enum.frequencies_by(&Atom.to_string(&1.event_kind))

    item_counts =
      evidence_items
      |> Enum.frequencies_by(fn item -> "#{item.kind}:#{item.status}" end)

    last_event = List.last(Enum.sort_by(events, &{&1.occurred_at, &1.id}))

    %{
      summary: summary(last_event, events, evidence_items),
      evidence_manifest: %{
        audit_event_count: length(events),
        evidence_item_count: length(evidence_items),
        event_counts: event_counts,
        evidence_item_counts: item_counts,
        last_event_kind: last_event && Atom.to_string(last_event.event_kind),
        last_event_at: last_event && last_event.occurred_at
      },
      completeness_status: %{
        audit_events: presence(length(events)),
        evidence_items: presence(length(evidence_items)),
        verified_evidence: presence(Enum.count(evidence_items, &match?(%{status: :verified}, &1)))
      }
    }
  end

  defp summary(nil, _events, evidence_items) do
    "No audit events recorded; #{length(evidence_items)} evidence items attached"
  end

  defp summary(last_event, events, evidence_items) do
    "#{length(events)} audit events, #{length(evidence_items)} evidence items, last event #{Atom.to_string(last_event.event_kind)}"
  end

  defp presence(0), do: :missing
  defp presence(_count), do: :present
end
