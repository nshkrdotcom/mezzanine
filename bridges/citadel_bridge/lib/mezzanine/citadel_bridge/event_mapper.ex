defmodule Mezzanine.CitadelBridge.EventMapper do
  @moduledoc """
  Maps lower Citadel or Spine lifecycle events back into Mezzanine audit attrs.
  """

  @spec to_audit_attrs(map(), map()) :: map()
  def to_audit_attrs(event, attrs \\ %{}) when is_map(event) and is_map(attrs) do
    %{
      program_id: Map.get(attrs, :program_id) || Map.get(attrs, "program_id"),
      work_object_id:
        Map.get(attrs, :work_object_id) || Map.get(attrs, "work_object_id") ||
          value(event, :work_object_id, nil),
      run_id: value(event, :run_id, nil),
      review_unit_id: value(event, :review_unit_id, nil),
      event_kind: event_kind(event),
      actor_kind: value(event, :actor_kind, :system),
      actor_ref: value(event, :actor_ref, "citadel"),
      payload: payload(event),
      occurred_at: value(event, :occurred_at, DateTime.utc_now())
    }
  end

  defp event_kind(event) do
    case value(event, :status, value(event, :event_kind, :observed)) do
      status when status in [:accepted, "accepted"] -> :run_accepted
      status when status in [:started, "started"] -> :run_started
      status when status in [:completed, "completed"] -> :run_completed
      status when status in [:failed, "failed"] -> :run_failed
      status when status in [:rejected, "rejected"] -> :run_rejected
      _ -> :run_observed
    end
  end

  defp payload(event) do
    Map.drop(event, [:status, "status", :event_kind, "event_kind", :run_id, "run_id"])
  end

  defp value(map, key, default) do
    Map.get(map, key) || Map.get(map, Atom.to_string(key)) || default
  end
end
