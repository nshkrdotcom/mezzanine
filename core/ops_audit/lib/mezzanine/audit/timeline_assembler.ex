defmodule Mezzanine.Audit.TimelineAssembler do
  @moduledoc """
  Pure assembly of ordered timeline rows from audit events.
  """

  @enforce_keys [:event_id, :event_kind, :occurred_at, :payload]
  defstruct [
    :event_id,
    :event_kind,
    :occurred_at,
    :payload,
    :actor_kind,
    :actor_ref,
    :run_id,
    :review_unit_id
  ]

  @type row :: %__MODULE__{
          event_id: String.t(),
          event_kind: String.t(),
          occurred_at: DateTime.t(),
          payload: map(),
          actor_kind: String.t() | nil,
          actor_ref: String.t() | nil,
          run_id: String.t() | nil,
          review_unit_id: String.t() | nil
        }

  @spec project([struct()]) :: [row()]
  def project(events) when is_list(events) do
    events
    |> Enum.sort_by(&{&1.occurred_at, &1.id})
    |> Enum.map(fn event ->
      %__MODULE__{
        event_id: event.id,
        event_kind: Atom.to_string(event.event_kind),
        occurred_at: event.occurred_at,
        payload: event.payload,
        actor_kind: maybe_stringify(event.actor_kind),
        actor_ref: event.actor_ref,
        run_id: event.run_id,
        review_unit_id: event.review_unit_id
      }
    end)
  end

  defp maybe_stringify(nil), do: nil
  defp maybe_stringify(value) when is_atom(value), do: Atom.to_string(value)
  defp maybe_stringify(value), do: to_string(value)
end
