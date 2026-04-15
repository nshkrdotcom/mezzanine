defmodule Mezzanine.Surfaces.WorkStatusProjection do
  @moduledoc """
  Human-friendly synthesized status for a single governed work object.
  """

  @enforce_keys [:work_object_id, :work_status]
  defstruct [
    :work_object_id,
    :work_status,
    :plan_status,
    :run_status,
    :control_mode,
    :review_status,
    :release_ready?,
    :last_event_at
  ]

  @type t :: %__MODULE__{
          work_object_id: Ecto.UUID.t(),
          work_status: atom(),
          plan_status: atom() | nil,
          run_status: atom() | nil,
          control_mode: atom() | nil,
          review_status: atom() | nil,
          release_ready?: boolean(),
          last_event_at: DateTime.t() | nil
        }
end
