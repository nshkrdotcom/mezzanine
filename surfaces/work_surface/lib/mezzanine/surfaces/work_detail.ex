defmodule Mezzanine.Surfaces.WorkDetail do
  @moduledoc """
  Rich work projection assembled for product and operator surfaces.
  """

  @enforce_keys [:work_object]
  defstruct [
    :work_object,
    :current_plan,
    :active_run,
    :run_series,
    :obligations,
    :pending_reviews,
    :evidence_bundle,
    :control_session,
    :timeline_projection,
    :gate_status
  ]

  @type t :: %__MODULE__{
          work_object: struct(),
          current_plan: struct() | nil,
          active_run: struct() | nil,
          run_series: [struct()],
          obligations: [String.t()],
          pending_reviews: [struct()],
          evidence_bundle: struct() | nil,
          control_session: struct() | nil,
          timeline_projection: struct() | nil,
          gate_status: map() | nil
        }
end
