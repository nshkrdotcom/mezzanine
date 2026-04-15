defmodule Mezzanine.Surfaces.RunDetail do
  @moduledoc """
  Rich operator projection of a run and its surrounding work context.
  """

  @enforce_keys [:run]
  defstruct [
    :run,
    :run_series,
    :work_object,
    :review_units,
    :run_artifacts,
    :evidence_bundles,
    :timeline,
    :audit_events
  ]

  @type t :: %__MODULE__{
          run: struct(),
          run_series: struct() | nil,
          work_object: struct() | nil,
          review_units: [struct()],
          run_artifacts: [struct()],
          evidence_bundles: [struct()],
          timeline: [struct()],
          audit_events: [struct()]
        }
end
