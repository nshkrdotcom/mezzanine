defmodule Mezzanine.Surfaces.ReviewDetail do
  @moduledoc """
  Rich review projection spanning work, run, evidence, and audit context.
  """

  @enforce_keys [:review_unit]
  defstruct [
    :review_unit,
    :work_object,
    :run,
    :evidence_bundle,
    :evidence_items,
    :run_artifacts,
    :audit_timeline,
    :gate_status,
    :decisions,
    :waivers,
    :escalations
  ]

  @type t :: %__MODULE__{
          review_unit: struct(),
          work_object: struct() | nil,
          run: struct() | nil,
          evidence_bundle: struct() | nil,
          evidence_items: [struct()],
          run_artifacts: [struct()],
          audit_timeline: struct() | nil,
          gate_status: map() | nil,
          decisions: [struct()],
          waivers: [struct()],
          escalations: [struct()]
        }
end
