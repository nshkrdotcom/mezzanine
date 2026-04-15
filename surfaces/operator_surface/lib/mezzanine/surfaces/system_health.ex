defmodule Mezzanine.Surfaces.SystemHealth do
  @moduledoc """
  Program-level operational health summary.
  """

  @enforce_keys [:program_id]
  defstruct [
    :program_id,
    :queue_stats,
    :pending_review_count,
    :open_control_session_count,
    :active_run_count,
    :stalled_run_count,
    :open_escalation_count
  ]

  @type t :: %__MODULE__{
          program_id: Ecto.UUID.t(),
          queue_stats: struct(),
          pending_review_count: non_neg_integer(),
          open_control_session_count: non_neg_integer(),
          active_run_count: non_neg_integer(),
          stalled_run_count: non_neg_integer(),
          open_escalation_count: non_neg_integer()
        }
end
