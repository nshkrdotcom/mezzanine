defmodule Mezzanine.Surfaces.WorkQueueStats do
  @moduledoc """
  Queue and activity summary for a program's governed work.
  """

  @enforce_keys [:program_id, :active_count, :counts_by_status]
  defstruct [
    :program_id,
    :active_count,
    :queued_count,
    :running_count,
    :awaiting_review_count,
    :blocked_count,
    :stalled_count,
    :open_escalation_count,
    :counts_by_status
  ]

  @type t :: %__MODULE__{
          program_id: Ecto.UUID.t(),
          active_count: non_neg_integer(),
          queued_count: non_neg_integer(),
          running_count: non_neg_integer(),
          awaiting_review_count: non_neg_integer(),
          blocked_count: non_neg_integer(),
          stalled_count: non_neg_integer(),
          open_escalation_count: non_neg_integer(),
          counts_by_status: %{optional(atom()) => non_neg_integer()}
        }
end
