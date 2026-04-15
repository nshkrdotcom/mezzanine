defmodule Mezzanine.Surfaces.ReviewSummary do
  @moduledoc """
  Lightweight review queue row for operator dashboards.
  """

  @enforce_keys [:review_unit_id, :work_object_id, :status]
  defstruct [
    :review_unit_id,
    :work_object_id,
    :status,
    :review_kind,
    :required_by,
    :reviewer_actor,
    :work_title
  ]

  @type t :: %__MODULE__{
          review_unit_id: Ecto.UUID.t(),
          work_object_id: Ecto.UUID.t(),
          status: atom(),
          review_kind: atom(),
          required_by: DateTime.t() | nil,
          reviewer_actor: map(),
          work_title: String.t() | nil
        }
end
