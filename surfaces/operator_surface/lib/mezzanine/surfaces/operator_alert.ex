defmodule Mezzanine.Surfaces.OperatorAlert do
  @moduledoc """
  Operator-facing alert for work requiring intervention or attention.
  """

  @enforce_keys [:work_object_id, :alert_kind, :severity, :message, :raised_at]
  defstruct [:work_object_id, :alert_kind, :severity, :message, :raised_at, :work_title]

  @type t :: %__MODULE__{
          work_object_id: Ecto.UUID.t(),
          alert_kind: atom(),
          severity: :info | :warning | :critical,
          message: String.t(),
          raised_at: DateTime.t(),
          work_title: String.t() | nil
        }
end
