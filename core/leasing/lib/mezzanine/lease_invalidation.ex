defmodule Mezzanine.LeaseInvalidation do
  @moduledoc """
  Durable truth record for lease invalidation.
  """

  use Ecto.Schema

  import Ecto.Changeset

  @timestamps_opts [type: :utc_datetime_usec]

  schema "lease_invalidations" do
    field(:lease_id, :binary_id)
    field(:lease_kind, :string)
    field(:tenant_id, :string)
    field(:installation_id, :string)
    field(:subject_id, :binary_id)
    field(:execution_id, :binary_id)
    field(:trace_id, :string)
    field(:reason, :string)
    field(:sequence_number, :integer)
    field(:invalidated_at, :utc_datetime_usec)

    timestamps(updated_at: false)
  end

  @type t :: %__MODULE__{
          id: integer() | nil,
          lease_id: Ecto.UUID.t() | nil,
          lease_kind: String.t() | nil,
          tenant_id: String.t() | nil,
          installation_id: String.t() | nil,
          subject_id: Ecto.UUID.t() | nil,
          execution_id: Ecto.UUID.t() | nil,
          trace_id: String.t() | nil,
          reason: String.t() | nil,
          sequence_number: non_neg_integer() | nil,
          invalidated_at: DateTime.t() | nil,
          inserted_at: DateTime.t() | nil
        }

  @required_fields [
    :lease_id,
    :lease_kind,
    :tenant_id,
    :trace_id,
    :reason,
    :sequence_number,
    :invalidated_at
  ]

  @optional_fields [:installation_id, :subject_id, :execution_id]

  @spec changeset(t(), map()) :: Ecto.Changeset.t()
  def changeset(invalidation, attrs) do
    invalidation
    |> cast(attrs, @required_fields ++ @optional_fields)
    |> validate_required(@required_fields)
    |> validate_inclusion(:lease_kind, ["read", "stream"])
    |> unique_constraint(:sequence_number, name: :lease_invalidations_sequence_number_index)
  end
end
