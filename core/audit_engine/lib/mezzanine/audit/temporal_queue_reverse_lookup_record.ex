defmodule Mezzanine.Audit.TemporalQueueReverseLookupRecord do
  @moduledoc """
  Durable reverse lookup from Temporal queue hash segments to typed refs.
  """

  use Ecto.Schema

  import Ecto.Changeset

  @primary_key {:id, :binary_id, autogenerate: true}
  @type t :: %__MODULE__{
          id: Ecto.UUID.t() | nil,
          hash_segment: String.t() | nil,
          typed_ref: String.t() | nil,
          ref_kind: String.t() | nil,
          queue: String.t() | nil,
          inserted_at: DateTime.t() | nil,
          updated_at: DateTime.t() | nil
        }

  schema "temporal_queue_reverse_lookups" do
    field(:hash_segment, :string)
    field(:typed_ref, :string)
    field(:ref_kind, :string)
    field(:queue, :string)

    timestamps(type: :utc_datetime_usec)
  end

  @doc false
  @spec changeset(%__MODULE__{}, map()) :: Ecto.Changeset.t()
  def changeset(record, attrs) do
    record
    |> cast(attrs, [:hash_segment, :typed_ref, :ref_kind, :queue])
    |> validate_required([:hash_segment, :typed_ref, :ref_kind, :queue])
    |> validate_format(:hash_segment, ~r/\A[a-z2-7]{20}\z/)
    |> unique_constraint(:hash_segment)
  end
end
