defmodule Mezzanine.StreamAttachLease do
  @moduledoc """
  Durable lease granting scoped lower stream attachment.
  """

  use Ecto.Schema

  import Ecto.Changeset

  @primary_key {:lease_id, :binary_id, autogenerate: false}
  @timestamps_opts [type: :utc_datetime_usec]

  schema "stream_attach_leases" do
    field(:trace_id, :string)
    field(:tenant_id, :string)
    field(:installation_id, :string)
    field(:subject_id, :binary_id)
    field(:execution_id, :binary_id)
    field(:lineage_anchor, :map, default: %{})
    field(:allowed_family, :string)
    field(:scope, :map, default: %{})
    field(:attach_token_digest, :string)
    field(:attach_token, :string, virtual: true)
    field(:expires_at, :utc_datetime_usec)
    field(:issued_invalidation_cursor, :integer)
    field(:last_invalidation_cursor, :integer)
    field(:invalidation_channel, :string)

    timestamps()
  end

  @type t :: %__MODULE__{
          lease_id: Ecto.UUID.t() | nil,
          trace_id: String.t() | nil,
          tenant_id: String.t() | nil,
          installation_id: String.t() | nil,
          subject_id: Ecto.UUID.t() | nil,
          execution_id: Ecto.UUID.t() | nil,
          lineage_anchor: map(),
          allowed_family: String.t() | nil,
          scope: map() | nil,
          attach_token_digest: String.t() | nil,
          attach_token: String.t() | nil,
          expires_at: DateTime.t() | nil,
          issued_invalidation_cursor: non_neg_integer() | nil,
          last_invalidation_cursor: non_neg_integer() | nil,
          invalidation_channel: String.t() | nil,
          inserted_at: DateTime.t() | nil,
          updated_at: DateTime.t() | nil
        }

  @required_fields [
    :lease_id,
    :trace_id,
    :tenant_id,
    :allowed_family,
    :attach_token_digest,
    :expires_at,
    :issued_invalidation_cursor,
    :last_invalidation_cursor,
    :invalidation_channel
  ]

  @optional_fields [
    :installation_id,
    :subject_id,
    :execution_id,
    :lineage_anchor,
    :scope,
    :attach_token
  ]

  @spec changeset(t(), map()) :: Ecto.Changeset.t()
  def changeset(lease, attrs) do
    lease
    |> cast(attrs, @required_fields ++ @optional_fields)
    |> validate_required(@required_fields)
    |> validate_change(:lineage_anchor, &validate_map/2)
    |> validate_change(:scope, &validate_map/2)
    |> validate_subject_or_execution_presence()
    |> unique_constraint(:lease_id, name: :stream_attach_leases_pkey)
  end

  defp validate_subject_or_execution_presence(changeset) do
    subject_id = get_field(changeset, :subject_id)
    execution_id = get_field(changeset, :execution_id)

    if is_nil(subject_id) and is_nil(execution_id) do
      add_error(changeset, :subject_id, "subject_id or execution_id is required")
    else
      changeset
    end
  end

  defp validate_map(_field, value) when is_map(value), do: []
  defp validate_map(field, _value), do: [{field, "must be a map"}]
end
