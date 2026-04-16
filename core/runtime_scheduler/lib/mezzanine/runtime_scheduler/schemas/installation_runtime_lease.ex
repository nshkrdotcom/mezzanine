defmodule Mezzanine.RuntimeScheduler.Schemas.InstallationRuntimeLease do
  @moduledoc false

  use Ecto.Schema

  import Ecto.Changeset

  @primary_key false
  schema "installation_runtime_leases" do
    field(:installation_id, :string, primary_key: true)
    field(:holder, :string)
    field(:lease_id, :string)
    field(:epoch, :integer)
    field(:compiled_pack_revision, :integer)
    field(:expires_at, :utc_datetime_usec)

    timestamps(type: :utc_datetime_usec)
  end

  @type t :: %__MODULE__{}

  @spec changeset(__MODULE__.t(), map()) :: Ecto.Changeset.t()
  def changeset(schema, attrs) do
    schema
    |> cast(attrs, [
      :installation_id,
      :holder,
      :lease_id,
      :epoch,
      :compiled_pack_revision,
      :expires_at
    ])
    |> validate_required([
      :installation_id,
      :holder,
      :lease_id,
      :epoch,
      :compiled_pack_revision,
      :expires_at
    ])
    |> validate_number(:epoch, greater_than_or_equal_to: 0)
    |> validate_number(:compiled_pack_revision, greater_than: 0)
    |> unique_constraint(:installation_id)
  end
end
