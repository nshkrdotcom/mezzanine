defmodule Mezzanine.Memory.SharedScopeRegistryEntry do
  @moduledoc false

  use Ecto.Schema

  import Ecto.Changeset

  @primary_key {:id, :binary_id, autogenerate: true}
  @foreign_key_type :binary_id

  schema "shared_scope_registry_entries" do
    field(:tenant_ref, :string)
    field(:scope_ref, :string)
    field(:governance_ref, :map, default: %{})
    field(:activation_epoch, :integer)
    field(:deregistration_epoch, :integer)
    field(:deregistration_governance_ref, :map)
    field(:source_node_ref, :string)
    field(:commit_lsn, :string)
    field(:commit_hlc, :map, default: %{})
    field(:invalidation_topic, :string)

    timestamps(type: :utc_datetime_usec)
  end

  @required [
    :tenant_ref,
    :scope_ref,
    :governance_ref,
    :activation_epoch,
    :source_node_ref,
    :commit_lsn,
    :commit_hlc,
    :invalidation_topic
  ]

  @optional [:deregistration_epoch, :deregistration_governance_ref]

  def changeset(entry, attrs) when is_map(attrs) do
    entry
    |> cast(attrs, @required ++ @optional)
    |> validate_required(@required)
    |> validate_number(:activation_epoch, greater_than: 0)
    |> validate_number(:deregistration_epoch, greater_than: 0)
    |> validate_non_empty_string(:tenant_ref)
    |> validate_non_empty_string(:scope_ref)
    |> validate_non_empty_string(:source_node_ref)
    |> validate_non_empty_string(:commit_lsn)
    |> validate_non_empty_string(:invalidation_topic)
    |> validate_change(:deregistration_epoch, fn :deregistration_epoch, value ->
      activation = get_field(entry, :activation_epoch) || get_change(entry, :activation_epoch)

      if is_integer(activation) and value <= activation do
        [deregistration_epoch: "must be greater than activation_epoch"]
      else
        []
      end
    end)
  end

  defp validate_non_empty_string(changeset, field) do
    validate_change(changeset, field, fn ^field, value ->
      if is_binary(value) and String.trim(value) != "" do
        []
      else
        [{field, "must be a non-empty string"}]
      end
    end)
  end
end
