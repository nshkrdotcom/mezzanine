defmodule Mezzanine.Projections.MaterializedProjection do
  @moduledoc """
  Durable async materialization for non-interactive named projections.
  """

  use Ash.Resource,
    domain: Mezzanine.Projections,
    data_layer: AshPostgres.DataLayer

  postgres do
    table("materialized_projections")
    repo(Mezzanine.Projections.Repo)

    custom_indexes do
      index([:installation_id, :projection_name], unique: true)
      index([:installation_id, :computed_at])
    end
  end

  code_interface do
    define(:upsert, action: :upsert)

    define(:by_installation_and_name,
      action: :by_installation_and_name,
      args: [:installation_id, :projection_name]
    )

    define(:list_for_installation, action: :list_for_installation, args: [:installation_id])
  end

  actions do
    defaults([:read])

    create :upsert do
      accept([
        :installation_id,
        :projection_name,
        :payload,
        :computed_at
      ])

      upsert?(true)
      upsert_identity(:unique_projection)
      upsert_fields([:payload, :computed_at])

      change(&ensure_computed_at/2)
    end

    read :by_installation_and_name do
      argument(:installation_id, :string, allow_nil?: false)
      argument(:projection_name, :string, allow_nil?: false)

      get?(true)

      filter(
        expr(
          installation_id == ^arg(:installation_id) and projection_name == ^arg(:projection_name)
        )
      )
    end

    read :list_for_installation do
      argument(:installation_id, :string, allow_nil?: false)
      filter(expr(installation_id == ^arg(:installation_id)))
      prepare(build(sort: [projection_name: :asc]))
    end
  end

  attributes do
    uuid_primary_key(:id)

    attribute :installation_id, :string do
      allow_nil?(false)
      public?(true)
    end

    attribute :projection_name, :string do
      allow_nil?(false)
      public?(true)
    end

    attribute :payload, :map do
      allow_nil?(false)
      default(%{})
      public?(true)
    end

    attribute :computed_at, :utc_datetime_usec do
      allow_nil?(false)
      public?(true)
    end

    timestamps()
  end

  identities do
    identity(:unique_projection, [:installation_id, :projection_name])
  end

  defp ensure_computed_at(changeset, _context) do
    if is_nil(Ash.Changeset.get_attribute(changeset, :computed_at)) do
      Ash.Changeset.change_attribute(changeset, :computed_at, DateTime.utc_now())
    else
      changeset
    end
  end
end
