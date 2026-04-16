defmodule Mezzanine.Projections.ProjectionRow do
  @moduledoc """
  Durable named projection row with indexed lineage joins for substrate read models.
  """

  use Ash.Resource,
    domain: Mezzanine.Projections,
    data_layer: AshPostgres.DataLayer

  postgres do
    table("projection_rows")
    repo(Mezzanine.Projections.Repo)

    custom_indexes do
      index([:installation_id, :projection_name, :sort_key])
      index([:installation_id, :trace_id])
      index([:causation_id])
      index([:subject_id])
      index([:installation_id, :projection_name, :row_key], unique: true)
    end
  end

  code_interface do
    define(:upsert, action: :upsert)

    define(:rows_for_projection,
      action: :rows_for_projection,
      args: [:installation_id, :projection_name]
    )

    define(:row_by_key, action: :row_by_key, args: [:installation_id, :projection_name, :row_key])
    define(:rows_for_trace, action: :rows_for_trace, args: [:installation_id, :trace_id])
  end

  actions do
    defaults([:read])

    create :upsert do
      accept([
        :installation_id,
        :projection_name,
        :row_key,
        :subject_id,
        :execution_id,
        :projection_kind,
        :sort_key,
        :trace_id,
        :causation_id,
        :payload,
        :computed_at
      ])

      upsert?(true)
      upsert_identity(:unique_projection_row)

      upsert_fields([
        :subject_id,
        :execution_id,
        :projection_kind,
        :sort_key,
        :trace_id,
        :causation_id,
        :payload,
        :computed_at
      ])

      change(&ensure_computed_at/2)
    end

    read :rows_for_projection do
      argument(:installation_id, :string, allow_nil?: false)
      argument(:projection_name, :string, allow_nil?: false)

      filter(
        expr(
          installation_id == ^arg(:installation_id) and projection_name == ^arg(:projection_name)
        )
      )

      prepare(build(sort: [sort_key: :asc, updated_at: :desc, inserted_at: :desc]))
    end

    read :row_by_key do
      argument(:installation_id, :string, allow_nil?: false)
      argument(:projection_name, :string, allow_nil?: false)
      argument(:row_key, :string, allow_nil?: false)

      get?(true)

      filter(
        expr(
          installation_id == ^arg(:installation_id) and projection_name == ^arg(:projection_name) and
            row_key == ^arg(:row_key)
        )
      )
    end

    read :rows_for_trace do
      argument(:installation_id, :string, allow_nil?: false)
      argument(:trace_id, :string, allow_nil?: false)

      filter(expr(installation_id == ^arg(:installation_id) and trace_id == ^arg(:trace_id)))
      prepare(build(sort: [projection_name: :asc, sort_key: :asc, updated_at: :desc]))
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

    attribute :row_key, :string do
      allow_nil?(false)
      public?(true)
    end

    attribute :subject_id, :uuid do
      public?(true)
    end

    attribute :execution_id, :uuid do
      public?(true)
    end

    attribute :projection_kind, :string do
      allow_nil?(false)
      default("queue")
      public?(true)
    end

    attribute :sort_key, :integer do
      allow_nil?(false)
      default(0)
      public?(true)
    end

    attribute :trace_id, :string do
      allow_nil?(false)
      public?(true)
    end

    attribute :causation_id, :string do
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
    identity(:unique_projection_row, [:installation_id, :projection_name, :row_key])
  end

  defp ensure_computed_at(changeset, _context) do
    if is_nil(Ash.Changeset.get_attribute(changeset, :computed_at)) do
      Ash.Changeset.change_attribute(changeset, :computed_at, DateTime.utc_now())
    else
      changeset
    end
  end
end
