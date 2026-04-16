defmodule Mezzanine.Projections.Repo.Migrations.InitProjectionEngine do
  use Ecto.Migration

  def change do
    create table(:materialized_projections, primary_key: false) do
      add(:id, :uuid, primary_key: true, default: fragment("gen_random_uuid()"))
      add(:installation_id, :text, null: false)
      add(:projection_name, :text, null: false)
      add(:payload, :map, null: false, default: %{})
      add(:computed_at, :utc_datetime_usec, null: false)

      timestamps(type: :utc_datetime_usec)
    end

    create(
      unique_index(:materialized_projections, [:installation_id, :projection_name],
        name: "materialized_projections_unique_installation_projection_index"
      )
    )

    create(index(:materialized_projections, [:installation_id, :computed_at]))

    create table(:projection_rows, primary_key: false) do
      add(:id, :uuid, primary_key: true, default: fragment("gen_random_uuid()"))
      add(:installation_id, :text, null: false)
      add(:projection_name, :text, null: false)
      add(:row_key, :text, null: false)
      add(:subject_id, :uuid)
      add(:execution_id, :uuid)
      add(:projection_kind, :text, null: false, default: "queue")
      add(:sort_key, :bigint, null: false, default: 0)
      add(:trace_id, :text, null: false)
      add(:causation_id, :text)
      add(:payload, :map, null: false, default: %{})
      add(:computed_at, :utc_datetime_usec, null: false)

      timestamps(type: :utc_datetime_usec)
    end

    create(
      unique_index(:projection_rows, [:installation_id, :projection_name, :row_key],
        name: "projection_rows_unique_installation_projection_row_key_index"
      )
    )

    create(index(:projection_rows, [:installation_id, :projection_name, :sort_key]))
    create(index(:projection_rows, [:installation_id, :trace_id]))
    create(index(:projection_rows, [:causation_id]))
    create(index(:projection_rows, [:subject_id]))
  end
end
