defmodule Mezzanine.Objects.Repo.Migrations.InitObjectEngine do
  use Ecto.Migration

  def change do
    execute("CREATE EXTENSION IF NOT EXISTS pgcrypto", "DROP EXTENSION IF EXISTS pgcrypto")

    create table(:subject_records, primary_key: false) do
      add :id, :uuid, primary_key: true, default: fragment("gen_random_uuid()")
      add :installation_id, :text, null: false
      add :source_ref, :text
      add :subject_kind, :text, null: false
      add :lifecycle_state, :text, null: false
      add :title, :text
      add :description, :text
      add :schema_ref, :text
      add :schema_version, :integer, null: false, default: 1
      add :payload, :map, null: false, default: %{}
      add :opened_at, :utc_datetime_usec
      add :blocked_at, :utc_datetime_usec
      add :block_reason, :text
      add :row_version, :integer, null: false, default: 1

      timestamps(type: :utc_datetime_usec)
    end

    create unique_index(:subject_records, [:installation_id, :source_ref],
             name: "subject_records_unique_installation_source_ref"
           )

    create index(:subject_records, [:installation_id, :lifecycle_state])
    create index(:subject_records, [:installation_id, :subject_kind])
  end
end
