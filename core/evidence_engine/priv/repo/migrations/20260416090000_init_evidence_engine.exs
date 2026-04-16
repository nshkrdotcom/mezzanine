defmodule Mezzanine.EvidenceLedger.Repo.Migrations.InitEvidenceEngine do
  use Ecto.Migration

  def change do
    execute("CREATE EXTENSION IF NOT EXISTS pgcrypto", "DROP EXTENSION IF EXISTS pgcrypto")

    create table(:evidence_records, primary_key: false) do
      add :id, :uuid, primary_key: true, default: fragment("gen_random_uuid()")
      add :installation_id, :text, null: false
      add :subject_id, :uuid, null: false
      add :execution_id, :uuid
      add :evidence_kind, :text, null: false
      add :collector_ref, :text
      add :content_ref, :text
      add :status, :text, null: false
      add :metadata, :map, null: false, default: %{}
      add :collected_at, :utc_datetime_usec
      add :verified_at, :utc_datetime_usec
      add :trace_id, :text, null: false
      add :causation_id, :text
      add :row_version, :integer, null: false, default: 1

      timestamps(type: :utc_datetime_usec)
    end

    create index(:evidence_records, [:installation_id, :trace_id])
    create index(:evidence_records, [:causation_id])
    create index(:evidence_records, [:subject_id, :status])
    create index(:evidence_records, [:execution_id, :status])

    create unique_index(
             :evidence_records,
             [:installation_id, :subject_id, :execution_id, :evidence_kind],
             name: "evidence_records_unique_subject_execution_kind"
           )
  end
end
