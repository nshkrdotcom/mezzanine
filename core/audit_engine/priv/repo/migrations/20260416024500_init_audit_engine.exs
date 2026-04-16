defmodule Mezzanine.Audit.Repo.Migrations.InitAuditEngine do
  use Ecto.Migration

  def up do
    execute("CREATE EXTENSION IF NOT EXISTS pgcrypto", "DROP EXTENSION IF EXISTS pgcrypto")

    create table(:audit_facts, primary_key: false) do
      add :id, :uuid, primary_key: true, default: fragment("gen_random_uuid()")
      add :installation_id, :text, null: false
      add :subject_id, :text
      add :execution_id, :text
      add :decision_id, :text
      add :evidence_id, :text
      add :trace_id, :text, null: false
      add :causation_id, :text
      add :fact_kind, :text, null: false
      add :actor_ref, :map, null: false, default: %{}
      add :payload, :map, null: false, default: %{}
      add :occurred_at, :utc_datetime_usec, null: false

      timestamps(type: :utc_datetime_usec)
    end

    create index(:audit_facts, [:installation_id, :trace_id, :occurred_at])
    create index(:audit_facts, [:causation_id])

    create table(:execution_lineage_records, primary_key: false) do
      add :id, :uuid, primary_key: true, default: fragment("gen_random_uuid()")
      add :trace_id, :text, null: false
      add :causation_id, :text
      add :installation_id, :text, null: false
      add :subject_id, :text, null: false
      add :execution_id, :text, null: false
      add :dispatch_outbox_entry_id, :text
      add :citadel_request_id, :text
      add :citadel_submission_id, :text
      add :ji_submission_key, :text
      add :lower_run_id, :text
      add :lower_attempt_id, :text
      add :artifact_refs, {:array, :text}, null: false, default: []

      timestamps(type: :utc_datetime_usec)
    end

    create unique_index(:execution_lineage_records, [:execution_id])
    create index(:execution_lineage_records, [:installation_id, :trace_id])
  end

  def down do
    drop table(:execution_lineage_records)
    drop table(:audit_facts)
    execute("DROP EXTENSION IF EXISTS pgcrypto")
  end
end
