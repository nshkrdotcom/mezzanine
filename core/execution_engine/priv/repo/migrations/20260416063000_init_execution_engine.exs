defmodule Mezzanine.Execution.Repo.Migrations.InitExecutionEngine do
  use Ecto.Migration

  def change do
    execute("CREATE EXTENSION IF NOT EXISTS pgcrypto", "DROP EXTENSION IF EXISTS pgcrypto")

    create table(:execution_records, primary_key: false) do
      add :id, :uuid, primary_key: true, default: fragment("gen_random_uuid()")
      add :installation_id, :text, null: false
      add :subject_id, :uuid, null: false
      add :recipe_ref, :text, null: false
      add :trace_id, :text, null: false
      add :causation_id, :text
      add :dispatch_state, :text, null: false
      add :dispatch_attempt_count, :integer, null: false, default: 0
      add :next_dispatch_at, :utc_datetime_usec
      add :submission_ref, :map, null: false, default: %{}
      add :lower_receipt, :map, null: false, default: %{}
      add :last_dispatch_error_kind, :text
      add :last_dispatch_error_payload, :map, null: false, default: %{}
      add :terminal_rejection_reason, :text
      add :row_version, :integer, null: false, default: 1

      timestamps(type: :utc_datetime_usec)
    end

    create index(:execution_records, [:installation_id, :trace_id])
    create index(:execution_records, [:causation_id])
    create index(:execution_records, [:subject_id, :dispatch_state])
    create index(:execution_records, [:next_dispatch_at])

    create table(:dispatch_outbox_entries, primary_key: false) do
      add :id, :uuid, primary_key: true, default: fragment("gen_random_uuid()")
      add :execution_id, :uuid, null: false
      add :installation_id, :text, null: false
      add :subject_id, :uuid, null: false
      add :trace_id, :text, null: false
      add :causation_id, :text
      add :status, :text, null: false
      add :dispatch_envelope, :map, null: false, default: %{}
      add :submission_dedupe_key, :text, null: false
      add :available_at, :utc_datetime_usec, null: false
      add :last_error_kind, :text
      add :last_error_payload, :map, null: false, default: %{}

      timestamps(type: :utc_datetime_usec)
    end

    create unique_index(:dispatch_outbox_entries, [:execution_id])
    create index(:dispatch_outbox_entries, [:status, :available_at])

    create unique_index(:dispatch_outbox_entries, [:installation_id, :submission_dedupe_key],
             name: "dispatch_outbox_entries_unique_submission_dedupe_key"
           )
  end
end
