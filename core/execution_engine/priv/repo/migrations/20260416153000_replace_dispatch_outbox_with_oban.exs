defmodule Mezzanine.Execution.Repo.Migrations.ReplaceDispatchOutboxWithOban do
  use Ecto.Migration

  def up do
    alter table(:execution_records) do
      add :dispatch_envelope, :map, null: false, default: %{}
      add :submission_dedupe_key, :text, null: false, default: ""
    end

    create unique_index(:execution_records, [:installation_id, :submission_dedupe_key],
             name: "execution_records_unique_submission_dedupe_key"
           )

    Oban.Migrations.up(version: 12)

    drop table(:dispatch_outbox_entries)
  end

  def down do
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
      add :compiled_pack_revision, :integer, null: false, default: 1
      add :binding_snapshot, :map, null: false, default: %{}

      timestamps(type: :utc_datetime_usec)
    end

    create unique_index(:dispatch_outbox_entries, [:execution_id])
    create index(:dispatch_outbox_entries, [:status, :available_at])

    create unique_index(:dispatch_outbox_entries, [:installation_id, :submission_dedupe_key],
             name: "dispatch_outbox_entries_unique_submission_dedupe_key"
           )

    Oban.Migrations.down(version: 12)

    drop_if_exists(index(:execution_records, [:installation_id, :submission_dedupe_key],
                     name: "execution_records_unique_submission_dedupe_key"
                   ))

    alter table(:execution_records) do
      remove :submission_dedupe_key
      remove :dispatch_envelope
    end
  end
end
