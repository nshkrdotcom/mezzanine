defmodule Mezzanine.Decisions.Repo.Migrations.InitDecisionEngine do
  use Ecto.Migration

  def change do
    execute("CREATE EXTENSION IF NOT EXISTS pgcrypto", "DROP EXTENSION IF EXISTS pgcrypto")

    create table(:decision_records, primary_key: false) do
      add :id, :uuid, primary_key: true, default: fragment("gen_random_uuid()")
      add :installation_id, :text, null: false
      add :subject_id, :uuid, null: false
      add :execution_id, :uuid
      add :decision_kind, :text, null: false
      add :lifecycle_state, :text, null: false
      add :decision_value, :text
      add :required_by, :utc_datetime_usec
      add :resolved_at, :utc_datetime_usec
      add :reason, :text
      add :trace_id, :text, null: false
      add :causation_id, :text
      add :row_version, :integer, null: false, default: 1

      timestamps(type: :utc_datetime_usec)
    end

    create index(:decision_records, [:installation_id, :trace_id])
    create index(:decision_records, [:causation_id])
    create index(:decision_records, [:subject_id, :lifecycle_state])
    create index(:decision_records, [:installation_id, :required_by])

    create unique_index(
             :decision_records,
             [:installation_id, :subject_id, :decision_kind, :execution_id],
             name: "decision_records_unique_subject_decision_execution"
           )
  end
end
