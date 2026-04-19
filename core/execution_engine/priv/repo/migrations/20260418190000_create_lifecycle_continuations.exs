defmodule Mezzanine.Execution.Repo.Migrations.CreateLifecycleContinuations do
  use Ecto.Migration

  def change do
    create table(:lifecycle_continuations, primary_key: false) do
      add :continuation_id, :text, primary_key: true
      add :tenant_id, :text, null: false
      add :installation_id, :text, null: false
      add :subject_id, :uuid, null: false
      add :execution_id, :uuid, null: false
      add :from_state, :text, null: false
      add :target_transition, :text, null: false
      add :attempt_count, :integer, null: false, default: 0
      add :next_attempt_at, :utc_datetime_usec, null: false
      add :last_error_class, :text
      add :last_error_message, :text
      add :trace_id, :text, null: false
      add :status, :text, null: false, default: "pending"
      add :actor_ref, :map, null: false, default: %{}
      add :metadata, :map, null: false, default: %{}

      timestamps(type: :utc_datetime_usec)
    end

    create index(:lifecycle_continuations, [:tenant_id, :installation_id, :status])
    create index(:lifecycle_continuations, [:subject_id])
    create index(:lifecycle_continuations, [:execution_id])
    create index(:lifecycle_continuations, [:trace_id])
    create index(:lifecycle_continuations, [:next_attempt_at])

    create constraint(:lifecycle_continuations, :lifecycle_continuations_status_check,
             check:
               "status IN ('pending', 'running', 'retry_scheduled', 'dead_lettered', 'completed')"
           )

    create constraint(:lifecycle_continuations, :lifecycle_continuations_attempt_count_check,
             check: "attempt_count >= 0"
           )
  end
end
