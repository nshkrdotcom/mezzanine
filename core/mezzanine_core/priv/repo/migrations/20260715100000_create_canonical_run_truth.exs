defmodule Mezzanine.Repo.Migrations.CreateCanonicalRunTruth do
  use Ecto.Migration

  def change do
    create table(:mezzanine_run_commands, primary_key: false) do
      add(:command_ref, :text, primary_key: true)
      add(:tenant_ref, :text, null: false)
      add(:installation_ref, :text, null: false)
      add(:idempotency_key, :text, null: false)
      add(:request_hash, :text, null: false)
      add(:run_ref, :text, null: false)
      add(:authority_context_ref, :text, null: false)
      add(:state, :text, null: false)
      add(:acceptance, :map, null: false)
      add(:row_version, :bigint, null: false, default: 1)
      timestamps(type: :utc_datetime_usec)
    end

    create(
      unique_index(
        :mezzanine_run_commands,
        [:tenant_ref, :installation_ref, :idempotency_key],
        name: :mezzanine_run_commands_idempotency
      )
    )

    create(unique_index(:mezzanine_run_commands, [:run_ref]))

    create table(:mezzanine_runs, primary_key: false) do
      add(:run_ref, :text, primary_key: true)
      add(:tenant_ref, :text, null: false)
      add(:installation_ref, :text, null: false)
      add(:actor_ref, :text, null: false)
      add(:subject_ref, :text, null: false)
      add(:trace_ref, :text, null: false)
      add(:correlation_ref, :text, null: false)
      add(:authority_context_ref, :text, null: false)
      add(:runtime_profile_ref, :text, null: false)
      add(:tool_catalog_ref, :text, null: false)
      add(:budget_ref, :text, null: false)
      add(:deadline_at, :utc_datetime_usec)
      add(:status, :text, null: false)
      add(:revision, :bigint, null: false)
      timestamps(type: :utc_datetime_usec)
    end

    create(index(:mezzanine_runs, [:tenant_ref, :installation_ref]))

    create table(:mezzanine_turns, primary_key: false) do
      add(:turn_ref, :text, primary_key: true)
      add(:run_ref, references(:mezzanine_runs, column: :run_ref, type: :text), null: false)
      add(:subject_ref, :text, null: false)
      add(:input_artifact_ref, :text, null: false)
      add(:payload_digest, :text, null: false)
      add(:idempotency_key, :text, null: false)
      add(:sequence, :bigint, null: false)
      add(:row_version, :bigint, null: false)
      timestamps(type: :utc_datetime_usec)
    end

    create(unique_index(:mezzanine_turns, [:run_ref, :sequence]))
    create(unique_index(:mezzanine_turns, [:run_ref, :idempotency_key]))

    create table(:mezzanine_run_events, primary_key: false) do
      add(:event_ref, :text, primary_key: true)
      add(:run_ref, references(:mezzanine_runs, column: :run_ref, type: :text), null: false)
      add(:tenant_ref, :text, null: false)
      add(:event_type, :text, null: false)
      add(:event_version, :integer, null: false)
      add(:sequence, :bigint, null: false)
      add(:command_ref, :text, null: false)
      add(:causation_ref, :text)
      add(:correlation_ref, :text, null: false)
      add(:payload_ref, :text, null: false)
      add(:payload_digest, :text, null: false)
      add(:recorded_at, :utc_datetime_usec, null: false)
      add(:row_version, :bigint, null: false)
    end

    create(unique_index(:mezzanine_run_events, [:run_ref, :sequence]))
    create(index(:mezzanine_run_events, [:tenant_ref, :recorded_at]))

    create table(:mezzanine_run_projections, primary_key: false) do
      add(:run_ref, references(:mezzanine_runs, column: :run_ref, type: :text), primary_key: true)
      add(:tenant_ref, :text, null: false)
      add(:subject_ref, :text, null: false)
      add(:latest_turn_ref, :text, null: false)
      add(:latest_event_ref, :text, null: false)
      add(:status, :text, null: false)
      add(:event_sequence, :bigint, null: false)
      add(:run_revision, :bigint, null: false)
      add(:projection, :map, null: false)
      timestamps(type: :utc_datetime_usec)
    end

    create(index(:mezzanine_run_projections, [:tenant_ref, :status]))

    create table(:mezzanine_run_cursors, primary_key: false) do
      add(:run_ref, references(:mezzanine_runs, column: :run_ref, type: :text), primary_key: true)
      add(:last_event_ref, :text, null: false)
      add(:sequence, :bigint, null: false)
      add(:row_version, :bigint, null: false)
      timestamps(type: :utc_datetime_usec)
    end

    create table(:mezzanine_workflow_outbox, primary_key: false) do
      add(:outbox_ref, :text, primary_key: true)
      add(:event_ref, :text, null: false)
      add(:run_ref, references(:mezzanine_runs, column: :run_ref, type: :text), null: false)
      add(:workflow_ref, :text, null: false)
      add(:workflow_type, :text, null: false)
      add(:temporal_namespace, :text, null: false)
      add(:task_queue, :text, null: false)
      add(:idempotency_key, :text, null: false)
      add(:state, :text, null: false)
      add(:attempt, :integer, null: false, default: 0)
      add(:last_error_ref, :text)
      add(:available_at, :utc_datetime_usec, null: false)
      add(:lock_owner, :text)
      add(:lock_expires_at, :utc_datetime_usec)
      add(:row_version, :bigint, null: false, default: 1)
      timestamps(type: :utc_datetime_usec)
    end

    create(unique_index(:mezzanine_workflow_outbox, [:run_ref, :idempotency_key]))
    create(index(:mezzanine_workflow_outbox, [:state, :available_at]))

    create(
      constraint(:mezzanine_workflow_outbox, :mezzanine_workflow_outbox_state,
        check: "state IN ('pending', 'dispatched', 'acknowledged', 'ambiguous', 'failed')"
      )
    )
  end
end
