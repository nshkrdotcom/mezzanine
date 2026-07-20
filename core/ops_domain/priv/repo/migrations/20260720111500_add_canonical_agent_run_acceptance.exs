defmodule Mezzanine.OpsDomain.Repo.Migrations.AddCanonicalAgentRunAcceptance do
  use Ecto.Migration

  def up do
    alter table(:runs) do
      add(:external_ref, :text)
      add(:row_version, :bigint, null: false, default: 1)
    end

    create(unique_index(:runs, [:external_ref], name: :runs_unique_external_ref_index))

    create table(:agent_run_commands, primary_key: false) do
      add(:command_ref, :text, primary_key: true)
      add(:tenant_id, :text, null: false)
      add(:installation_ref, :text, null: false)
      add(:idempotency_key, :text, null: false)
      add(:request_hash, :text, null: false)
      add(:run_id, references(:runs, type: :uuid))
      add(:run_ref, :text, null: false)
      add(:authority_context_ref, :text, null: false)
      add(:state, :text, null: false)
      add(:acceptance, :map, null: false, default: %{})
      add(:row_version, :bigint, null: false, default: 1)
      timestamps(type: :utc_datetime_usec)
    end

    create(
      unique_index(
        :agent_run_commands,
        [:tenant_id, :installation_ref, :idempotency_key],
        name: :agent_run_commands_idempotency
      )
    )

    create(unique_index(:agent_run_commands, [:tenant_id, :run_ref]))

    create table(:agent_turns, primary_key: false) do
      add(:turn_ref, :text, primary_key: true)
      add(:run_id, references(:runs, type: :uuid), null: false)
      add(:tenant_id, :text, null: false)
      add(:subject_ref, :text, null: false)
      add(:input_artifact_ref, :text, null: false)
      add(:payload_digest, :text, null: false)
      add(:idempotency_key, :text, null: false)
      add(:sequence, :bigint, null: false)
      add(:status, :text, null: false)
      add(:provider_attempt_ref, :text)
      add(:row_version, :bigint, null: false, default: 1)
      timestamps(type: :utc_datetime_usec)
    end

    create(unique_index(:agent_turns, [:run_id, :sequence]))
    create(unique_index(:agent_turns, [:run_id, :idempotency_key]))

    create table(:agent_run_events, primary_key: false) do
      add(:event_ref, :text, primary_key: true)
      add(:run_id, references(:runs, type: :uuid), null: false)
      add(:run_ref, :text, null: false)
      add(:tenant_id, :text, null: false)
      add(:event_type, :text, null: false)
      add(:event_version, :integer, null: false)
      add(:sequence, :bigint, null: false)
      add(:command_ref, :text, null: false)
      add(:causation_ref, :text)
      add(:correlation_ref, :text, null: false)
      add(:payload_ref, :text, null: false)
      add(:payload_digest, :text, null: false)
      add(:recorded_at, :utc_datetime_usec, null: false)
      add(:row_version, :bigint, null: false, default: 1)
    end

    create(unique_index(:agent_run_events, [:run_id, :sequence]))
    create(index(:agent_run_events, [:tenant_id, :recorded_at]))

    create table(:agent_run_projections, primary_key: false) do
      add(:run_id, references(:runs, type: :uuid), primary_key: true)
      add(:run_ref, :text, null: false)
      add(:tenant_id, :text, null: false)
      add(:work_object_id, references(:work_objects, type: :uuid), null: false)
      add(:subject_ref, :text, null: false)
      add(:latest_turn_ref, :text, null: false)
      add(:latest_event_ref, :text, null: false)
      add(:status, :text, null: false)
      add(:event_sequence, :bigint, null: false)
      add(:run_revision, :bigint, null: false)
      add(:projection, :map, null: false)
      timestamps(type: :utc_datetime_usec)
    end

    create(unique_index(:agent_run_projections, [:tenant_id, :run_ref]))
    create(index(:agent_run_projections, [:tenant_id, :status]))

    create table(:agent_run_cursors, primary_key: false) do
      add(:run_id, references(:runs, type: :uuid), primary_key: true)
      add(:run_ref, :text, null: false)
      add(:last_event_ref, :text, null: false)
      add(:sequence, :bigint, null: false)
      add(:row_version, :bigint, null: false, default: 1)
      timestamps(type: :utc_datetime_usec)
    end

    create(unique_index(:agent_run_cursors, [:run_ref]))

    create table(:agent_workflow_outbox, primary_key: false) do
      add(:outbox_ref, :text, primary_key: true)
      add(:event_ref, :text, null: false)
      add(:run_id, references(:runs, type: :uuid), null: false)
      add(:run_ref, :text, null: false)
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

    create(unique_index(:agent_workflow_outbox, [:run_id, :idempotency_key]))
    create(index(:agent_workflow_outbox, [:state, :available_at]))

    create(
      constraint(:agent_workflow_outbox, :agent_workflow_outbox_state,
        check: "state IN ('pending', 'dispatched', 'acknowledged', 'ambiguous', 'failed')"
      )
    )

    drop_if_exists(table(:mezzanine_workflow_outbox))
    drop_if_exists(table(:mezzanine_run_cursors))
    drop_if_exists(table(:mezzanine_run_projections))
    drop_if_exists(table(:mezzanine_run_events))
    drop_if_exists(table(:mezzanine_turns))
    drop_if_exists(table(:mezzanine_run_commands))
    drop_if_exists(table(:mezzanine_runs))
  end

  def down do
    drop_if_exists(table(:agent_workflow_outbox))
    drop_if_exists(table(:agent_run_cursors))
    drop_if_exists(table(:agent_run_projections))
    drop_if_exists(table(:agent_run_events))
    drop_if_exists(table(:agent_turns))
    drop_if_exists(table(:agent_run_commands))

    drop_if_exists(index(:runs, [:external_ref], name: :runs_unique_external_ref_index))

    alter table(:runs) do
      remove(:external_ref)
      remove(:row_version)
    end
  end
end
