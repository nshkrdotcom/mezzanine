defmodule Mezzanine.OpsDomain.Repo.Migrations.AddAgentRunRecoveryControl do
  use Ecto.Migration

  def up do
    alter table(:agent_run_projections) do
      add(:control_state, :text, null: false, default: "accepted")
      add(:control_generation, :bigint, null: false, default: 1)
      add(:control_attempt_sequence, :bigint, null: false, default: 0)
      add(:control_sequence, :bigint, null: false, default: 0)
      add(:control_row_version, :bigint, null: false, default: 1)
      add(:current_attempt_ref, :text)
      add(:generation_ref, :text)
      add(:external_operation_ref, :text)
      add(:deadline_at, :utc_datetime_usec)
      add(:fence_epoch, :bigint, null: false, default: 1)
      add(:reconciliation_attempts, :bigint, null: false, default: 0)
      add(:reconcile_owner, :text)
      add(:reconcile_lease_expires_at, :utc_datetime_usec)
      add(:next_reconcile_at, :utc_datetime_usec)
      add(:terminal_receipt_ref, :text)
      add(:last_control_error, :text)
      add(:control_updated_at, :utc_datetime_usec)
    end

    execute("""
    UPDATE agent_run_projections
    SET control_state = CASE status
          WHEN 'completed' THEN 'completed'
          WHEN 'failed' THEN 'failed'
          WHEN 'cancelled' THEN 'cancelled'
          ELSE 'accepted'
        END,
        control_updated_at = updated_at
    """)

    create(index(:agent_run_projections, [:tenant_id, :control_state]))
    create(index(:agent_run_projections, [:control_state, :next_reconcile_at]))
    create(index(:agent_run_projections, [:control_state, :deadline_at]))

    create(
      constraint(:agent_run_projections, :agent_run_control_state,
        check: """
        control_state IN (
          'accepted', 'running', 'pause_requested', 'paused', 'resume_requested',
          'cancel_requested', 'retry_requested', 'supersede_requested',
          'outcome_unknown', 'reconciling', 'operator_required',
          'completed', 'failed', 'cancelled'
        )
        """
      )
    )

    create(
      constraint(:agent_run_projections, :agent_run_control_versions,
        check: """
        control_generation > 0 AND control_attempt_sequence >= 0 AND
        control_sequence >= 0 AND control_row_version > 0 AND fence_epoch > 0 AND
        reconciliation_attempts >= 0
        """
      )
    )

    create table(:agent_run_control_commands, primary_key: false) do
      add(:command_ref, :text, primary_key: true)
      add(:run_ref, :text, null: false)
      add(:tenant_id, :text, null: false)
      add(:idempotency_key, :text, null: false)
      add(:request_digest, :text, null: false)
      add(:action, :text, null: false)
      add(:expected_row_version, :bigint, null: false)
      add(:result_row_version, :bigint, null: false)
      add(:result, :map, null: false)
      add(:recorded_at, :utc_datetime_usec, null: false)
    end

    create(
      unique_index(:agent_run_control_commands, [:run_ref, :idempotency_key],
        name: :agent_run_control_commands_idempotency
      )
    )

    create(index(:agent_run_control_commands, [:tenant_id, :run_ref, :recorded_at]))

    create table(:agent_run_control_events, primary_key: false) do
      add(:event_ref, :text, primary_key: true)
      add(:run_ref, :text, null: false)
      add(:tenant_id, :text, null: false)
      add(:sequence, :bigint, null: false)
      add(:command_ref, :text, null: false)
      add(:event_type, :text, null: false)
      add(:from_state, :text, null: false)
      add(:to_state, :text, null: false)
      add(:attempt_ref, :text)
      add(:generation_ref, :text)
      add(:external_operation_ref, :text)
      add(:fence_epoch, :bigint, null: false)
      add(:payload_digest, :text, null: false)
      add(:metadata, :map, null: false, default: %{})
      add(:recorded_at, :utc_datetime_usec, null: false)
    end

    create(unique_index(:agent_run_control_events, [:run_ref, :sequence]))
    create(index(:agent_run_control_events, [:tenant_id, :recorded_at]))

    create table(:agent_control_signal_outbox, primary_key: false) do
      add(:outbox_ref, :text, primary_key: true)
      add(:run_ref, :text, null: false)
      add(:tenant_id, :text, null: false)
      add(:command_ref, :text, null: false)
      add(:workflow_ref, :text, null: false)
      add(:signal_id, :text, null: false)
      add(:signal_name, :text, null: false)
      add(:signal_version, :text, null: false)
      add(:signal_payload, :map, null: false)
      add(:payload_digest, :text, null: false)
      add(:authority_ref, :text, null: false)
      add(:idempotency_key, :text, null: false)
      add(:state, :text, null: false)
      add(:attempt, :integer, null: false, default: 0)
      add(:available_at, :utc_datetime_usec, null: false)
      add(:lock_owner, :text)
      add(:lock_expires_at, :utc_datetime_usec)
      add(:dispatch_fence, :bigint, null: false, default: 0)
      add(:last_error_ref, :text)
      add(:delivered_at, :utc_datetime_usec)
      add(:row_version, :bigint, null: false, default: 1)
      timestamps(type: :utc_datetime_usec)
    end

    create(
      unique_index(:agent_control_signal_outbox, [:run_ref, :idempotency_key],
        name: :agent_control_signal_outbox_idempotency
      )
    )

    create(index(:agent_control_signal_outbox, [:state, :available_at]))

    create(
      constraint(:agent_control_signal_outbox, :agent_control_signal_outbox_state,
        check: """
        state IN (
          'queued', 'dispatching', 'delivered', 'retryable',
          'terminal_failure', 'operator_required'
        )
        """
      )
    )
  end

  def down do
    drop_if_exists(table(:agent_control_signal_outbox))
    drop_if_exists(table(:agent_run_control_events))
    drop_if_exists(table(:agent_run_control_commands))

    alter table(:agent_run_projections) do
      remove(:control_state)
      remove(:control_generation)
      remove(:control_attempt_sequence)
      remove(:control_sequence)
      remove(:control_row_version)
      remove(:current_attempt_ref)
      remove(:generation_ref)
      remove(:external_operation_ref)
      remove(:deadline_at)
      remove(:fence_epoch)
      remove(:reconciliation_attempts)
      remove(:reconcile_owner)
      remove(:reconcile_lease_expires_at)
      remove(:next_reconcile_at)
      remove(:terminal_receipt_ref)
      remove(:last_control_error)
      remove(:control_updated_at)
    end
  end
end
