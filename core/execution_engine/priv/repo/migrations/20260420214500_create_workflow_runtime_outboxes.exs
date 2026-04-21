defmodule Mezzanine.Execution.Repo.Migrations.CreateWorkflowRuntimeOutboxes do
  use Ecto.Migration

  def change do
    create_if_not_exists table(:workflow_start_outbox, primary_key: false) do
      add(:outbox_id, :text, primary_key: true)
      add(:tenant_ref, :text, null: false)
      add(:installation_ref, :text, null: false)
      add(:workspace_ref, :text)
      add(:project_ref, :text)
      add(:environment_ref, :text)
      add(:principal_ref, :text, null: false)
      add(:system_actor_ref, :text)
      add(:resource_ref, :text, null: false)
      add(:command_envelope_ref, :text)
      add(:command_receipt_ref, :text, null: false)
      add(:command_id, :text, null: false)
      add(:workflow_type, :text, null: false)
      add(:workflow_id, :text, null: false)
      add(:workflow_run_id, :text)
      add(:workflow_version, :text, null: false)
      add(:workflow_input_version, :text, null: false)
      add(:workflow_input_ref, :text, null: false)
      add(:authority_packet_ref, :text, null: false)
      add(:permission_decision_ref, :text, null: false)
      add(:idempotency_key, :text, null: false)
      add(:dedupe_scope, :text, null: false)
      add(:trace_id, :text, null: false)
      add(:correlation_id, :text)
      add(:release_manifest_ref, :text, null: false)
      add(:payload_hash, :text, null: false)
      add(:payload_ref, :text)
      add(:dispatch_state, :text, null: false)
      add(:retry_count, :integer, null: false, default: 0)
      add(:last_error_class, :text, null: false, default: "none")
      add(:started_at, :utc_datetime_usec)
      add(:available_at, :text)
      add(:oban_job_ref, :text)
      add(:row_version, :integer, null: false, default: 1)

      timestamps(type: :utc_datetime_usec)
    end

    create_if_not_exists(
      unique_index(:workflow_start_outbox, [:workflow_id, :idempotency_key],
        name: "workflow_start_outbox_unique_workflow_idempotency"
      )
    )

    create_if_not_exists(
      index(:workflow_start_outbox, [:dispatch_state],
        name: "workflow_start_outbox_dispatch_state_idx"
      )
    )

    create_if_not_exists table(:workflow_signal_outbox, primary_key: false) do
      add(:outbox_id, :text, primary_key: true)
      add(:tenant_ref, :text, null: false)
      add(:installation_ref, :text, null: false)
      add(:workspace_ref, :text)
      add(:project_ref, :text)
      add(:environment_ref, :text)
      add(:principal_ref, :text, null: false)
      add(:system_actor_ref, :text)
      add(:operator_ref, :text, null: false)
      add(:resource_ref, :text, null: false)
      add(:signal_id, :text, null: false)
      add(:workflow_id, :text, null: false)
      add(:workflow_run_id, :text)
      add(:signal_name, :text, null: false)
      add(:signal_version, :text, null: false)
      add(:signal_sequence, :integer)
      add(:authority_packet_ref, :text, null: false)
      add(:permission_decision_ref, :text, null: false)
      add(:idempotency_key, :text, null: false)
      add(:trace_id, :text, null: false)
      add(:correlation_id, :text)
      add(:release_manifest_ref, :text, null: false)
      add(:dispatch_state, :text, null: false)
      add(:workflow_effect_state, :text, null: false)
      add(:projection_state, :text, null: false)
      add(:available_at, :text, null: false)
      add(:dispatch_attempt_count, :integer, null: false, default: 0)
      add(:last_error_class, :text, null: false, default: "none")
      add(:oban_job_ref, :text)
      add(:row_version, :integer, null: false, default: 1)

      timestamps(type: :utc_datetime_usec)
    end

    create_if_not_exists(
      unique_index(:workflow_signal_outbox, [:workflow_id, :signal_id, :idempotency_key],
        name: "workflow_signal_outbox_unique_signal_idempotency"
      )
    )

    create_if_not_exists(
      index(:workflow_signal_outbox, [:dispatch_state],
        name: "workflow_signal_outbox_dispatch_state_idx"
      )
    )
  end
end
