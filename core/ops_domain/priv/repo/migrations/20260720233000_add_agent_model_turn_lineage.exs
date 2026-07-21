defmodule Mezzanine.OpsDomain.Repo.Migrations.AddAgentModelTurnLineage do
  use Ecto.Migration

  def up do
    create table(:agent_model_turn_lineage, primary_key: false) do
      add(
        :turn_ref,
        references(:agent_turns, column: :turn_ref, type: :text, on_delete: :delete_all),
        primary_key: true
      )

      add(:run_id, references(:runs, type: :uuid, on_delete: :delete_all), null: false)
      add(:run_ref, :text, null: false)
      add(:tenant_id, :text, null: false)
      add(:context_artifact_ref, :text, null: false)
      add(:context_digest, :text, null: false)
      add(:prompt_artifact_ref, :text, null: false)
      add(:decision_ref, :text, null: false)
      add(:grant_ref, :text, null: false)
      add(:provider_attempt_ref, :text, null: false)
      add(:provider_family, :text, null: false)
      add(:model_ref, :text, null: false)
      add(:operation_ref, :text, null: false)
      add(:state, :text, null: false)
      add(:provisional_event_sequence, :bigint, null: false, default: 0)
      add(:committed_event_sequence, :bigint, null: false, default: 0)
      add(:last_committed_provider_event_ref, :text)
      add(:reply_publication_ref, :text)
      add(:reply_artifact_ref, :text)
      add(:continuation_context_ref, :text)
      add(:continuation_context_digest, :text)
      add(:row_version, :bigint, null: false, default: 1)
      timestamps(type: :utc_datetime_usec)
    end

    create(unique_index(:agent_model_turn_lineage, [:tenant_id, :run_ref, :turn_ref]))
    create(unique_index(:agent_model_turn_lineage, [:provider_attempt_ref]))
    create(index(:agent_model_turn_lineage, [:tenant_id, :state]))

    create(
      constraint(:agent_model_turn_lineage, :agent_model_turn_lineage_state,
        check: "state IN ('running', 'completed', 'failed', 'cancelled', 'ambiguous')"
      )
    )

    create(
      constraint(:agent_model_turn_lineage, :agent_model_turn_lineage_sequences,
        check:
          "provisional_event_sequence >= committed_event_sequence AND committed_event_sequence >= 0"
      )
    )

    create table(:agent_provider_events, primary_key: false) do
      add(:event_ref, :text, primary_key: true)

      add(
        :turn_ref,
        references(:agent_model_turn_lineage,
          column: :turn_ref,
          type: :text,
          on_delete: :delete_all
        ),
        null: false
      )

      add(:run_ref, :text, null: false)
      add(:provider_attempt_ref, :text, null: false)
      add(:sequence, :bigint, null: false)
      add(:event_type, :text, null: false)
      add(:stream, :text, null: false)
      add(:payload_ref, :text, null: false)
      add(:payload_digest, :text, null: false)
      add(:commit_state, :text, null: false)
      add(:observed_at, :utc_datetime_usec, null: false)
      add(:committed_at, :utc_datetime_usec)
      add(:row_version, :bigint, null: false, default: 1)
      timestamps(type: :utc_datetime_usec)
    end

    create(unique_index(:agent_provider_events, [:turn_ref, :sequence]))
    create(index(:agent_provider_events, [:provider_attempt_ref, :commit_state]))

    create(
      constraint(:agent_provider_events, :agent_provider_events_commit_state,
        check: "commit_state IN ('provisional', 'committed')"
      )
    )
  end

  def down do
    drop_if_exists(table(:agent_provider_events))
    drop_if_exists(table(:agent_model_turn_lineage))
  end
end
