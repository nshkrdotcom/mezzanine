defmodule Mezzanine.OpsDomain.Repo.Migrations.AddAgentTurnSubmission do
  use Ecto.Migration

  def up do
    create table(:agent_turn_commands, primary_key: false) do
      add(:command_ref, :text, primary_key: true)
      add(:run_id, references(:runs, type: :uuid), null: false)
      add(:run_ref, :text, null: false)
      add(:tenant_id, :text, null: false)
      add(:idempotency_key, :text, null: false)
      add(:request_hash, :text, null: false)
      add(:turn_kind, :text, null: false)
      add(:state, :text, null: false)
      add(:acceptance, :map, null: false, default: %{})
      add(:row_version, :bigint, null: false, default: 1)
      timestamps(type: :utc_datetime_usec)
    end

    create(
      unique_index(:agent_turn_commands, [:run_ref, :idempotency_key],
        name: :agent_turn_commands_idempotency
      )
    )

    create(index(:agent_turn_commands, [:tenant_id, :run_ref, :inserted_at]))

    create(
      constraint(:agent_turn_commands, :agent_turn_commands_state,
        check: "state IN ('pending', 'accepted')"
      )
    )
  end

  def down do
    drop_if_exists(table(:agent_turn_commands))
  end
end
