defmodule Mezzanine.Projections.Repo.Migrations.AddProjectionLineageOutbox do
  use Ecto.Migration

  def change do
    create table(:projection_outbox_records, primary_key: false) do
      add(:id, :text, primary_key: true)
      add(:projection_ref, :text, null: false)
      add(:operation_context_ref, :text)
      add(:subject_ref, :text)
      add(:trace_ref, :text)
      add(:attrs, :binary, null: false)

      timestamps(type: :utc_datetime_usec)
    end

    create(index(:projection_outbox_records, [:projection_ref]))
    create(index(:projection_outbox_records, [:operation_context_ref]))
    create(index(:projection_outbox_records, [:subject_ref]))
    create(index(:projection_outbox_records, [:trace_ref]))

    create table(:projection_outbox_events, primary_key: false) do
      add(:id, :uuid, primary_key: true, default: fragment("gen_random_uuid()"))
      add(:record_id, references(:projection_outbox_records, type: :text, on_delete: :delete_all),
        null: false
      )

      add(:sequence, :bigint, null: false)
      add(:event_ref, :text, null: false)
      add(:trace_ref, :text)
      add(:event_kind, :text)
      add(:causal_order, :bigint)
      add(:payload, :binary, null: false)

      timestamps(type: :utc_datetime_usec)
    end

    create(unique_index(:projection_outbox_events, [:record_id, :sequence]))
    create(unique_index(:projection_outbox_events, [:record_id, :event_ref]))
    create(index(:projection_outbox_events, [:trace_ref]))
    create(index(:projection_outbox_events, [:event_kind]))
    create(index(:projection_outbox_events, [:causal_order]))
  end
end
