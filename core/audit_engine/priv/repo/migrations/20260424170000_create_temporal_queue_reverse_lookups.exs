defmodule Mezzanine.Audit.Repo.Migrations.CreateTemporalQueueReverseLookups do
  use Ecto.Migration

  def change do
    create table(:temporal_queue_reverse_lookups, primary_key: false) do
      add(:id, :uuid, primary_key: true, null: false, default: fragment("gen_random_uuid()"))
      add(:hash_segment, :text, null: false)
      add(:typed_ref, :text, null: false)
      add(:ref_kind, :text, null: false)
      add(:queue, :text, null: false)

      timestamps(type: :utc_datetime_usec)
    end

    create(unique_index(:temporal_queue_reverse_lookups, [:hash_segment]))
    create(index(:temporal_queue_reverse_lookups, [:queue]))
    create(index(:temporal_queue_reverse_lookups, [:ref_kind, :typed_ref]))
  end
end
