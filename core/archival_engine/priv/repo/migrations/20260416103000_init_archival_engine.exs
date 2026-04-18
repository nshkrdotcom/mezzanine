defmodule Mezzanine.Archival.Repo.Migrations.InitArchivalEngine do
  use Ecto.Migration

  def change do
    create table(:archival_manifests, primary_key: false) do
      add(:id, :uuid, primary_key: true, default: fragment("gen_random_uuid()"))
      add(:manifest_ref, :text, null: false)
      add(:installation_id, :text, null: false)
      add(:subject_id, :uuid, null: false)
      add(:subject_state, :text, null: false)
      add(:execution_states, {:array, :text}, null: false, default: [])
      add(:trace_ids, {:array, :text}, null: false, default: [])
      add(:execution_ids, {:array, :uuid}, null: false, default: [])
      add(:decision_ids, {:array, :uuid}, null: false, default: [])
      add(:evidence_ids, {:array, :uuid}, null: false, default: [])
      add(:audit_fact_ids, {:array, :uuid}, null: false, default: [])
      add(:projection_names, {:array, :text}, null: false, default: [])
      add(:terminal_at, :utc_datetime_usec, null: false)
      add(:due_at, :utc_datetime_usec, null: false)
      add(:retention_seconds, :bigint, null: false)
      add(:storage_kind, :text, null: false)
      add(:status, :text, null: false, default: "staging")
      add(:storage_uri, :text)
      add(:checksum, :text)
      add(:verified_at, :utc_datetime_usec)
      add(:archived_at, :utc_datetime_usec)
      add(:failure_reason, :text)
      add(:metadata, :map, null: false, default: %{})
      add(:row_version, :bigint, null: false, default: 1)

      timestamps(type: :utc_datetime_usec)
    end

    create(
      unique_index(:archival_manifests, [:manifest_ref],
        name: "archival_manifests_unique_manifest_ref_index"
      )
    )

    create(index(:archival_manifests, [:installation_id, :status, :due_at]))
    create(index(:archival_manifests, [:installation_id, :subject_id, :terminal_at]))
    create(index(:archival_manifests, [:trace_ids], using: :gin))
    create(index(:archival_manifests, [:execution_ids], using: :gin))
  end
end
