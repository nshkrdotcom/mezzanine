defmodule Mezzanine.Objects.Repo.Migrations.AddSourceMetadataToSubjectRecords do
  use Ecto.Migration

  def change do
    alter table(:subject_records) do
      add :source_event_id, :text
      add :source_binding_id, :text
      add :provider, :text
      add :provider_external_ref, :text
      add :provider_revision, :text
      add :source_state, :text
      add :state_mapping, :map, null: false, default: %{}
      add :blocker_refs, {:array, :map}, null: false, default: []
      add :labels, {:array, :text}, null: false, default: []
      add :priority, :integer
      add :branch_ref, :text
      add :source_url, :text
      add :workpad_ref, :text
      add :progress_ref, :text
      add :source_routing, :map, null: false, default: %{}
      add :lifecycle_version, :integer, null: false, default: 1
      add :payload_schema_revision, :text
    end

    create index(:subject_records, [:installation_id, :source_binding_id, :provider_external_ref],
             name: "subject_records_source_binding_external_ref_index"
           )

    create index(:subject_records, [:installation_id, :source_state])
  end
end
