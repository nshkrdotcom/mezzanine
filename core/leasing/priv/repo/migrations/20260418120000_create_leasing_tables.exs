defmodule Mezzanine.Leasing.Repo.Migrations.CreateLeasingTables do
  use Ecto.Migration

  def change do
    create table(:read_leases, primary_key: false) do
      add :lease_id, :uuid, primary_key: true
      add :trace_id, :text, null: false
      add :tenant_id, :text, null: false
      add :installation_id, :text
      add :subject_id, :uuid
      add :execution_id, :uuid
      add :lineage_anchor, :map, null: false, default: %{}
      add :allowed_family, :text, null: false
      add :allowed_operations, {:array, :text}, null: false, default: []
      add :scope, :map, null: false, default: %{}
      add :lease_token_digest, :text, null: false
      add :expires_at, :utc_datetime_usec, null: false
      add :issued_invalidation_cursor, :bigint, null: false, default: 0
      add :invalidation_channel, :text, null: false

      timestamps(type: :utc_datetime_usec)
    end

    create index(:read_leases, [:subject_id])
    create index(:read_leases, [:execution_id])
    create index(:read_leases, [:installation_id])
    create index(:read_leases, [:tenant_id])
    create index(:read_leases, [:trace_id])

    create table(:stream_attach_leases, primary_key: false) do
      add :lease_id, :uuid, primary_key: true
      add :trace_id, :text, null: false
      add :tenant_id, :text, null: false
      add :installation_id, :text
      add :subject_id, :uuid
      add :execution_id, :uuid
      add :lineage_anchor, :map, null: false, default: %{}
      add :allowed_family, :text, null: false
      add :scope, :map, null: false, default: %{}
      add :attach_token_digest, :text, null: false
      add :expires_at, :utc_datetime_usec, null: false
      add :issued_invalidation_cursor, :bigint, null: false, default: 0
      add :last_invalidation_cursor, :bigint, null: false, default: 0
      add :invalidation_channel, :text, null: false

      timestamps(type: :utc_datetime_usec)
    end

    create index(:stream_attach_leases, [:subject_id])
    create index(:stream_attach_leases, [:execution_id])
    create index(:stream_attach_leases, [:installation_id])
    create index(:stream_attach_leases, [:tenant_id])
    create index(:stream_attach_leases, [:trace_id])

    create table(:lease_invalidations) do
      add :lease_id, :uuid, null: false
      add :lease_kind, :text, null: false
      add :tenant_id, :text, null: false
      add :installation_id, :text
      add :subject_id, :uuid
      add :execution_id, :uuid
      add :trace_id, :text, null: false
      add :reason, :text, null: false
      add :sequence_number, :bigint, null: false
      add :invalidated_at, :utc_datetime_usec, null: false

      timestamps(type: :utc_datetime_usec, updated_at: false)
    end

    create unique_index(:lease_invalidations, [:sequence_number],
             name: :lease_invalidations_sequence_number_index
           )

    create index(:lease_invalidations, [:lease_id, :lease_kind, :sequence_number])
    create index(:lease_invalidations, [:subject_id])
    create index(:lease_invalidations, [:execution_id])
    create index(:lease_invalidations, [:installation_id])
    create index(:lease_invalidations, [:tenant_id])
  end
end
