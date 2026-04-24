defmodule Mezzanine.ConfigRegistry.Repo.Migrations.CreateSharedScopeRegistryEntries do
  use Ecto.Migration

  def change do
    execute "CREATE EXTENSION IF NOT EXISTS \"pgcrypto\""

    create table(:shared_scope_registry_entries, primary_key: false) do
      add(:id, :uuid, primary_key: true, default: fragment("gen_random_uuid()"))
      add(:tenant_ref, :text, null: false)
      add(:scope_ref, :text, null: false)
      add(:governance_ref, :map, null: false, default: %{})
      add(:activation_epoch, :bigint, null: false)
      add(:deregistration_epoch, :bigint)
      add(:deregistration_governance_ref, :map)
      add(:source_node_ref, :text, null: false)
      add(:commit_lsn, :text, null: false)
      add(:commit_hlc, :map, null: false, default: %{})
      add(:invalidation_topic, :text, null: false)

      timestamps(type: :utc_datetime_usec)
    end

    create(index(:shared_scope_registry_entries, [:tenant_ref, :scope_ref, :activation_epoch]))
    create(index(:shared_scope_registry_entries, [:tenant_ref, :scope_ref, :deregistration_epoch]))
  end
end
