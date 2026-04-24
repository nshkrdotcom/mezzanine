defmodule Mezzanine.ConfigRegistry.Repo.Migrations.CreatePolicies do
  use Ecto.Migration

  def change do
    create table(:policies, primary_key: false) do
      add(:id, :uuid, primary_key: true, default: fragment("gen_random_uuid()"))
      add(:policy_id, :text, null: false)
      add(:tenant_ref, :text)
      add(:installation_ref, :text)
      add(:kind, :text, null: false)
      add(:version, :integer, null: false)
      add(:granularity_scope, :text, null: false)
      add(:spec, :map, null: false, default: %{})
      add(:effective_from, :utc_datetime_usec, null: false)
      add(:effective_until, :utc_datetime_usec)
      add(:authoring_bundle_ref, :map, null: false, default: %{})
      add(:trusted_registry_ref, :map, null: false, default: %{})

      timestamps(type: :utc_datetime_usec)
    end

    create(index(:policies, [:kind, :tenant_ref, :installation_ref, :granularity_scope]))
    create(index(:policies, [:policy_id, :version]))
  end
end
