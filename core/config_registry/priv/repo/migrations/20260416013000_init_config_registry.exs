defmodule Mezzanine.ConfigRegistry.Repo.Migrations.InitConfigRegistry do
  use Ecto.Migration

  def change do
    execute "CREATE EXTENSION IF NOT EXISTS \"pgcrypto\""

    create table(:pack_registrations, primary_key: false) do
      add :id, :uuid, primary_key: true, default: fragment("gen_random_uuid()")
      add :pack_slug, :text, null: false
      add :version, :text, null: false
      add :status, :text, null: false
      add :compiled_manifest, :map, null: false, default: %{}
      add :canonical_subject_kinds, {:array, :text}, null: false, default: []
      add :serializer_version, :integer, null: false, default: 1
      add :migration_strategy, :text, null: false, default: "additive"

      timestamps(type: :utc_datetime_usec)
    end

    create unique_index(:pack_registrations, [:pack_slug, :version],
             name: "pack_registrations_unique_pack_version"
           )

    create table(:installations, primary_key: false) do
      add :id, :uuid, primary_key: true, default: fragment("gen_random_uuid()")
      add :tenant_id, :text, null: false
      add :environment, :text, null: false, default: "default"
      add :pack_slug, :text, null: false
      add :pack_registration_id,
          references(:pack_registrations, type: :uuid, on_delete: :restrict),
          null: false

      add :status, :text, null: false, default: "inactive"
      add :compiled_pack_revision, :integer, null: false, default: 1
      add :binding_config, :map, null: false, default: %{}
      add :metadata, :map, null: false, default: %{}

      timestamps(type: :utc_datetime_usec)
    end

    create unique_index(:installations, [:tenant_id, :environment, :pack_slug],
             name: "installations_unique_scope"
           )

    create index(:installations, [:status])
    create index(:installations, [:pack_registration_id])
  end
end
