defmodule Mezzanine.ConfigRegistry.Repo.Migrations.CreateBindingRegistryResources do
  use Ecto.Migration

  def change do
    execute "CREATE EXTENSION IF NOT EXISTS \"pgcrypto\""
    execute "CREATE SEQUENCE IF NOT EXISTS binding_registry_epoch_seq"

    create table(:binding_sets, primary_key: false) do
      add(:id, :uuid, primary_key: true, default: fragment("gen_random_uuid()"))
      add(:tenant_id, :text, null: false)
      add(:environment, :text, null: false, default: "default")
      add(:pack_slug, :text, null: false)
      add(:installation_id, references(:installations, type: :uuid, on_delete: :restrict),
        null: false
      )

      add(:pack_registration_id, references(:pack_registrations, type: :uuid, on_delete: :restrict),
        null: false
      )

      add(:compiled_pack_revision, :bigint, null: false)
      add(:binding_epoch, :bigint, null: false)
      add(:status, :text, null: false, default: "active")
      add(:binding_config, :map, null: false, default: %{})
      add(:metadata, :map, null: false, default: %{})

      timestamps(type: :utc_datetime_usec)
    end

    create(
      unique_index(:binding_sets, [:tenant_id, :environment, :pack_slug, :binding_epoch],
        name: "binding_sets_unique_binding_epoch"
      )
    )

    create(index(:binding_sets, [:installation_id]))
    create(index(:binding_sets, [:pack_registration_id]))

    create table(:active_binding_sets, primary_key: false) do
      add(:id, :uuid, primary_key: true, default: fragment("gen_random_uuid()"))
      add(:tenant_id, :text, null: false)
      add(:environment, :text, null: false, default: "default")
      add(:pack_slug, :text, null: false)
      add(:installation_id, references(:installations, type: :uuid, on_delete: :restrict),
        null: false
      )

      add(:binding_set_id, references(:binding_sets, type: :uuid, on_delete: :restrict),
        null: false
      )

      add(:binding_epoch, :bigint, null: false)
      add(:compiled_pack_revision, :bigint, null: false)
      add(:metadata, :map, null: false, default: %{})

      timestamps(type: :utc_datetime_usec)
    end

    create(
      unique_index(:active_binding_sets, [:tenant_id, :environment, :pack_slug],
        name: "active_binding_sets_unique_scope"
      )
    )

    create(index(:active_binding_sets, [:binding_set_id]))

    create table(:compiled_bindings, primary_key: false) do
      add(:id, :uuid, primary_key: true, default: fragment("gen_random_uuid()"))
      add(:binding_set_id, references(:binding_sets, type: :uuid, on_delete: :delete_all),
        null: false
      )

      add(:binding_ref, :text, null: false)
      add(:binding_kind, :text, null: false)
      add(:connector_ref, :text, null: false)
      add(:manifest_ref, :text, null: false)
      add(:credential_binding_ref, :text, null: false)
      add(:runtime_family, :text)
      add(:operation_refs, :map, null: false, default: %{})
      add(:binding_payload, :map, null: false, default: %{})
      add(:metadata, :map, null: false, default: %{})

      timestamps(type: :utc_datetime_usec)
    end

    create(
      unique_index(:compiled_bindings, [:binding_set_id, :binding_ref],
        name: "compiled_bindings_unique_ref"
      )
    )

    create(index(:compiled_bindings, [:binding_kind]))
    create(index(:compiled_bindings, [:connector_ref, :manifest_ref]))

    create table(:binding_manifest_dependencies, primary_key: false) do
      add(:id, :uuid, primary_key: true, default: fragment("gen_random_uuid()"))
      add(:binding_set_id, references(:binding_sets, type: :uuid, on_delete: :delete_all),
        null: false
      )

      add(:compiled_binding_id, references(:compiled_bindings, type: :uuid, on_delete: :delete_all),
        null: false
      )

      add(:binding_ref, :text, null: false)
      add(:binding_kind, :text, null: false)
      add(:connector_ref, :text, null: false)
      add(:manifest_ref, :text, null: false)
      add(:operation_role, :text, null: false)
      add(:operation_ref, :text, null: false)
      add(:operation_class, :text, null: false)
      add(:side_effect_class, :text)
      add(:credential_scope_ref, :text, null: false)
      add(:required_runtime_family, :text)
      add(:manifest_digest, :text)
      add(:required_scopes, {:array, :text}, null: false, default: [])
      add(:metadata, :map, null: false, default: %{})

      timestamps(type: :utc_datetime_usec)
    end

    create(
      unique_index(:binding_manifest_dependencies, [
        :compiled_binding_id,
        :operation_role,
        :operation_ref
      ],
        name: "binding_manifest_dependencies_unique_operation"
      )
    )

    create(index(:binding_manifest_dependencies, [:binding_set_id]))
    create(index(:binding_manifest_dependencies, [:connector_ref, :manifest_ref]))

    create table(:run_binding_snapshots, primary_key: false) do
      add(:id, :uuid, primary_key: true, default: fragment("gen_random_uuid()"))
      add(:snapshot_ref, :text, null: false)
      add(:tenant_id, :text, null: false)
      add(:environment, :text, null: false, default: "default")
      add(:pack_slug, :text, null: false)
      add(:run_ref, :text, null: false)
      add(:binding_ref, :text, null: false)
      add(:binding_kind, :text, null: false)
      add(:binding_set_id, references(:binding_sets, type: :uuid, on_delete: :restrict),
        null: false
      )

      add(:compiled_binding_id, references(:compiled_bindings, type: :uuid, on_delete: :restrict),
        null: false
      )

      add(:binding_epoch, :bigint, null: false)
      add(:compiled_pack_revision, :bigint, null: false)
      add(:descriptor, :map, null: false, default: %{})
      add(:manifest_dependencies, :map, null: false, default: %{})
      add(:metadata, :map, null: false, default: %{})

      timestamps(type: :utc_datetime_usec)
    end

    create(unique_index(:run_binding_snapshots, [:snapshot_ref],
             name: "run_binding_snapshots_unique_snapshot_ref"
           ))

    create(
      unique_index(:run_binding_snapshots, [:tenant_id, :environment, :run_ref, :binding_ref],
        name: "run_binding_snapshots_unique_run_binding"
      )
    )

    create(index(:run_binding_snapshots, [:binding_set_id]))
  end
end
