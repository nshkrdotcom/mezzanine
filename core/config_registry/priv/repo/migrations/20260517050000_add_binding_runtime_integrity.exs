defmodule Mezzanine.ConfigRegistry.Repo.Migrations.AddBindingRuntimeIntegrity do
  use Ecto.Migration

  def change do
    alter table(:compiled_bindings) do
      add(:policy_refs, {:array, :text}, null: false, default: [])
      add(:checksum, :text, null: false, default: "")
    end

    create(index(:compiled_bindings, [:checksum]))

    create(
      index(:binding_manifest_dependencies, [:binding_set_id, :binding_ref, :operation_role],
        name: "binding_manifest_dependencies_role_lookup"
      )
    )
  end
end
