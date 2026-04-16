defmodule Mezzanine.ConfigRegistry.Installation do
  @moduledoc """
  Tenant-scoped installation record for an activated pack registration.

  Installations carry revisioned binding configuration so the runtime cache can
  deterministically reload the correct compiled pack snapshot.
  """
  use Ash.Resource,
    domain: Mezzanine.ConfigRegistry,
    data_layer: AshPostgres.DataLayer

  postgres do
    table("installations")
    repo(Mezzanine.ConfigRegistry.Repo)
  end

  code_interface do
    define(:create_installation, action: :create_installation)
    define(:get, action: :read)
    define(:active_installations, action: :active_installations)
    define(:activate_installation, action: :activate_installation)
    define(:suspend_installation, action: :suspend_installation)
    define(:reactivate_installation, action: :reactivate_installation)
    define(:update_bindings, action: :update_bindings)
  end

  actions do
    defaults([:read])

    create :create_installation do
      accept([
        :tenant_id,
        :environment,
        :pack_slug,
        :pack_registration_id,
        :binding_config,
        :metadata
      ])

      change(set_attribute(:status, :inactive))
      change(set_attribute(:compiled_pack_revision, 1))
    end

    read :active_installations do
      filter(expr(status == :active))
    end

    update :activate_installation do
      accept([])
      require_atomic?(false)
      change(set_attribute(:status, :active))
    end

    update :suspend_installation do
      accept([])
      require_atomic?(false)
      change(set_attribute(:status, :suspended))
    end

    update :reactivate_installation do
      accept([])
      require_atomic?(false)
      change(set_attribute(:status, :active))
    end

    update :update_bindings do
      argument(:binding_config, :map, allow_nil?: false)
      require_atomic?(false)

      change(fn changeset, _context ->
        updated_revision = (changeset.data.compiled_pack_revision || 0) + 1

        changeset
        |> Ash.Changeset.force_change_attribute(
          :binding_config,
          Ash.Changeset.get_argument(changeset, :binding_config)
        )
        |> Ash.Changeset.force_change_attribute(:compiled_pack_revision, updated_revision)
      end)
    end
  end

  attributes do
    uuid_primary_key(:id)

    attribute :tenant_id, :string do
      allow_nil?(false)
      public?(true)
    end

    attribute :environment, :string do
      allow_nil?(false)
      default("default")
      public?(true)
    end

    attribute :pack_slug, :string do
      allow_nil?(false)
      public?(true)
    end

    attribute :status, :atom do
      allow_nil?(false)
      default(:inactive)
      constraints(one_of: [:inactive, :active, :suspended, :degraded])
      public?(true)
    end

    attribute :compiled_pack_revision, :integer do
      allow_nil?(false)
      default(1)
      public?(true)
    end

    attribute :binding_config, :map do
      allow_nil?(false)
      default(%{})
      public?(true)
    end

    attribute :metadata, :map do
      allow_nil?(false)
      default(%{})
      public?(true)
    end

    timestamps()
  end

  relationships do
    belongs_to :pack_registration, Mezzanine.ConfigRegistry.PackRegistration do
      allow_nil?(false)
      public?(true)
      attribute_writable?(true)
    end
  end

  identities do
    identity(:unique_installation_scope, [:tenant_id, :environment, :pack_slug])
  end
end
