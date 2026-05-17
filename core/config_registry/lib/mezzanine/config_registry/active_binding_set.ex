defmodule Mezzanine.ConfigRegistry.ActiveBindingSet do
  @moduledoc """
  Current binding-set pointer for a tenant/environment/pack scope.
  """

  use Ash.Resource,
    domain: Mezzanine.ConfigRegistry,
    data_layer: AshPostgres.DataLayer

  postgres do
    table("active_binding_sets")
    repo(Mezzanine.ConfigRegistry.Repo)
  end

  code_interface do
    define(:activate, action: :activate)
    define(:get, action: :read)
    define(:by_scope, action: :by_scope, args: [:tenant_id, :environment, :pack_slug])
    define(:by_installation, action: :by_installation, args: [:installation_id])
    define(:replace_binding_set, action: :replace_binding_set)
  end

  actions do
    defaults([:read])

    create :activate do
      accept([
        :tenant_id,
        :environment,
        :pack_slug,
        :installation_id,
        :binding_set_id,
        :binding_epoch,
        :compiled_pack_revision,
        :metadata
      ])
    end

    read :by_scope do
      argument(:tenant_id, :string, allow_nil?: false)
      argument(:environment, :string, allow_nil?: false)
      argument(:pack_slug, :string, allow_nil?: false)
      get?(true)

      filter(
        expr(
          tenant_id == ^arg(:tenant_id) and environment == ^arg(:environment) and
            pack_slug == ^arg(:pack_slug)
        )
      )
    end

    read :by_installation do
      argument(:installation_id, :uuid, allow_nil?: false)
      get?(true)

      filter(expr(installation_id == ^arg(:installation_id)))
    end

    update :replace_binding_set do
      accept([
        :installation_id,
        :binding_set_id,
        :binding_epoch,
        :compiled_pack_revision,
        :metadata
      ])

      require_atomic?(false)
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

    attribute :installation_id, :uuid do
      allow_nil?(false)
      public?(true)
    end

    attribute :binding_set_id, :uuid do
      allow_nil?(false)
      public?(true)
    end

    attribute :binding_epoch, :integer do
      allow_nil?(false)
      public?(true)
    end

    attribute :compiled_pack_revision, :integer do
      allow_nil?(false)
      public?(true)
    end

    attribute :metadata, :map do
      allow_nil?(false)
      default(%{})
      public?(true)
    end

    timestamps()
  end

  identities do
    identity(:unique_active_binding_scope, [:tenant_id, :environment, :pack_slug])
  end
end
