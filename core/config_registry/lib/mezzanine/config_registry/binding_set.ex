defmodule Mezzanine.ConfigRegistry.BindingSet do
  @moduledoc """
  Durable activation epoch for a pack installation's generic binding graph.
  """

  use Ash.Resource,
    domain: Mezzanine.ConfigRegistry,
    data_layer: AshPostgres.DataLayer

  postgres do
    table("binding_sets")
    repo(Mezzanine.ConfigRegistry.Repo)
  end

  code_interface do
    define(:register, action: :register)
    define(:get, action: :read)

    define(:by_scope_epoch,
      action: :by_scope_epoch,
      args: [:tenant_id, :environment, :pack_slug, :binding_epoch]
    )

    define(:for_installation, action: :for_installation, args: [:installation_id])
  end

  actions do
    defaults([:read])

    create :register do
      accept([
        :tenant_id,
        :environment,
        :pack_slug,
        :installation_id,
        :pack_registration_id,
        :compiled_pack_revision,
        :binding_epoch,
        :status,
        :binding_config,
        :metadata
      ])
    end

    read :by_scope_epoch do
      argument(:tenant_id, :string, allow_nil?: false)
      argument(:environment, :string, allow_nil?: false)
      argument(:pack_slug, :string, allow_nil?: false)
      argument(:binding_epoch, :integer, allow_nil?: false)
      get?(true)

      filter(
        expr(
          tenant_id == ^arg(:tenant_id) and environment == ^arg(:environment) and
            pack_slug == ^arg(:pack_slug) and binding_epoch == ^arg(:binding_epoch)
        )
      )
    end

    read :for_installation do
      argument(:installation_id, :uuid, allow_nil?: false)
      filter(expr(installation_id == ^arg(:installation_id)))
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

    attribute :pack_registration_id, :uuid do
      allow_nil?(false)
      public?(true)
    end

    attribute :compiled_pack_revision, :integer do
      allow_nil?(false)
      public?(true)
    end

    attribute :binding_epoch, :integer do
      allow_nil?(false)
      public?(true)
    end

    attribute :status, :atom do
      allow_nil?(false)
      default(:active)
      constraints(one_of: [:active, :retired])
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

  identities do
    identity(:unique_binding_epoch, [:tenant_id, :environment, :pack_slug, :binding_epoch])
  end
end
