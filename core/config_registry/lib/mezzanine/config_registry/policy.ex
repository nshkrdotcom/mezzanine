defmodule Mezzanine.ConfigRegistry.Policy do
  @moduledoc """
  Durable Phase 7 governed-memory policy registry row.
  """

  use Ash.Resource,
    domain: Mezzanine.ConfigRegistry,
    data_layer: AshPostgres.DataLayer

  postgres do
    table("policies")
    repo(Mezzanine.ConfigRegistry.Repo)
  end

  code_interface do
    define(:register, action: :register)
    define(:get, action: :read)
  end

  actions do
    defaults([:read])

    create :register do
      accept([
        :policy_id,
        :tenant_ref,
        :installation_ref,
        :kind,
        :version,
        :granularity_scope,
        :spec,
        :effective_from,
        :effective_until,
        :authoring_bundle_ref,
        :trusted_registry_ref
      ])
    end
  end

  attributes do
    uuid_primary_key(:id)

    attribute :policy_id, :string do
      allow_nil?(false)
      public?(true)
    end

    attribute :tenant_ref, :string do
      public?(true)
    end

    attribute :installation_ref, :string do
      public?(true)
    end

    attribute :kind, :atom do
      allow_nil?(false)
      constraints(one_of: [:read, :write, :transform, :share_up, :promote, :invalidate])
      public?(true)
    end

    attribute :version, :integer do
      allow_nil?(false)
      public?(true)
    end

    attribute :granularity_scope, :atom do
      allow_nil?(false)

      constraints(
        one_of: [:global, :tenant, :installation, :workspace, :agent, :actor_role, :time_window]
      )

      public?(true)
    end

    attribute :spec, :map do
      allow_nil?(false)
      default(%{})
      public?(true)
    end

    attribute :effective_from, :utc_datetime_usec do
      allow_nil?(false)
      public?(true)
    end

    attribute :effective_until, :utc_datetime_usec do
      public?(true)
    end

    attribute :authoring_bundle_ref, :map do
      allow_nil?(false)
      default(%{})
      public?(true)
    end

    attribute :trusted_registry_ref, :map do
      allow_nil?(false)
      default(%{})
      public?(true)
    end

    timestamps()
  end

  identities do
    identity(:unique_policy_scope_version, [
      :policy_id,
      :kind,
      :version,
      :granularity_scope,
      :tenant_ref,
      :installation_ref
    ])
  end
end
