defmodule Mezzanine.Programs.PlacementProfile do
  @moduledoc false
  use Ash.Resource,
    domain: Mezzanine.Programs,
    data_layer: AshPostgres.DataLayer,
    authorizers: [Ash.Policy.Authorizer]

  postgres do
    table "placement_profiles"
    repo Mezzanine.OpsDomain.Repo
  end

  code_interface do
    define :create_profile, action: :create_profile
    define :list_for_program, action: :list_for_program, args: [:program_id]
    define :update_profile, action: :update_profile
    define :activate, action: :activate
    define :retire, action: :retire
  end

  actions do
    defaults []

    read :read do
      primary? true
    end

    create :create_profile do
      accept [
        :program_id,
        :profile_id,
        :strategy,
        :target_selector,
        :runtime_preferences,
        :workspace_policy,
        :metadata
      ]

      change set_attribute(:status, :draft)
    end

    read :list_for_program do
      argument :program_id, :uuid, allow_nil?: false
      filter expr(program_id == ^arg(:program_id))
    end

    update :update_profile do
      accept [
        :strategy,
        :target_selector,
        :runtime_preferences,
        :workspace_policy,
        :metadata
      ]

      require_atomic? false
    end

    update :activate do
      accept []
      require_atomic? false
      change set_attribute(:status, :active)
    end

    update :retire do
      accept []
      require_atomic? false
      change set_attribute(:status, :retired)
    end
  end

  policies do
    policy action_type(:read) do
      authorize_if expr(tenant_id == ^actor(:tenant_id))
    end

    policy action(:create_profile) do
      authorize_if actor_present()
    end

    policy action_type(:update) do
      authorize_if expr(tenant_id == ^actor(:tenant_id))
    end
  end

  multitenancy do
    strategy :attribute
    attribute :tenant_id
    global? false
  end

  attributes do
    uuid_primary_key :id

    attribute :tenant_id, :string do
      allow_nil? false
      public? true
    end

    attribute :program_id, :uuid do
      allow_nil? false
      public? true
    end

    attribute :profile_id, :string do
      allow_nil? false
      public? true
    end

    attribute :strategy, :string do
      allow_nil? false
      public? true
    end

    attribute :target_selector, :map do
      allow_nil? false
      default %{}
      public? true
    end

    attribute :runtime_preferences, :map do
      allow_nil? false
      default %{}
      public? true
    end

    attribute :workspace_policy, :map do
      allow_nil? false
      default %{}
      public? true
    end

    attribute :metadata, :map do
      allow_nil? false
      default %{}
      public? true
    end

    attribute :status, :atom do
      allow_nil? false
      default :draft
      constraints one_of: [:draft, :active, :retired]
      public? true
    end

    timestamps()
  end

  relationships do
    belongs_to :program, Mezzanine.Programs.Program do
      attribute_type :uuid
      allow_nil? false
      public? true
    end
  end

  identities do
    identity :unique_profile_per_program, [:program_id, :profile_id]
  end
end
