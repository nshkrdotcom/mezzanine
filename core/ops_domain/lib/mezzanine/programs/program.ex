defmodule Mezzanine.Programs.Program do
  @moduledoc false
  use Ash.Resource,
    domain: Mezzanine.Programs,
    data_layer: AshPostgres.DataLayer,
    authorizers: [Ash.Policy.Authorizer]

  postgres do
    table "programs"
    repo Mezzanine.OpsDomain.Repo
  end

  code_interface do
    define :create_program, action: :create_program
    define :list_for_tenant, action: :list_for_tenant, args: [:tenant_id]
    define :by_slug, action: :by_slug, args: [:tenant_id, :slug]
    define :update_program, action: :update_program
    define :activate
    define :suspend
  end

  actions do
    defaults []

    read :read do
      primary? true
    end

    create :create_program do
      accept [:slug, :name, :product_family, :configuration, :metadata]
      change set_attribute(:status, :draft)
    end

    read :list_for_tenant do
      argument :tenant_id, :string, allow_nil?: false
      filter expr(tenant_id == ^arg(:tenant_id))
    end

    read :by_slug do
      argument :tenant_id, :string, allow_nil?: false
      argument :slug, :string, allow_nil?: false
      get? true
      filter expr(tenant_id == ^arg(:tenant_id) and slug == ^arg(:slug))
    end

    update :update_program do
      accept [:name, :product_family, :configuration, :metadata]
      require_atomic? false
    end

    update :activate do
      accept []
      require_atomic? false
      change set_attribute(:status, :active)
    end

    update :suspend do
      accept []
      require_atomic? false
      change set_attribute(:status, :suspended)
    end
  end

  policies do
    policy action_type(:read) do
      authorize_if expr(tenant_id == ^actor(:tenant_id))
    end

    policy action(:create_program) do
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

    attribute :slug, :string do
      allow_nil? false
      public? true
    end

    attribute :name, :string do
      allow_nil? false
      public? true
    end

    attribute :status, :atom do
      allow_nil? false
      default :draft
      constraints one_of: [:draft, :active, :suspended, :archived]
      public? true
    end

    attribute :product_family, :string do
      public? true
    end

    attribute :configuration, :map do
      allow_nil? false
      default %{}
      public? true
    end

    attribute :metadata, :map do
      allow_nil? false
      default %{}
      public? true
    end

    timestamps()
  end

  relationships do
    has_many :policy_bundles, Mezzanine.Programs.PolicyBundle do
      destination_attribute :program_id
    end

    has_many :placement_profiles, Mezzanine.Programs.PlacementProfile do
      destination_attribute :program_id
    end

    has_many :work_classes, Mezzanine.Work.WorkClass do
      destination_attribute :program_id
    end

    has_many :work_objects, Mezzanine.Work.WorkObject do
      destination_attribute :program_id
    end

    has_many :control_sessions, Mezzanine.Control.ControlSession do
      destination_attribute :program_id
    end
  end

  identities do
    identity :unique_slug_per_tenant, [:tenant_id, :slug]
  end
end
