defmodule Mezzanine.Work.WorkClass do
  @moduledoc false
  use Ash.Resource,
    domain: Mezzanine.Work,
    data_layer: AshPostgres.DataLayer,
    authorizers: [Ash.Policy.Authorizer]

  postgres do
    table "work_classes"
    repo Mezzanine.OpsDomain.Repo
  end

  code_interface do
    define :create_work_class, action: :create_work_class
    define :list_for_program, action: :list_for_program, args: [:program_id]
    define :update_work_class, action: :update_work_class
  end

  actions do
    defaults []

    read :read do
      primary? true
    end

    create :create_work_class do
      accept [
        :program_id,
        :name,
        :kind,
        :intake_schema,
        :policy_bundle_id,
        :default_review_profile,
        :default_run_profile
      ]

      change set_attribute(:status, :active)
    end

    update :update_work_class do
      accept [
        :name,
        :kind,
        :intake_schema,
        :policy_bundle_id,
        :default_review_profile,
        :default_run_profile,
        :status
      ]

      require_atomic? false
    end

    read :list_for_program do
      argument :program_id, :uuid, allow_nil?: false
      filter expr(program_id == ^arg(:program_id))
    end
  end

  policies do
    policy action_type(:read) do
      authorize_if expr(tenant_id == ^actor(:tenant_id))
    end

    policy action(:create_work_class) do
      authorize_if actor_present()
    end

    policy action(:update_work_class) do
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

    attribute :name, :string do
      allow_nil? false
      public? true
    end

    attribute :kind, :string do
      allow_nil? false
      public? true
    end

    attribute :intake_schema, :map do
      allow_nil? false
      default %{}
      public? true
    end

    attribute :policy_bundle_id, :uuid do
      public? true
    end

    attribute :default_review_profile, :map do
      allow_nil? false
      default %{}
      public? true
    end

    attribute :default_run_profile, :map do
      allow_nil? false
      default %{}
      public? true
    end

    attribute :status, :atom do
      allow_nil? false
      default :active
      constraints one_of: [:active, :inactive]
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

    belongs_to :policy_bundle, Mezzanine.Programs.PolicyBundle do
      attribute_type :uuid
      public? true
    end

    has_many :work_objects, Mezzanine.Work.WorkObject do
      destination_attribute :work_class_id
    end
  end

  identities do
    identity :unique_name_per_program, [:program_id, :name]
  end
end
