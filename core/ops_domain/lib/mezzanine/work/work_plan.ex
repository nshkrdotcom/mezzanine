defmodule Mezzanine.Work.WorkPlan do
  @moduledoc false
  use Ash.Resource,
    domain: Mezzanine.Work,
    data_layer: AshPostgres.DataLayer,
    authorizers: [Ash.Policy.Authorizer]

  alias Mezzanine.Work.Changes.CompileWorkPlan

  postgres do
    table "work_plans"
    repo Mezzanine.OpsDomain.Repo
  end

  code_interface do
    define :compile, action: :compile
    define :list_for_work_object, action: :list_for_work_object, args: [:work_object_id]
    define :supersede, action: :supersede
  end

  actions do
    defaults []

    read :read do
      primary? true
    end

    create :compile do
      accept [:work_object_id, :policy_bundle_id]
      change CompileWorkPlan
    end

    read :list_for_work_object do
      argument :work_object_id, :uuid, allow_nil?: false
      filter expr(work_object_id == ^arg(:work_object_id))
    end

    update :supersede do
      accept []
      require_atomic? false
      change set_attribute(:status, :superseded)
    end
  end

  policies do
    policy action_type(:read) do
      authorize_if expr(tenant_id == ^actor(:tenant_id))
    end

    policy action(:compile) do
      authorize_if actor_present()
    end

    policy action(:supersede) do
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

    attribute :work_object_id, :uuid do
      allow_nil? false
      public? true
    end

    attribute :policy_bundle_id, :uuid do
      public? true
    end

    attribute :version, :integer do
      allow_nil? false
      default 1
      public? true
    end

    attribute :status, :atom do
      allow_nil? false
      default :draft
      constraints one_of: [:draft, :compiled, :superseded]
      public? true
    end

    attribute :plan_payload, :map do
      allow_nil? false
      default %{}
      public? true
    end

    attribute :derived_run_intents, {:array, :map} do
      allow_nil? false
      default []
      public? true
    end

    attribute :derived_review_intents, {:array, :map} do
      allow_nil? false
      default []
      public? true
    end

    attribute :derived_effect_intents, {:array, :map} do
      allow_nil? false
      default []
      public? true
    end

    attribute :derived_read_intents, {:array, :map} do
      allow_nil? false
      default []
      public? true
    end

    attribute :derived_notification_intents, {:array, :map} do
      allow_nil? false
      default []
      public? true
    end

    attribute :obligation_ids, {:array, :string} do
      allow_nil? false
      default []
      public? true
    end

    attribute :compiled_at, :utc_datetime do
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
    belongs_to :work_object, Mezzanine.Work.WorkObject do
      attribute_type :uuid
      allow_nil? false
      public? true
    end

    belongs_to :policy_bundle, Mezzanine.Programs.PolicyBundle do
      attribute_type :uuid
      public? true
    end
  end

  identities do
    identity :unique_version_per_work_object, [:work_object_id, :version]
  end
end
