defmodule Mezzanine.Review.Escalation do
  @moduledoc false
  use Ash.Resource,
    domain: Mezzanine.Review,
    data_layer: AshPostgres.DataLayer,
    authorizers: [Ash.Policy.Authorizer]

  postgres do
    table "escalations"
    repo Mezzanine.OpsDomain.Repo
  end

  code_interface do
    define :raise_escalation, action: :raise_escalation
    define :resolve, action: :resolve
  end

  actions do
    defaults []

    read :read do
      primary? true
    end

    create :raise_escalation do
      accept [:review_unit_id, :work_object_id, :reason, :escalated_by, :assigned_to, :priority]
      change set_attribute(:status, :open)
    end

    update :resolve do
      accept [:resolved_at]
      require_atomic? false
      change set_attribute(:status, :resolved)
    end
  end

  policies do
    policy action_type(:read) do
      authorize_if expr(tenant_id == ^actor(:tenant_id))
    end

    policy action(:raise_escalation) do
      authorize_if actor_present()
    end

    policy action(:resolve) do
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

    attribute :review_unit_id, :uuid do
      allow_nil? false
      public? true
    end

    attribute :work_object_id, :uuid do
      allow_nil? false
      public? true
    end

    attribute :reason, :string do
      public? true
    end

    attribute :escalated_by, :string do
      public? true
    end

    attribute :assigned_to, :string do
      public? true
    end

    attribute :priority, :atom do
      constraints one_of: [:normal, :urgent, :critical]
      public? true
    end

    attribute :resolved_at, :utc_datetime do
      public? true
    end

    attribute :status, :atom do
      allow_nil? false
      default :open
      constraints one_of: [:open, :resolved, :dropped]
      public? true
    end

    timestamps()
  end

  relationships do
    belongs_to :review_unit, Mezzanine.Review.ReviewUnit do
      attribute_type :uuid
      allow_nil? false
      public? true
    end

    belongs_to :work_object, Mezzanine.Work.WorkObject do
      attribute_type :uuid
      allow_nil? false
      public? true
    end
  end
end
