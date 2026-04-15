defmodule Mezzanine.Review.Waiver do
  @moduledoc false
  use Ash.Resource,
    domain: Mezzanine.Review,
    data_layer: AshPostgres.DataLayer,
    authorizers: [Ash.Policy.Authorizer]

  postgres do
    table "waivers"
    repo Mezzanine.OpsDomain.Repo
  end

  code_interface do
    define :grant_waiver, action: :grant_waiver
    define :expire, action: :expire
    define :revoke, action: :revoke
  end

  actions do
    defaults []

    read :read do
      primary? true
    end

    create :grant_waiver do
      accept [:review_unit_id, :work_object_id, :reason, :granted_by, :expires_at, :conditions]
      change set_attribute(:status, :active)
    end

    update :expire do
      accept []
      require_atomic? false
      change set_attribute(:status, :expired)
    end

    update :revoke do
      accept []
      require_atomic? false
      change set_attribute(:status, :revoked)
    end
  end

  policies do
    policy action_type(:read) do
      authorize_if expr(tenant_id == ^actor(:tenant_id))
    end

    policy action(:grant_waiver) do
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

    attribute :review_unit_id, :uuid do
      allow_nil? false
      public? true
    end

    attribute :work_object_id, :uuid do
      allow_nil? false
      public? true
    end

    attribute :reason, :string do
      allow_nil? false
      public? true
    end

    attribute :granted_by, :string do
      allow_nil? false
      public? true
    end

    attribute :expires_at, :utc_datetime do
      public? true
    end

    attribute :conditions, {:array, :string} do
      allow_nil? false
      default []
      public? true
    end

    attribute :status, :atom do
      allow_nil? false
      default :active
      constraints one_of: [:active, :expired, :revoked]
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
