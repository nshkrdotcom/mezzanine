defmodule Mezzanine.Review.ReviewDecision do
  @moduledoc false
  use Ash.Resource,
    domain: Mezzanine.Review,
    data_layer: AshPostgres.DataLayer,
    authorizers: [Ash.Policy.Authorizer]

  postgres do
    table "review_decisions"
    repo Mezzanine.OpsDomain.Repo
  end

  code_interface do
    define :record_decision, action: :record_decision
    define :list_for_review_unit, action: :list_for_review_unit, args: [:review_unit_id]
  end

  actions do
    defaults []

    read :read do
      primary? true
    end

    create :record_decision do
      accept [:review_unit_id, :decision, :actor_kind, :actor_ref, :reason, :payload, :decided_at]
    end

    read :list_for_review_unit do
      argument :review_unit_id, :uuid, allow_nil?: false
      filter expr(review_unit_id == ^arg(:review_unit_id))
    end
  end

  policies do
    policy action_type(:read) do
      authorize_if expr(tenant_id == ^actor(:tenant_id))
    end

    policy action(:record_decision) do
      authorize_if actor_present()
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

    attribute :decision, :atom do
      allow_nil? false
      constraints one_of: [:accept, :reject, :waive, :escalate]
      public? true
    end

    attribute :actor_kind, :atom do
      constraints one_of: [:human, :agent, :system]
      public? true
    end

    attribute :actor_ref, :string do
      public? true
    end

    attribute :reason, :string do
      public? true
    end

    attribute :payload, :map do
      allow_nil? false
      default %{}
      public? true
    end

    attribute :decided_at, :utc_datetime do
      allow_nil? false
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
  end
end
