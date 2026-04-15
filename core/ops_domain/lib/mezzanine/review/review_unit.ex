defmodule Mezzanine.Review.ReviewUnit do
  @moduledoc false
  use Ash.Resource,
    domain: Mezzanine.Review,
    data_layer: AshPostgres.DataLayer,
    authorizers: [Ash.Policy.Authorizer]

  postgres do
    table "review_units"
    repo Mezzanine.OpsDomain.Repo
  end

  code_interface do
    define :create_review_unit, action: :create_review_unit
    define :list_for_work_object, action: :list_for_work_object, args: [:work_object_id]
    define :begin_review, action: :begin_review
    define :accept, action: :accept
    define :reject, action: :reject
    define :waive, action: :waive
    define :escalate, action: :escalate
  end

  actions do
    defaults []

    read :read do
      primary? true
    end

    create :create_review_unit do
      accept [
        :work_object_id,
        :run_id,
        :review_kind,
        :required_by,
        :decision_profile,
        :evidence_bundle_id,
        :reviewer_actor
      ]

      change set_attribute(:status, :pending)
    end

    read :list_for_work_object do
      argument :work_object_id, :uuid, allow_nil?: false
      filter expr(work_object_id == ^arg(:work_object_id))
    end

    update :begin_review do
      accept []
      require_atomic? false
      change set_attribute(:status, :in_review)
    end

    update :accept do
      accept []
      require_atomic? false
      change set_attribute(:status, :accepted)
    end

    update :reject do
      accept []
      require_atomic? false
      change set_attribute(:status, :rejected)
    end

    update :waive do
      accept []
      require_atomic? false
      change set_attribute(:status, :waived)
    end

    update :escalate do
      accept []
      require_atomic? false
      change set_attribute(:status, :escalated)
    end
  end

  policies do
    policy action_type(:read) do
      authorize_if expr(tenant_id == ^actor(:tenant_id))
    end

    policy action(:create_review_unit) do
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

    attribute :work_object_id, :uuid do
      allow_nil? false
      public? true
    end

    attribute :run_id, :uuid do
      public? true
    end

    attribute :review_kind, :atom do
      allow_nil? false

      constraints one_of: [
                    :code_review,
                    :policy_review,
                    :release_review,
                    :operator_review,
                    :evidence_review
                  ]

      public? true
    end

    attribute :status, :atom do
      allow_nil? false
      default :pending
      constraints one_of: [:pending, :in_review, :accepted, :rejected, :waived, :escalated]
      public? true
    end

    attribute :required_by, :utc_datetime do
      public? true
    end

    attribute :decision_profile, :map do
      allow_nil? false
      default %{}
      public? true
    end

    attribute :evidence_bundle_id, :uuid do
      public? true
    end

    attribute :reviewer_actor, :map do
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

    belongs_to :run, Mezzanine.Runs.Run do
      attribute_type :uuid
      public? true
    end

    belongs_to :evidence_bundle, Mezzanine.Evidence.EvidenceBundle do
      source_attribute :evidence_bundle_id
      attribute_type :uuid
      public? true
    end

    has_many :review_decisions, Mezzanine.Review.ReviewDecision do
      destination_attribute :review_unit_id
    end

    has_many :waivers, Mezzanine.Review.Waiver do
      destination_attribute :review_unit_id
    end

    has_many :escalations, Mezzanine.Review.Escalation do
      destination_attribute :review_unit_id
    end
  end
end
