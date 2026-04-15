defmodule Mezzanine.Evidence.EvidenceItem do
  @moduledoc false
  use Ash.Resource,
    domain: Mezzanine.Evidence,
    data_layer: AshPostgres.DataLayer,
    authorizers: [Ash.Policy.Authorizer]

  postgres do
    table "evidence_items"
    repo Mezzanine.OpsDomain.Repo
  end

  code_interface do
    define :record_item, action: :record_item
    define :verify_item, action: :verify_item
    define :reject_item, action: :reject_item
  end

  actions do
    defaults []

    read :read do
      primary? true
    end

    create :record_item do
      accept [:evidence_bundle_id, :kind, :ref, :metadata]
      change set_attribute(:status, :pending)
    end

    update :verify_item do
      accept []
      require_atomic? false
      change set_attribute(:status, :verified)
    end

    update :reject_item do
      accept []
      require_atomic? false
      change set_attribute(:status, :rejected)
    end
  end

  policies do
    policy action_type(:read) do
      authorize_if expr(tenant_id == ^actor(:tenant_id))
    end

    policy action(:record_item) do
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

    attribute :evidence_bundle_id, :uuid do
      allow_nil? false
      public? true
    end

    attribute :kind, :atom do
      allow_nil? false

      constraints one_of: [
                    :run_transcript,
                    :pr_link,
                    :test_result,
                    :diff,
                    :ci_status,
                    :check_result,
                    :log
                  ]

      public? true
    end

    attribute :ref, :string do
      allow_nil? false
      public? true
    end

    attribute :metadata, :map do
      allow_nil? false
      default %{}
      public? true
    end

    attribute :status, :atom do
      allow_nil? false
      default :pending
      constraints one_of: [:pending, :verified, :rejected]
      public? true
    end

    timestamps()
  end

  relationships do
    belongs_to :evidence_bundle, Mezzanine.Evidence.EvidenceBundle do
      attribute_type :uuid
      allow_nil? false
      public? true
    end
  end
end
