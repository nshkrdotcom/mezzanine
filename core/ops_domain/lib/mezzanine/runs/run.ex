defmodule Mezzanine.Runs.Run do
  @moduledoc false
  use Ash.Resource,
    domain: Mezzanine.Runs,
    data_layer: AshPostgres.DataLayer,
    authorizers: [Ash.Policy.Authorizer]

  postgres do
    table "runs"
    repo Mezzanine.OpsDomain.Repo
  end

  code_interface do
    define :schedule, action: :schedule
    define :list_for_series, action: :list_for_series, args: [:run_series_id]
    define :record_started, action: :record_started
    define :record_completed, action: :record_completed
    define :record_failed, action: :record_failed
    define :record_cancelled, action: :record_cancelled
    define :mark_stalled, action: :mark_stalled
  end

  actions do
    defaults []

    read :read do
      primary? true
    end

    create :schedule do
      accept [:run_series_id, :attempt, :runtime_profile, :placement_profile_id, :grant_profile]
      change set_attribute(:status, :scheduled)
    end

    read :list_for_series do
      argument :run_series_id, :uuid, allow_nil?: false
      filter expr(run_series_id == ^arg(:run_series_id))
    end

    update :record_started do
      accept [:raw_runtime_ref, :started_at]
      require_atomic? false
      change set_attribute(:status, :running)
    end

    update :record_completed do
      accept [:completed_at, :result_summary, :token_usage, :evidence_bundle_id]
      require_atomic? false
      change set_attribute(:status, :completed)
    end

    update :record_failed do
      accept [:completed_at, :result_summary, :evidence_bundle_id]
      require_atomic? false
      change set_attribute(:status, :failed)
    end

    update :record_cancelled do
      accept [:completed_at, :result_summary]
      require_atomic? false
      change set_attribute(:status, :cancelled)
    end

    update :mark_stalled do
      accept []
      require_atomic? false
      change set_attribute(:status, :stalled)
    end
  end

  policies do
    policy action_type(:read) do
      authorize_if expr(tenant_id == ^actor(:tenant_id))
    end

    policy action(:schedule) do
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

    attribute :run_series_id, :uuid do
      allow_nil? false
      public? true
    end

    attribute :attempt, :integer do
      allow_nil? false
      default 1
      public? true
    end

    attribute :status, :atom do
      allow_nil? false
      default :pending

      constraints one_of: [
                    :pending,
                    :scheduled,
                    :running,
                    :completed,
                    :failed,
                    :cancelled,
                    :stalled
                  ]

      public? true
    end

    attribute :runtime_profile, :map do
      allow_nil? false
      default %{}
      public? true
    end

    attribute :placement_profile_id, :uuid do
      public? true
    end

    attribute :grant_profile, :map do
      allow_nil? false
      default %{}
      public? true
    end

    attribute :started_at, :utc_datetime do
      public? true
    end

    attribute :completed_at, :utc_datetime do
      public? true
    end

    attribute :result_summary, :string do
      public? true
    end

    attribute :raw_runtime_ref, :string do
      public? true
    end

    attribute :token_usage, :map do
      allow_nil? false
      default %{}
      public? true
    end

    attribute :evidence_bundle_id, :uuid do
      public? true
    end

    timestamps()
  end

  relationships do
    belongs_to :run_series, Mezzanine.Runs.RunSeries do
      attribute_type :uuid
      allow_nil? false
      public? true
    end

    belongs_to :evidence_bundle, Mezzanine.Evidence.EvidenceBundle do
      source_attribute :evidence_bundle_id
      attribute_type :uuid
      public? true
    end

    has_many :run_grants, Mezzanine.Runs.RunGrant do
      destination_attribute :run_id
    end

    has_many :run_artifacts, Mezzanine.Runs.RunArtifact do
      destination_attribute :run_id
    end

    has_many :review_units, Mezzanine.Review.ReviewUnit do
      destination_attribute :run_id
    end
  end
end
