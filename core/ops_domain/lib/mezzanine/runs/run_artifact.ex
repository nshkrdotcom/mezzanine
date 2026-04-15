defmodule Mezzanine.Runs.RunArtifact do
  @moduledoc false
  use Ash.Resource,
    domain: Mezzanine.Runs,
    data_layer: AshPostgres.DataLayer,
    authorizers: [Ash.Policy.Authorizer]

  postgres do
    table "run_artifacts"
    repo Mezzanine.OpsDomain.Repo
  end

  code_interface do
    define :record_artifact, action: :record_artifact
    define :list_for_run, action: :list_for_run, args: [:run_id]
    define :verify_artifact, action: :verify_artifact
    define :reject_artifact, action: :reject_artifact
  end

  actions do
    defaults []

    read :read do
      primary? true
    end

    create :record_artifact do
      accept [:run_id, :kind, :ref, :metadata]
      change set_attribute(:status, :pending)
    end

    read :list_for_run do
      argument :run_id, :uuid, allow_nil?: false
      filter expr(run_id == ^arg(:run_id))
    end

    update :verify_artifact do
      accept []
      require_atomic? false
      change set_attribute(:status, :verified)
    end

    update :reject_artifact do
      accept []
      require_atomic? false
      change set_attribute(:status, :rejected)
    end
  end

  policies do
    policy action_type(:read) do
      authorize_if expr(tenant_id == ^actor(:tenant_id))
    end

    policy action(:record_artifact) do
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

    attribute :run_id, :uuid do
      allow_nil? false
      public? true
    end

    attribute :kind, :atom do
      allow_nil? false
      constraints one_of: [:pr, :diff, :test_result, :log_excerpt, :file, :log]
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
    belongs_to :run, Mezzanine.Runs.Run do
      attribute_type :uuid
      allow_nil? false
      public? true
    end
  end
end
