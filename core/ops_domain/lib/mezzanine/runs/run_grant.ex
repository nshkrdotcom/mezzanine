defmodule Mezzanine.Runs.RunGrant do
  @moduledoc false
  use Ash.Resource,
    domain: Mezzanine.Runs,
    data_layer: AshPostgres.DataLayer,
    authorizers: [Ash.Policy.Authorizer]

  postgres do
    table "run_grants"
    repo Mezzanine.OpsDomain.Repo
  end

  code_interface do
    define :grant, action: :grant
    define :list_for_run, action: :list_for_run, args: [:run_id]
  end

  actions do
    defaults []

    read :read do
      primary? true
    end

    create :grant do
      accept [:run_id, :grant_kind, :scope, :approval_class]
      change set_attribute(:status, :active)
    end

    read :list_for_run do
      argument :run_id, :uuid, allow_nil?: false
      filter expr(run_id == ^arg(:run_id))
    end
  end

  policies do
    policy action_type(:read) do
      authorize_if expr(tenant_id == ^actor(:tenant_id))
    end

    policy action(:grant) do
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

    attribute :run_id, :uuid do
      allow_nil? false
      public? true
    end

    attribute :grant_kind, :atom do
      allow_nil? false

      constraints one_of: [
                    :connector,
                    :repo_write,
                    :tracker_mutation,
                    :model_runtime,
                    :env_secret,
                    :tool
                  ]

      public? true
    end

    attribute :scope, :map do
      allow_nil? false
      default %{}
      public? true
    end

    attribute :approval_class, :atom do
      constraints one_of: [:pre_approved, :requires_review, :operator_only]
      public? true
    end

    attribute :status, :atom do
      allow_nil? false
      default :active
      constraints one_of: [:active, :revoked]
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
