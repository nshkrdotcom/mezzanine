defmodule Mezzanine.Evidence.EvidenceBundle do
  @moduledoc false
  use Ash.Resource,
    domain: Mezzanine.Evidence,
    data_layer: AshPostgres.DataLayer,
    authorizers: [Ash.Policy.Authorizer]

  postgres do
    table "evidence_bundles"
    repo Mezzanine.OpsDomain.Repo
  end

  code_interface do
    define :assemble, action: :assemble
    define :list_for_work_object, action: :list_for_work_object, args: [:work_object_id]
    define :mark_ready, action: :mark_ready
  end

  actions do
    defaults []

    read :read do
      primary? true
    end

    create :assemble do
      accept [
        :program_id,
        :work_object_id,
        :run_id,
        :summary,
        :evidence_manifest,
        :completeness_status,
        :assembled_at
      ]

      change set_attribute(:status, :assembling)
    end

    read :list_for_work_object do
      argument :work_object_id, :uuid, allow_nil?: false
      filter expr(work_object_id == ^arg(:work_object_id))
    end

    update :mark_ready do
      accept [:summary, :evidence_manifest, :completeness_status, :assembled_at]
      require_atomic? false
      change set_attribute(:status, :ready)
    end
  end

  policies do
    policy action_type(:read) do
      authorize_if expr(tenant_id == ^actor(:tenant_id))
    end

    policy action(:assemble) do
      authorize_if actor_present()
    end

    policy action(:mark_ready) do
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

    attribute :work_object_id, :uuid do
      allow_nil? false
      public? true
    end

    attribute :run_id, :uuid do
      public? true
    end

    attribute :status, :atom do
      allow_nil? false
      default :assembling
      constraints one_of: [:assembling, :ready]
      public? true
    end

    attribute :summary, :string do
      public? true
    end

    attribute :evidence_manifest, :map do
      allow_nil? false
      default %{}
      public? true
    end

    attribute :completeness_status, :map do
      allow_nil? false
      default %{}
      public? true
    end

    attribute :assembled_at, :utc_datetime do
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

    belongs_to :work_object, Mezzanine.Work.WorkObject do
      attribute_type :uuid
      allow_nil? false
      public? true
    end

    belongs_to :run, Mezzanine.Runs.Run do
      attribute_type :uuid
      public? true
    end

    has_many :evidence_items, Mezzanine.Evidence.EvidenceItem do
      destination_attribute :evidence_bundle_id
    end
  end
end
