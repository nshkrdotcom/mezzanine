defmodule Mezzanine.Evidence.TimelineProjection do
  @moduledoc false
  use Ash.Resource,
    domain: Mezzanine.Evidence,
    data_layer: AshPostgres.DataLayer,
    authorizers: [Ash.Policy.Authorizer]

  postgres do
    table "timeline_projections"
    repo Mezzanine.OpsDomain.Repo
  end

  code_interface do
    define :project, action: :project
    define :refresh, action: :refresh
  end

  actions do
    defaults []

    read :read do
      primary? true
    end

    create :project do
      accept [:work_object_id, :timeline, :last_event_at, :projected_at]
    end

    update :refresh do
      accept [:timeline, :last_event_at, :projected_at]
      require_atomic? false
    end
  end

  policies do
    policy action_type(:read) do
      authorize_if expr(tenant_id == ^actor(:tenant_id))
    end

    policy action(:project) do
      authorize_if actor_present()
    end

    policy action(:refresh) do
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

    attribute :timeline, {:array, :map} do
      allow_nil? false
      default []
      public? true
    end

    attribute :last_event_at, :utc_datetime do
      public? true
    end

    attribute :projected_at, :utc_datetime do
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
  end

  identities do
    identity :unique_work_object_projection, [:work_object_id]
  end
end
