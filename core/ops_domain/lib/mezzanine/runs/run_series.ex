defmodule Mezzanine.Runs.RunSeries do
  @moduledoc false
  use Ash.Resource,
    domain: Mezzanine.Runs,
    data_layer: AshPostgres.DataLayer,
    authorizers: [Ash.Policy.Authorizer]

  postgres do
    table "run_series"
    repo Mezzanine.OpsDomain.Repo
  end

  code_interface do
    define :open_series, action: :open_series
    define :list_for_work_object, action: :list_for_work_object, args: [:work_object_id]
    define :attach_current_run, action: :attach_current_run
    define :close_series, action: :close_series
  end

  actions do
    defaults []

    read :read do
      primary? true
    end

    create :open_series do
      accept [:work_object_id, :control_session_id]
      change set_attribute(:status, :active)
    end

    read :list_for_work_object do
      argument :work_object_id, :uuid, allow_nil?: false
      filter expr(work_object_id == ^arg(:work_object_id))
    end

    update :attach_current_run do
      accept [:current_run_id]
      require_atomic? false
    end

    update :close_series do
      argument :status, :atom,
        allow_nil?: false,
        constraints: [one_of: [:completed, :failed, :cancelled]]

      require_atomic? false
      change set_attribute(:status, arg(:status))
    end
  end

  policies do
    policy action_type(:read) do
      authorize_if expr(tenant_id == ^actor(:tenant_id))
    end

    policy action(:open_series) do
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

    attribute :sequence, :integer do
      allow_nil? false
      default 1
      public? true
    end

    attribute :status, :atom do
      allow_nil? false
      default :active
      constraints one_of: [:active, :completed, :failed, :cancelled]
      public? true
    end

    attribute :current_run_id, :uuid do
      public? true
    end

    attribute :control_session_id, :uuid do
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

    belongs_to :control_session, Mezzanine.Control.ControlSession do
      source_attribute :control_session_id
      attribute_type :uuid
      public? true
    end

    belongs_to :current_run, Mezzanine.Runs.Run do
      source_attribute :current_run_id
      attribute_type :uuid
      public? true
    end

    has_many :runs, Mezzanine.Runs.Run do
      destination_attribute :run_series_id
    end
  end
end
