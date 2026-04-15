defmodule Mezzanine.Control.ControlSession do
  @moduledoc false
  use Ash.Resource,
    domain: Mezzanine.Control,
    data_layer: AshPostgres.DataLayer,
    authorizers: [Ash.Policy.Authorizer]

  postgres do
    table "control_sessions"
    repo Mezzanine.OpsDomain.Repo
  end

  code_interface do
    define :open, action: :open
    define :list_for_program, action: :list_for_program, args: [:program_id]
    define :pause, action: :pause
    define :resume, action: :resume
    define :apply_grant_override, action: :apply_grant_override
  end

  actions do
    defaults []

    read :read do
      primary? true
    end

    create :open do
      accept [:program_id, :work_object_id]
      change set_attribute(:status, :active)
      change set_attribute(:current_mode, :normal)
    end

    read :list_for_program do
      argument :program_id, :uuid, allow_nil?: false
      filter expr(program_id == ^arg(:program_id))
    end

    update :pause do
      accept []
      require_atomic? false
      change set_attribute(:current_mode, :paused)
    end

    update :resume do
      accept []
      require_atomic? false
      change set_attribute(:current_mode, :normal)
    end

    update :apply_grant_override do
      accept [:active_override_set]
      require_atomic? false
    end
  end

  policies do
    policy action_type(:read) do
      authorize_if expr(tenant_id == ^actor(:tenant_id))
    end

    policy action(:open) do
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

    attribute :program_id, :uuid do
      allow_nil? false
      public? true
    end

    attribute :work_object_id, :uuid do
      allow_nil? false
      public? true
    end

    attribute :status, :atom do
      allow_nil? false
      default :active
      constraints one_of: [:active, :closed]
      public? true
    end

    attribute :current_mode, :atom do
      allow_nil? false
      default :normal
      constraints one_of: [:normal, :paused, :review_gate, :operator_attention, :escalated]
      public? true
    end

    attribute :active_override_set, :map do
      allow_nil? false
      default %{}
      public? true
    end

    attribute :last_operator_action_at, :utc_datetime do
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

    has_many :run_series, Mezzanine.Runs.RunSeries do
      destination_attribute :control_session_id
    end

    has_many :operator_interventions, Mezzanine.Control.OperatorIntervention do
      destination_attribute :control_session_id
    end
  end

  identities do
    identity :unique_work_object_session, [:work_object_id]
  end
end
