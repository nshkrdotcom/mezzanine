defmodule Mezzanine.Control.OperatorIntervention do
  @moduledoc false
  use Ash.Resource,
    domain: Mezzanine.Control,
    data_layer: AshPostgres.DataLayer,
    authorizers: [Ash.Policy.Authorizer]

  postgres do
    table "operator_interventions"
    repo Mezzanine.OpsDomain.Repo
  end

  code_interface do
    define :record_intervention, action: :record_intervention

    define :list_for_control_session,
      action: :list_for_control_session,
      args: [:control_session_id]
  end

  actions do
    defaults []

    read :read do
      primary? true
    end

    create :record_intervention do
      accept [:control_session_id, :operator_ref, :intervention_kind, :payload, :occurred_at]
    end

    read :list_for_control_session do
      argument :control_session_id, :uuid, allow_nil?: false
      filter expr(control_session_id == ^arg(:control_session_id))
    end
  end

  policies do
    policy action_type(:read) do
      authorize_if expr(tenant_id == ^actor(:tenant_id))
    end

    policy action(:record_intervention) do
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

    attribute :control_session_id, :uuid do
      allow_nil? false
      public? true
    end

    attribute :operator_ref, :string do
      allow_nil? false
      public? true
    end

    attribute :intervention_kind, :atom do
      allow_nil? false
      constraints one_of: [:pause, :resume, :cancel, :replan, :grant_override, :force_review]
      public? true
    end

    attribute :payload, :map do
      allow_nil? false
      default %{}
      public? true
    end

    attribute :occurred_at, :utc_datetime do
      allow_nil? false
      public? true
    end

    timestamps()
  end

  relationships do
    belongs_to :control_session, Mezzanine.Control.ControlSession do
      attribute_type :uuid
      allow_nil? false
      public? true
    end
  end
end
