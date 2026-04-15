defmodule Mezzanine.Work.WorkObject do
  @moduledoc false
  use Ash.Resource,
    domain: Mezzanine.Work,
    data_layer: AshPostgres.DataLayer,
    authorizers: [Ash.Policy.Authorizer]

  postgres do
    table "work_objects"
    repo Mezzanine.OpsDomain.Repo
  end

  code_interface do
    define :ingest, action: :ingest
    define :list_for_program, action: :list_for_program, args: [:program_id]

    define :by_program_and_external_ref,
      action: :by_program_and_external_ref,
      args: [:program_id, :external_ref]

    define :refresh_intake, action: :refresh_intake
    define :compile_plan, action: :compile_plan
    define :mark_planned
    define :block, action: :block
    define :unblock, action: :unblock
    define :mark_running
    define :mark_terminal
  end

  actions do
    defaults []

    read :read do
      primary? true
    end

    create :ingest do
      accept [
        :program_id,
        :work_class_id,
        :external_ref,
        :title,
        :description,
        :priority,
        :source_kind,
        :payload,
        :normalized_payload
      ]

      change set_attribute(:status, :pending)
    end

    read :list_for_program do
      argument :program_id, :uuid, allow_nil?: false
      filter expr(program_id == ^arg(:program_id))
    end

    read :by_program_and_external_ref do
      argument :program_id, :uuid, allow_nil?: false
      argument :external_ref, :string, allow_nil?: false
      get? true
      filter expr(program_id == ^arg(:program_id) and external_ref == ^arg(:external_ref))
    end

    update :refresh_intake do
      accept [
        :work_class_id,
        :external_ref,
        :title,
        :description,
        :priority,
        :source_kind,
        :payload,
        :normalized_payload
      ]

      require_atomic? false
    end

    update :mark_planned do
      accept [:current_plan_id]
      require_atomic? false
      change set_attribute(:status, :planned)
    end

    update :block do
      accept [:blocked_by_work_id]
      require_atomic? false
      change set_attribute(:status, :blocked)
    end

    update :unblock do
      accept []
      require_atomic? false

      change fn changeset, _context ->
        restored_status =
          if Ash.Changeset.get_data(changeset, :current_plan_id) do
            :planned
          else
            :pending
          end

        changeset
        |> Ash.Changeset.change_attribute(:blocked_by_work_id, nil)
        |> Ash.Changeset.change_attribute(:status, restored_status)
      end
    end

    update :compile_plan do
      argument :policy_bundle_id, :uuid
      require_atomic? false

      change after_action(fn changeset, work_object, context ->
               tenant = changeset.tenant || work_object.tenant_id

               attrs =
                 %{work_object_id: work_object.id}
                 |> maybe_put_policy_bundle_id(
                   Ash.Changeset.get_argument(changeset, :policy_bundle_id)
                 )

               case Mezzanine.Work.WorkPlan
                    |> Ash.Changeset.for_create(:compile, attrs)
                    |> Ash.Changeset.set_tenant(tenant)
                    |> Ash.create(
                      actor: context.actor,
                      authorize?: false,
                      domain: Mezzanine.Work
                    ) do
                 {:ok, plan} ->
                   work_object
                   |> Ash.Changeset.for_update(:mark_planned, %{current_plan_id: plan.id})
                   |> Ash.Changeset.set_tenant(tenant)
                   |> Ash.update(
                     actor: context.actor,
                     authorize?: false,
                     domain: Mezzanine.Work
                   )

                 error ->
                   error
               end
             end)
    end

    update :mark_running do
      accept []
      require_atomic? false
      change set_attribute(:status, :running)
    end

    update :mark_terminal do
      argument :status, :atom,
        allow_nil?: false,
        constraints: [one_of: [:completed, :cancelled, :failed]]

      require_atomic? false
      change set_attribute(:status, arg(:status))
    end
  end

  policies do
    policy action_type(:read) do
      authorize_if expr(tenant_id == ^actor(:tenant_id))
    end

    policy action(:ingest) do
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

    attribute :work_class_id, :uuid do
      allow_nil? false
      public? true
    end

    attribute :external_ref, :string do
      public? true
    end

    attribute :title, :string do
      allow_nil? false
      public? true
    end

    attribute :description, :string do
      public? true
    end

    attribute :status, :atom do
      allow_nil? false
      default :pending

      constraints one_of: [
                    :pending,
                    :planning,
                    :planned,
                    :running,
                    :awaiting_review,
                    :completed,
                    :cancelled,
                    :failed,
                    :blocked
                  ]

      public? true
    end

    attribute :priority, :integer do
      allow_nil? false
      default 50
      public? true
    end

    attribute :source_kind, :string do
      public? true
    end

    attribute :payload, :map do
      allow_nil? false
      default %{}
      public? true
    end

    attribute :normalized_payload, :map do
      allow_nil? false
      default %{}
      public? true
    end

    attribute :current_plan_id, :uuid do
      public? true
    end

    attribute :blocked_by_work_id, :uuid do
      public? true
    end

    attribute :lease_owner, :string do
      public? true
    end

    attribute :lease_expires_at, :utc_datetime do
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

    belongs_to :work_class, Mezzanine.Work.WorkClass do
      attribute_type :uuid
      allow_nil? false
      public? true
    end

    belongs_to :current_plan, Mezzanine.Work.WorkPlan do
      source_attribute :current_plan_id
      attribute_type :uuid
      public? true
    end

    has_many :work_plans, Mezzanine.Work.WorkPlan do
      destination_attribute :work_object_id
    end

    has_many :run_series, Mezzanine.Runs.RunSeries do
      destination_attribute :work_object_id
    end

    has_many :review_units, Mezzanine.Review.ReviewUnit do
      destination_attribute :work_object_id
    end

    has_many :evidence_bundles, Mezzanine.Evidence.EvidenceBundle do
      destination_attribute :work_object_id
    end

    has_one :control_session, Mezzanine.Control.ControlSession do
      destination_attribute :work_object_id
    end
  end

  identities do
    identity :unique_external_ref_per_program, [:program_id, :external_ref]
  end

  defp maybe_put_policy_bundle_id(attrs, nil), do: attrs

  defp maybe_put_policy_bundle_id(attrs, policy_bundle_id),
    do: Map.put(attrs, :policy_bundle_id, policy_bundle_id)
end
