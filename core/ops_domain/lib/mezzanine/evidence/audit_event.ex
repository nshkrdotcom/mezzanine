defmodule Mezzanine.Evidence.AuditEvent do
  @moduledoc false
  use Ash.Resource,
    domain: Mezzanine.Evidence,
    data_layer: AshPostgres.DataLayer,
    authorizers: [Ash.Policy.Authorizer]

  postgres do
    table("audit_events")
    repo(Mezzanine.OpsDomain.Repo)
  end

  code_interface do
    define(:record, action: :record)
    define(:list_for_work_object, action: :list_for_work_object, args: [:work_object_id])
  end

  actions do
    defaults([])

    read :read do
      primary?(true)
    end

    create :record do
      accept([
        :program_id,
        :work_object_id,
        :run_id,
        :review_unit_id,
        :event_kind,
        :actor_kind,
        :actor_ref,
        :payload,
        :occurred_at
      ])
    end

    read :list_for_work_object do
      argument(:work_object_id, :uuid, allow_nil?: false)
      filter(expr(work_object_id == ^arg(:work_object_id)))
    end
  end

  policies do
    policy action_type(:read) do
      authorize_if(expr(tenant_id == ^actor(:tenant_id)))
    end

    policy action(:record) do
      authorize_if(actor_present())
    end
  end

  multitenancy do
    strategy(:attribute)
    attribute(:tenant_id)
    global?(false)
  end

  attributes do
    uuid_primary_key(:id)

    attribute :tenant_id, :string do
      allow_nil?(false)
      public?(true)
    end

    attribute :program_id, :uuid do
      allow_nil?(false)
      public?(true)
    end

    attribute :work_object_id, :uuid do
      public?(true)
    end

    attribute :run_id, :uuid do
      public?(true)
    end

    attribute :review_unit_id, :uuid do
      public?(true)
    end

    attribute :event_kind, :atom do
      allow_nil?(false)

      constraints(
        one_of: [
          :work_ingested,
          :work_planned,
          :work_blocked,
          :work_completed,
          :run_scheduled,
          :run_started,
          :run_completed,
          :run_failed,
          :review_created,
          :review_accepted,
          :review_rejected,
          :review_waived,
          :review_conflict_attempt,
          :escalation_raised,
          :escalation_resolved,
          :operator_paused,
          :operator_resumed,
          :operator_cancelled,
          :grant_override_applied,
          :replan_requested
        ]
      )

      public?(true)
    end

    attribute :actor_kind, :atom do
      constraints(one_of: [:human, :agent, :system])
      public?(true)
    end

    attribute :actor_ref, :string do
      public?(true)
    end

    attribute :payload, :map do
      allow_nil?(false)
      default(%{})
      public?(true)
    end

    attribute :occurred_at, :utc_datetime do
      allow_nil?(false)
      public?(true)
    end
  end

  relationships do
    belongs_to :program, Mezzanine.Programs.Program do
      attribute_type(:uuid)
      allow_nil?(false)
      public?(true)
    end

    belongs_to :work_object, Mezzanine.Work.WorkObject do
      attribute_type(:uuid)
      public?(true)
    end

    belongs_to :run, Mezzanine.Runs.Run do
      attribute_type(:uuid)
      public?(true)
    end

    belongs_to :review_unit, Mezzanine.Review.ReviewUnit do
      attribute_type(:uuid)
      public?(true)
    end
  end
end
