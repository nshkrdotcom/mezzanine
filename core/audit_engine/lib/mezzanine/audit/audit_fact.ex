defmodule Mezzanine.Audit.AuditFact do
  @moduledoc """
  Durable audit fact written in the same transaction as significant substrate state changes.
  """

  use Ash.Resource,
    domain: Mezzanine.Audit,
    data_layer: AshPostgres.DataLayer

  postgres do
    table("audit_facts")
    repo(Mezzanine.Audit.Repo)

    custom_indexes do
      index([:installation_id, :trace_id, :occurred_at])
      index([:causation_id])
      index([:installation_id, :idempotency_key], unique: true)
    end
  end

  code_interface do
    define(:record, action: :record)
    define(:list_trace, action: :list_trace, args: [:installation_id, :trace_id])

    define(:by_idempotency_key,
      action: :by_idempotency_key,
      args: [:installation_id, :idempotency_key]
    )
  end

  actions do
    defaults([:read])

    create :record do
      accept([
        :installation_id,
        :subject_id,
        :execution_id,
        :decision_id,
        :evidence_id,
        :trace_id,
        :causation_id,
        :fact_kind,
        :actor_ref,
        :payload,
        :occurred_at,
        :idempotency_key
      ])
    end

    read :list_trace do
      argument(:installation_id, :string, allow_nil?: false)
      argument(:trace_id, :string, allow_nil?: false)
      filter(expr(installation_id == ^arg(:installation_id) and trace_id == ^arg(:trace_id)))
      prepare(build(sort: [occurred_at: :asc]))
    end

    read :by_idempotency_key do
      argument(:installation_id, :string, allow_nil?: false)
      argument(:idempotency_key, :string, allow_nil?: false)

      filter(
        expr(
          installation_id == ^arg(:installation_id) and
            idempotency_key == ^arg(:idempotency_key)
        )
      )

      prepare(build(limit: 1))
    end
  end

  attributes do
    uuid_primary_key(:id)

    attribute :installation_id, :string do
      allow_nil?(false)
      public?(true)
    end

    attribute :subject_id, :string do
      public?(true)
    end

    attribute :execution_id, :string do
      public?(true)
    end

    attribute :decision_id, :string do
      public?(true)
    end

    attribute :evidence_id, :string do
      public?(true)
    end

    attribute :trace_id, :string do
      allow_nil?(false)
      public?(true)
    end

    attribute :causation_id, :string do
      public?(true)
    end

    attribute :fact_kind, :atom do
      allow_nil?(false)
      public?(true)
    end

    attribute :actor_ref, :map do
      allow_nil?(false)
      default(%{})
      public?(true)
    end

    attribute :payload, :map do
      allow_nil?(false)
      default(%{})
      public?(true)
    end

    attribute :occurred_at, :utc_datetime_usec do
      allow_nil?(false)
      public?(true)
    end

    attribute :idempotency_key, :string do
      public?(true)
    end

    timestamps()
  end
end
