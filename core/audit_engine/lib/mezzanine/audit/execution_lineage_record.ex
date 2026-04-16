defmodule Mezzanine.Audit.ExecutionLineageRecord do
  @moduledoc """
  Durable bridge-lineage record keyed by substrate execution id.

  This keeps public substrate lookup keys and lower-owned identifiers stable as
  reconciliation fills in more downstream execution facts.
  """

  use Ash.Resource,
    domain: Mezzanine.AuditDomain,
    data_layer: AshPostgres.DataLayer

  postgres do
    table("execution_lineage_records")
    repo(Mezzanine.Audit.Repo)

    custom_indexes do
      index([:installation_id, :trace_id])
      index([:execution_id], unique: true)
    end
  end

  code_interface do
    define(:store, action: :store)
    define(:by_execution_id, action: :by_execution_id, args: [:execution_id])
    define(:list_trace, action: :list_trace, args: [:installation_id, :trace_id])
  end

  actions do
    defaults([:read])

    create :store do
      accept([
        :trace_id,
        :causation_id,
        :installation_id,
        :subject_id,
        :execution_id,
        :dispatch_outbox_entry_id,
        :citadel_request_id,
        :citadel_submission_id,
        :ji_submission_key,
        :lower_run_id,
        :lower_attempt_id,
        :artifact_refs
      ])

      upsert?(true)
      upsert_identity(:unique_execution_id)

      upsert_fields([
        :trace_id,
        :causation_id,
        :installation_id,
        :subject_id,
        :dispatch_outbox_entry_id,
        :citadel_request_id,
        :citadel_submission_id,
        :ji_submission_key,
        :lower_run_id,
        :lower_attempt_id,
        :artifact_refs
      ])
    end

    read :by_execution_id do
      argument(:execution_id, :string, allow_nil?: false)
      get?(true)
      filter(expr(execution_id == ^arg(:execution_id)))
    end

    read :list_trace do
      argument(:installation_id, :string, allow_nil?: false)
      argument(:trace_id, :string, allow_nil?: false)
      filter(expr(installation_id == ^arg(:installation_id) and trace_id == ^arg(:trace_id)))
      prepare(build(sort: [execution_id: :asc]))
    end
  end

  attributes do
    uuid_primary_key(:id)

    attribute :trace_id, :string do
      allow_nil?(false)
      public?(true)
    end

    attribute :causation_id, :string do
      public?(true)
    end

    attribute :installation_id, :string do
      allow_nil?(false)
      public?(true)
    end

    attribute :subject_id, :string do
      allow_nil?(false)
      public?(true)
    end

    attribute :execution_id, :string do
      allow_nil?(false)
      public?(true)
    end

    attribute :dispatch_outbox_entry_id, :string do
      public?(true)
    end

    attribute :citadel_request_id, :string do
      public?(true)
    end

    attribute :citadel_submission_id, :string do
      public?(true)
    end

    attribute :ji_submission_key, :string do
      public?(true)
    end

    attribute :lower_run_id, :string do
      public?(true)
    end

    attribute :lower_attempt_id, :string do
      public?(true)
    end

    attribute :artifact_refs, {:array, :string} do
      allow_nil?(false)
      default([])
      public?(true)
    end

    timestamps()
  end

  identities do
    identity(:unique_execution_id, [:execution_id])
  end
end
