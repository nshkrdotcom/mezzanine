defmodule Mezzanine.Audit.MemoryProofTokenRecord do
  @moduledoc """
  Durable proof-token row keyed by proof id and tenant trace.
  """

  use Ash.Resource,
    domain: Mezzanine.Audit,
    data_layer: AshPostgres.DataLayer

  @kinds Mezzanine.Audit.MemoryProofToken.kinds()

  postgres do
    table("memory_proof_tokens")
    repo(Mezzanine.Audit.Repo)

    custom_indexes do
      index([:tenant_ref, :trace_id, :t_event])
      index([:tenant_ref, :proof_hash])
      index([:proof_id], unique: true)
    end
  end

  code_interface do
    define(:store, action: :store)
    define(:by_proof_id, action: :by_proof_id, args: [:proof_id])
    define(:list_trace, action: :list_trace, args: [:tenant_ref, :trace_id])
  end

  actions do
    defaults([:read])

    create :store do
      accept([
        :proof_id,
        :kind,
        :tenant_ref,
        :installation_id,
        :subject_id,
        :execution_id,
        :user_ref,
        :agent_ref,
        :t_event,
        :epoch_used,
        :policy_refs,
        :fragment_ids,
        :transform_hashes,
        :access_projection_hashes,
        :proof_hash,
        :trace_id,
        :parent_fragment_id,
        :child_fragment_id,
        :evidence_refs,
        :governance_decision_ref,
        :metadata
      ])
    end

    read :by_proof_id do
      argument(:proof_id, :string, allow_nil?: false)
      get?(true)
      filter(expr(proof_id == ^arg(:proof_id)))
    end

    read :list_trace do
      argument(:tenant_ref, :string, allow_nil?: false)
      argument(:trace_id, :string, allow_nil?: false)
      filter(expr(tenant_ref == ^arg(:tenant_ref) and trace_id == ^arg(:trace_id)))
      prepare(build(sort: [t_event: :asc, proof_id: :asc]))
    end
  end

  attributes do
    uuid_primary_key(:id)

    attribute :proof_id, :string do
      allow_nil?(false)
      public?(true)
    end

    attribute :kind, :atom do
      allow_nil?(false)
      constraints(one_of: @kinds)
      public?(true)
    end

    attribute :tenant_ref, :string do
      allow_nil?(false)
      public?(true)
    end

    attribute :installation_id, :string do
      public?(true)
    end

    attribute :subject_id, :string do
      public?(true)
    end

    attribute :execution_id, :string do
      public?(true)
    end

    attribute :user_ref, :string do
      public?(true)
    end

    attribute :agent_ref, :string do
      public?(true)
    end

    attribute :t_event, :utc_datetime_usec do
      allow_nil?(false)
      public?(true)
    end

    attribute :epoch_used, :integer do
      allow_nil?(false)
      public?(true)
    end

    attribute :policy_refs, {:array, :map} do
      allow_nil?(false)
      default([])
      public?(true)
    end

    attribute :fragment_ids, {:array, :string} do
      allow_nil?(false)
      default([])
      public?(true)
    end

    attribute :transform_hashes, {:array, :string} do
      allow_nil?(false)
      default([])
      public?(true)
    end

    attribute :access_projection_hashes, {:array, :string} do
      allow_nil?(false)
      default([])
      public?(true)
    end

    attribute :proof_hash, :string do
      allow_nil?(false)
      public?(true)
    end

    attribute :trace_id, :string do
      allow_nil?(false)
      public?(true)
    end

    attribute :parent_fragment_id, :string do
      public?(true)
    end

    attribute :child_fragment_id, :string do
      public?(true)
    end

    attribute :evidence_refs, {:array, :map} do
      allow_nil?(false)
      default([])
      public?(true)
    end

    attribute :governance_decision_ref, :map do
      public?(true)
    end

    attribute :metadata, :map do
      allow_nil?(false)
      default(%{})
      public?(true)
    end

    timestamps()
  end

  identities do
    identity(:unique_proof_id, [:proof_id])
  end
end
