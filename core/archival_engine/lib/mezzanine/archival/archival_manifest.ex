defmodule Mezzanine.Archival.ArchivalManifest do
  @moduledoc """
  Durable archival manifest carrying the terminal hot-graph snapshot and
  cold-storage completion metadata.
  """

  use Ash.Resource,
    domain: Mezzanine.Archival,
    data_layer: AshPostgres.DataLayer

  alias Mezzanine.Archival.{CountdownPolicy, Graph, OffloadPlan}

  postgres do
    table("archival_manifests")
    repo(Mezzanine.Archival.Repo)

    custom_indexes do
      index([:manifest_ref], unique: true)
      index([:installation_id, :status, :due_at])
      index([:installation_id, :subject_id, :terminal_at])
    end
  end

  code_interface do
    define(:plan, action: :plan)
    define(:complete, action: :complete)
    define(:by_manifest_ref, action: :by_manifest_ref, args: [:manifest_ref])

    define(:for_subject,
      action: :for_subject,
      args: [:installation_id, :subject_id]
    )

    define(:due_for_installation,
      action: :due_for_installation,
      args: [:installation_id, :now]
    )
  end

  actions do
    defaults([:read])

    create :plan do
      accept([
        :manifest_ref,
        :installation_id,
        :subject_id,
        :subject_state,
        :execution_states,
        :trace_ids,
        :execution_ids,
        :decision_ids,
        :evidence_ids,
        :audit_fact_ids,
        :projection_names,
        :terminal_at,
        :due_at,
        :retention_seconds,
        :storage_kind,
        :metadata
      ])

      upsert?(true)
      upsert_identity(:unique_manifest_ref)

      upsert_fields([
        :subject_state,
        :execution_states,
        :trace_ids,
        :execution_ids,
        :decision_ids,
        :evidence_ids,
        :audit_fact_ids,
        :projection_names,
        :terminal_at,
        :due_at,
        :retention_seconds,
        :storage_kind,
        :metadata
      ])

      change(set_attribute(:status, "pending"))
    end

    update :complete do
      accept([])
      require_atomic?(false)

      argument(:storage_uri, :string, allow_nil?: false)
      argument(:checksum, :string, allow_nil?: false)
      argument(:completed_at, :utc_datetime_usec, allow_nil?: false)
      argument(:metadata, :map, default: %{})

      change(optimistic_lock(:row_version))
      change(set_attribute(:status, "completed"))
      change(set_attribute(:storage_uri, arg(:storage_uri)))
      change(set_attribute(:checksum, arg(:checksum)))
      change(set_attribute(:completed_at, arg(:completed_at)))
      change(set_attribute(:metadata, arg(:metadata)))
    end

    read :by_manifest_ref do
      argument(:manifest_ref, :string, allow_nil?: false)
      get?(true)
      filter(expr(manifest_ref == ^arg(:manifest_ref)))
    end

    read :for_subject do
      argument(:installation_id, :string, allow_nil?: false)
      argument(:subject_id, :uuid, allow_nil?: false)

      filter(expr(installation_id == ^arg(:installation_id) and subject_id == ^arg(:subject_id)))

      prepare(build(sort: [terminal_at: :desc, inserted_at: :desc]))
    end

    read :due_for_installation do
      argument(:installation_id, :string, allow_nil?: false)
      argument(:now, :utc_datetime_usec, allow_nil?: false)

      filter(
        expr(
          installation_id == ^arg(:installation_id) and status == "pending" and
            due_at <= ^arg(:now)
        )
      )

      prepare(build(sort: [due_at: :asc, terminal_at: :asc]))
    end
  end

  attributes do
    uuid_primary_key(:id)

    attribute :manifest_ref, :string do
      allow_nil?(false)
      public?(true)
    end

    attribute :installation_id, :string do
      allow_nil?(false)
      public?(true)
    end

    attribute :subject_id, :uuid do
      allow_nil?(false)
      public?(true)
    end

    attribute :subject_state, :string do
      allow_nil?(false)
      public?(true)
    end

    attribute :execution_states, {:array, :string} do
      allow_nil?(false)
      default([])
      public?(true)
    end

    attribute :trace_ids, {:array, :string} do
      allow_nil?(false)
      default([])
      public?(true)
    end

    attribute :execution_ids, {:array, :uuid} do
      allow_nil?(false)
      default([])
      public?(true)
    end

    attribute :decision_ids, {:array, :uuid} do
      allow_nil?(false)
      default([])
      public?(true)
    end

    attribute :evidence_ids, {:array, :uuid} do
      allow_nil?(false)
      default([])
      public?(true)
    end

    attribute :audit_fact_ids, {:array, :uuid} do
      allow_nil?(false)
      default([])
      public?(true)
    end

    attribute :projection_names, {:array, :string} do
      allow_nil?(false)
      default([])
      public?(true)
    end

    attribute :terminal_at, :utc_datetime_usec do
      allow_nil?(false)
      public?(true)
    end

    attribute :due_at, :utc_datetime_usec do
      allow_nil?(false)
      public?(true)
    end

    attribute :retention_seconds, :integer do
      allow_nil?(false)
      public?(true)
    end

    attribute :storage_kind, :string do
      allow_nil?(false)
      public?(true)
    end

    attribute :status, :string do
      allow_nil?(false)
      default("pending")
      public?(true)
    end

    attribute :storage_uri, :string do
      public?(true)
    end

    attribute :checksum, :string do
      public?(true)
    end

    attribute :completed_at, :utc_datetime_usec do
      public?(true)
    end

    attribute :metadata, :map do
      allow_nil?(false)
      default(%{})
      public?(true)
    end

    attribute :row_version, :integer do
      allow_nil?(false)
      default(1)
      public?(true)
    end

    timestamps()
  end

  identities do
    identity(:unique_manifest_ref, [:manifest_ref])
  end

  @spec plan_from_graph(Graph.t(), CountdownPolicy.t(), map()) :: {:ok, map()} | {:error, term()}
  def plan_from_graph(%Graph{} = graph, %CountdownPolicy{} = policy, attrs \\ %{})
      when is_map(attrs) do
    with {:ok, %OffloadPlan{} = plan} <- OffloadPlan.build(graph, policy) do
      plan(%{
        manifest_ref: build_manifest_ref(graph),
        installation_id: graph.installation_id,
        subject_id: graph.subject_id,
        subject_state: Atom.to_string(graph.subject_state),
        execution_states: Enum.map(graph.execution_states, &Atom.to_string/1),
        trace_ids: graph.trace_ids,
        execution_ids: graph.execution_ids,
        decision_ids: graph.decision_ids,
        evidence_ids: graph.evidence_ids,
        audit_fact_ids: graph.audit_fact_ids,
        projection_names: Map.get(attrs, :projection_names, []),
        terminal_at: graph.terminal_at,
        due_at: plan.due_at,
        retention_seconds: policy.hot_retention_seconds,
        storage_kind: Atom.to_string(policy.cold_storage_kind),
        metadata: Map.get(attrs, :metadata, %{})
      })
    end
  end

  @spec hot_deletion_allowed?(map(), DateTime.t()) :: boolean()
  def hot_deletion_allowed?(manifest, %DateTime{} = now) when is_map(manifest) do
    Map.get(manifest, :status) == "completed" and not is_nil(Map.get(manifest, :completed_at)) and
      DateTime.compare(now, Map.fetch!(manifest, :due_at)) != :lt
  end

  defp build_manifest_ref(%Graph{} = graph) do
    terminal_at_us = DateTime.to_unix(graph.terminal_at, :microsecond)
    "archive/#{graph.installation_id}/#{graph.subject_id}/#{terminal_at_us}"
  end
end
