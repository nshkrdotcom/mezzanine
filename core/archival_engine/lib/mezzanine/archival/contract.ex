defmodule Mezzanine.Archival.CountdownPolicy do
  @moduledoc """
  Freezes the hot-retention and manifest-before-delete rules for archival.
  """

  @enforce_keys [:hot_retention_seconds]
  defstruct hot_retention_seconds: nil,
            terminal_subject_states: [:completed, :cancelled, :failed, :rejected],
            terminal_execution_states: [:completed, :cancelled, :failed, :rejected],
            cold_storage_kind: :object_store,
            requires_completed_manifest?: true

  @type t :: %__MODULE__{
          hot_retention_seconds: pos_integer(),
          terminal_subject_states: [atom()],
          terminal_execution_states: [atom()],
          cold_storage_kind: atom(),
          requires_completed_manifest?: boolean()
        }

  @spec new!(map()) :: t()
  def new!(attrs) when is_map(attrs) do
    attrs = Map.new(attrs)
    retention = Map.get(attrs, :hot_retention_seconds)

    unless is_integer(retention) and retention > 0 do
      raise ArgumentError, "hot_retention_seconds must be a positive integer"
    end

    struct!(__MODULE__, attrs)
  end
end

defmodule Mezzanine.Archival.Graph do
  @moduledoc """
  Terminal hot-graph candidate that may be offloaded to cold storage.
  """

  @enforce_keys [
    :installation_id,
    :subject_id,
    :trace_ids,
    :subject_state,
    :execution_states,
    :terminal_at
  ]
  defstruct installation_id: nil,
            subject_id: nil,
            trace_ids: [],
            subject_state: nil,
            execution_states: [],
            terminal_at: nil,
            execution_ids: [],
            decision_ids: [],
            evidence_ids: [],
            audit_fact_ids: []

  @type t :: %__MODULE__{
          installation_id: String.t(),
          subject_id: String.t(),
          trace_ids: [String.t()],
          subject_state: atom(),
          execution_states: [atom()],
          terminal_at: DateTime.t(),
          execution_ids: [String.t()],
          decision_ids: [String.t()],
          evidence_ids: [String.t()],
          audit_fact_ids: [String.t()]
        }

  @spec new!(map()) :: t()
  def new!(attrs) when is_map(attrs) do
    attrs =
      attrs
      |> Map.new()
      |> normalize_list(:trace_ids)
      |> normalize_list(:execution_states)
      |> normalize_list(:execution_ids)
      |> normalize_list(:decision_ids)
      |> normalize_list(:evidence_ids)
      |> normalize_list(:audit_fact_ids)

    case Enum.find(
           [:installation_id, :subject_id, :subject_state, :terminal_at],
           &missing?(Map.get(attrs, &1))
         ) do
      nil ->
        struct!(__MODULE__, attrs)

      field ->
        raise ArgumentError, "missing required archival graph field: #{field}"
    end
  end

  defp normalize_list(attrs, key), do: Map.update(attrs, key, [], &List.wrap/1)

  defp missing?(nil), do: true
  defp missing?(""), do: true
  defp missing?([]), do: true
  defp missing?(_value), do: false
end

defmodule Mezzanine.Archival.Manifest do
  @moduledoc """
  Cold-storage manifest that must complete before hot-row deletion is allowed.
  """

  @enforce_keys [:manifest_id, :installation_id, :subject_id, :trace_ids, :status]
  defstruct manifest_id: nil,
            installation_id: nil,
            subject_id: nil,
            trace_ids: [],
            execution_ids: [],
            decision_ids: [],
            evidence_ids: [],
            audit_fact_ids: [],
            storage_uri: nil,
            checksum: nil,
            completed_at: nil,
            status: :pending

  @type t :: %__MODULE__{
          manifest_id: String.t(),
          installation_id: String.t(),
          subject_id: String.t(),
          trace_ids: [String.t()],
          execution_ids: [String.t()],
          decision_ids: [String.t()],
          evidence_ids: [String.t()],
          audit_fact_ids: [String.t()],
          storage_uri: String.t() | nil,
          checksum: String.t() | nil,
          completed_at: DateTime.t() | nil,
          status: :pending | :completed
        }

  @spec new!(Mezzanine.Archival.Graph.t()) :: t()
  def new!(%Mezzanine.Archival.Graph{} = graph) do
    %__MODULE__{
      manifest_id: "archive/#{graph.installation_id}/#{graph.subject_id}",
      installation_id: graph.installation_id,
      subject_id: graph.subject_id,
      trace_ids: graph.trace_ids,
      execution_ids: graph.execution_ids,
      decision_ids: graph.decision_ids,
      evidence_ids: graph.evidence_ids,
      audit_fact_ids: graph.audit_fact_ids,
      status: :pending
    }
  end

  @spec complete(t(), map()) :: t()
  def complete(%__MODULE__{} = manifest, attrs) when is_map(attrs) do
    storage_uri = Map.get(attrs, :storage_uri)
    checksum = Map.get(attrs, :checksum)
    completed_at = Map.get(attrs, :completed_at)

    if missing?(storage_uri) or missing?(checksum) or is_nil(completed_at) do
      raise ArgumentError, "completed manifest requires storage_uri, checksum, and completed_at"
    end

    %__MODULE__{
      manifest
      | storage_uri: storage_uri,
        checksum: checksum,
        completed_at: completed_at,
        status: :completed
    }
  end

  @spec completed?(t()) :: boolean()
  def completed?(%__MODULE__{status: :completed}), do: true
  def completed?(%__MODULE__{}), do: false

  defp missing?(nil), do: true
  defp missing?(""), do: true
  defp missing?(_value), do: false
end

defmodule Mezzanine.Archival.OffloadPlan do
  @moduledoc """
  Pure archival countdown and hot-delete eligibility planner.
  """

  alias Mezzanine.Archival.{CountdownPolicy, Graph, Manifest}

  @enforce_keys [:graph, :policy, :due_at, :manifest]
  defstruct graph: nil,
            policy: nil,
            due_at: nil,
            manifest: nil

  @type t :: %__MODULE__{
          graph: Graph.t(),
          policy: CountdownPolicy.t(),
          due_at: DateTime.t(),
          manifest: Manifest.t()
        }

  @spec build(Graph.t(), CountdownPolicy.t()) ::
          {:ok, t()} | {:error, {:graph_not_terminal, atom()}}
  def build(%Graph{} = graph, %CountdownPolicy{} = policy) do
    with :ok <- ensure_terminal_subject_state(graph, policy),
         :ok <- ensure_terminal_execution_states(graph, policy) do
      {:ok,
       %__MODULE__{
         graph: graph,
         policy: policy,
         due_at: DateTime.add(graph.terminal_at, policy.hot_retention_seconds, :second),
         manifest: Manifest.new!(graph)
       }}
    end
  end

  @spec complete_manifest(t(), map()) :: t()
  def complete_manifest(%__MODULE__{} = plan, attrs) when is_map(attrs) do
    %__MODULE__{plan | manifest: Manifest.complete(plan.manifest, attrs)}
  end

  @spec hot_deletion_allowed?(t(), DateTime.t()) :: boolean()
  def hot_deletion_allowed?(%__MODULE__{} = plan, %DateTime{} = now) do
    countdown_complete? = DateTime.compare(now, plan.due_at) != :lt

    manifest_complete? =
      if plan.policy.requires_completed_manifest? do
        Manifest.completed?(plan.manifest)
      else
        true
      end

    countdown_complete? and manifest_complete?
  end

  defp ensure_terminal_subject_state(%Graph{} = graph, %CountdownPolicy{} = policy) do
    if graph.subject_state in policy.terminal_subject_states do
      :ok
    else
      {:error, {:graph_not_terminal, :subject_state}}
    end
  end

  defp ensure_terminal_execution_states(%Graph{} = graph, %CountdownPolicy{} = policy) do
    if Enum.all?(graph.execution_states, &(&1 in policy.terminal_execution_states)) do
      :ok
    else
      {:error, {:graph_not_terminal, :execution_states}}
    end
  end
end
