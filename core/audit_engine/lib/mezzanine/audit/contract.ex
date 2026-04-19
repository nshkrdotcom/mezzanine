defmodule Mezzanine.Audit.TraceContract do
  @moduledoc """
  Freezes the operational join-key and lookup posture for the neutral audit
  substrate.
  """

  @indexed_join_keys [:trace_id, :causation_id]
  @indexed_ledger_families [:audit_fact, :execution_record, :decision_record, :evidence_record]
  @public_lookup_keys [:installation_id, :subject_id, :execution_id, :trace_id]
  @lower_identifier_fields [
    :citadel_submission_id,
    :ji_submission_key,
    :lower_run_id,
    :lower_attempt_id,
    :artifact_refs
  ]

  @spec indexed_join_keys() :: [atom()]
  def indexed_join_keys, do: @indexed_join_keys

  @spec indexed_ledger_families() :: [atom()]
  def indexed_ledger_families, do: @indexed_ledger_families

  @spec public_lookup_keys() :: [atom()]
  def public_lookup_keys, do: @public_lookup_keys

  @spec lower_identifier_fields() :: [atom()]
  def lower_identifier_fields, do: @lower_identifier_fields
end

defmodule Mezzanine.Audit.ExecutionLineage do
  @moduledoc """
  Cross-layer lineage record for one substrate-owned execution.

  Public reads remain keyed by substrate lineage. Lower identifiers are kept as
  internal bridge linkage once the substrate has already resolved and
  authorized the execution.
  """

  alias Mezzanine.Audit.TraceContract

  @enforce_keys [:trace_id, :installation_id, :subject_id, :execution_id]
  defstruct trace_id: nil,
            causation_id: nil,
            installation_id: nil,
            subject_id: nil,
            execution_id: nil,
            citadel_request_id: nil,
            citadel_submission_id: nil,
            ji_submission_key: nil,
            lower_run_id: nil,
            lower_attempt_id: nil,
            artifact_refs: []

  @type t :: %__MODULE__{
          trace_id: String.t(),
          causation_id: String.t() | nil,
          installation_id: String.t(),
          subject_id: String.t(),
          execution_id: String.t(),
          citadel_request_id: String.t() | nil,
          citadel_submission_id: String.t() | nil,
          ji_submission_key: String.t() | nil,
          lower_run_id: String.t() | nil,
          lower_attempt_id: String.t() | nil,
          artifact_refs: [String.t()]
        }

  @required_fields [:trace_id, :installation_id, :subject_id, :execution_id]

  @spec new!(map()) :: t()
  def new!(attrs) when is_map(attrs) do
    attrs = attrs |> Map.new() |> Map.update(:artifact_refs, [], &List.wrap/1)

    case Enum.find(@required_fields, &blank?(Map.get(attrs, &1))) do
      nil -> struct!(__MODULE__, attrs)
      field -> raise ArgumentError, "missing required execution lineage field: #{field}"
    end
  end

  @spec public_lookup(t()) :: map()
  def public_lookup(%__MODULE__{} = lineage) do
    lineage
    |> Map.from_struct()
    |> Map.take(TraceContract.public_lookup_keys())
  end

  @spec lower_identifiers(t()) :: map()
  def lower_identifiers(%__MODULE__{} = lineage) do
    lineage
    |> Map.from_struct()
    |> Map.take(TraceContract.lower_identifier_fields())
  end

  defp blank?(nil), do: true
  defp blank?(""), do: true
  defp blank?(_value), do: false
end

defmodule Mezzanine.Audit.Staleness do
  @moduledoc """
  Truth-precedence classifier for unified trace assembly and operator safety.
  """

  @classes [
    :authoritative_hot,
    :authoritative_archived,
    :lower_fresh,
    :projection_stale,
    :diagnostic_only,
    :unavailable
  ]

  @source_classes %{
    audit_fact: :authoritative_hot,
    execution_record: :authoritative_hot,
    decision_record: :authoritative_hot,
    evidence_record: :authoritative_hot,
    archived_audit_fact: :authoritative_archived,
    archived_execution_record: :authoritative_archived,
    archived_decision_record: :authoritative_archived,
    archived_evidence_record: :authoritative_archived,
    lower_run_status: :lower_fresh,
    lower_attempt_status: :lower_fresh,
    lower_artifact_status: :lower_fresh,
    operator_projection: :projection_stale,
    bridge_diagnostic: :diagnostic_only
  }

  @type t ::
          :authoritative_hot
          | :authoritative_archived
          | :lower_fresh
          | :projection_stale
          | :diagnostic_only
          | :unavailable

  @spec classes() :: [t()]
  def classes, do: @classes

  @spec source_classes() :: %{atom() => t()}
  def source_classes, do: @source_classes

  @spec classify_source(atom()) :: t()
  def classify_source(source), do: Map.get(@source_classes, source, :diagnostic_only)

  @spec operator_actionable?(t()) :: boolean()
  def operator_actionable?(class) when class in [:authoritative_hot, :authoritative_archived],
    do: true

  def operator_actionable?(_staleness_class), do: false
end

defmodule Mezzanine.Audit.UnifiedTrace.Query do
  @moduledoc """
  Request contract for the operator-facing unified trace query.
  """

  @enforce_keys [:trace_id, :installation_id]
  defstruct trace_id: nil,
            installation_id: nil,
            include_lower?: true,
            include_diagnostic?: false

  @type t :: %__MODULE__{
          trace_id: String.t(),
          installation_id: String.t(),
          include_lower?: boolean(),
          include_diagnostic?: boolean()
        }

  @required_fields [:trace_id, :installation_id]

  @spec new!(map()) :: t()
  def new!(attrs) when is_map(attrs) do
    attrs =
      attrs
      |> Map.new()
      |> Map.put_new(:include_lower?, true)
      |> Map.put_new(:include_diagnostic?, false)

    case Enum.find(@required_fields, &missing?(Map.get(attrs, &1))) do
      nil -> struct!(__MODULE__, attrs)
      field -> raise ArgumentError, "missing required unified trace query field: #{field}"
    end
  end

  defp missing?(nil), do: true
  defp missing?(""), do: true
  defp missing?(_value), do: false
end

defmodule Mezzanine.Audit.UnifiedTrace.Step do
  @moduledoc """
  One operator-visible unified trace step with explicit source and staleness
  semantics.
  """

  @enforce_keys [:ref, :source, :occurred_at, :trace_id, :staleness_class]
  defstruct ref: nil,
            source: nil,
            occurred_at: nil,
            trace_id: nil,
            causation_id: nil,
            staleness_class: nil,
            operator_actionable?: false,
            diagnostic?: false,
            payload: %{}

  @type t :: %__MODULE__{
          ref: String.t(),
          source: atom(),
          occurred_at: DateTime.t(),
          trace_id: String.t(),
          causation_id: String.t() | nil,
          staleness_class: Mezzanine.Audit.Staleness.t(),
          operator_actionable?: boolean(),
          diagnostic?: boolean(),
          payload: map()
        }
end

defmodule Mezzanine.Audit.UnifiedTrace.Timeline do
  @moduledoc """
  End-to-end operator-facing timeline keyed by one `trace_id`.
  """

  alias Mezzanine.Audit.TraceContract

  @enforce_keys [:trace_id, :installation_id, :steps]
  defstruct trace_id: nil,
            installation_id: nil,
            steps: [],
            join_keys: TraceContract.indexed_join_keys()

  @type t :: %__MODULE__{
          trace_id: String.t(),
          installation_id: String.t(),
          steps: [Mezzanine.Audit.UnifiedTrace.Step.t()],
          join_keys: [atom()]
        }
end

defmodule Mezzanine.Audit.UnifiedTrace do
  @moduledoc """
  Pure unified-trace assembler for the operator-facing “3 AM query”.
  """

  alias Mezzanine.Audit.Staleness
  alias Mezzanine.Audit.UnifiedTrace.{Query, Step, Timeline}

  @source_families [:audit_facts, :executions, :decisions, :evidence, :lower_facts]

  @spec assemble(Query.t(), map()) :: {:ok, Timeline.t()}
  def assemble(%Query{} = query, sources) when is_map(sources) do
    steps =
      @source_families
      |> Enum.flat_map(&source_steps(Map.get(sources, &1, []), &1, query))
      |> Enum.sort_by(&DateTime.to_unix(&1.occurred_at, :microsecond))

    {:ok,
     %Timeline{
       trace_id: query.trace_id,
       installation_id: query.installation_id,
       steps: steps
     }}
  end

  defp source_steps(_records, :lower_facts, %Query{include_lower?: false}), do: []

  defp source_steps(records, family, %Query{} = query) when is_list(records) do
    Enum.flat_map(records, &step_for_record(&1, family, query))
  end

  defp step_for_record(record, family, %Query{} = query) when is_map(record) do
    record = Map.new(record)

    if Map.get(record, :trace_id) != query.trace_id do
      []
    else
      source = source_for_record(record, family)
      staleness_class = staleness_class_for_record(record, source)
      diagnostic? = staleness_class in [:diagnostic_only, :unavailable]

      if diagnostic? and not query.include_diagnostic? do
        []
      else
        [
          %Step{
            ref: Map.fetch!(record, :id),
            source: source,
            occurred_at: occurred_at_for_record(record),
            trace_id: Map.fetch!(record, :trace_id),
            causation_id: Map.get(record, :causation_id),
            staleness_class: staleness_class,
            operator_actionable?: Staleness.operator_actionable?(staleness_class),
            diagnostic?: diagnostic?,
            payload: payload_for_step(record)
          }
        ]
      end
    end
  end

  defp source_for_record(record, :lower_facts), do: Map.get(record, :source, :bridge_diagnostic)
  defp source_for_record(_record, :audit_facts), do: :audit_fact
  defp source_for_record(_record, :executions), do: :execution_record
  defp source_for_record(_record, :decisions), do: :decision_record
  defp source_for_record(_record, :evidence), do: :evidence_record

  defp occurred_at_for_record(record) do
    [:occurred_at, :updated_at, :inserted_at, :collected_at, :required_by]
    |> Enum.find_value(&coerce_datetime(Map.get(record, &1)))
    |> case do
      %DateTime{} = occurred_at -> occurred_at
      nil -> Map.fetch!(record, :occurred_at)
    end
  end

  defp coerce_datetime(%DateTime{} = value), do: value
  defp coerce_datetime(%NaiveDateTime{} = value), do: DateTime.from_naive!(value, "Etc/UTC")

  defp coerce_datetime(value) when is_binary(value) do
    case DateTime.from_iso8601(value) do
      {:ok, datetime, _offset} ->
        datetime

      {:error, _reason} ->
        case NaiveDateTime.from_iso8601(value) do
          {:ok, naive_datetime} -> DateTime.from_naive!(naive_datetime, "Etc/UTC")
          {:error, _reason} -> nil
        end
    end
  end

  defp coerce_datetime(_value), do: nil

  defp staleness_class_for_record(record, source) do
    record
    |> Map.get(:staleness_class)
    |> normalize_staleness_class()
    |> case do
      nil -> Staleness.classify_source(source)
      staleness_class -> staleness_class
    end
  end

  defp normalize_staleness_class(value) when is_atom(value) do
    if value in Staleness.classes(), do: value
  end

  defp normalize_staleness_class(value) when is_binary(value) do
    case value do
      "authoritative_hot" -> :authoritative_hot
      "authoritative_archived" -> :authoritative_archived
      "lower_fresh" -> :lower_fresh
      "projection_stale" -> :projection_stale
      "diagnostic_only" -> :diagnostic_only
      "unavailable" -> :unavailable
      _other -> nil
    end
  end

  defp normalize_staleness_class(_value), do: nil

  defp payload_for_step(record) do
    Map.drop(record, [:id, :trace_id, :causation_id, :occurred_at, :staleness_class])
  end
end
