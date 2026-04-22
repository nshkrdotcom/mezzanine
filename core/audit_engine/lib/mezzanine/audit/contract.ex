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

  @enforce_keys [:trace_id, :tenant_id, :installation_id, :subject_id, :execution_id]
  defstruct trace_id: nil,
            causation_id: nil,
            tenant_id: nil,
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
          tenant_id: String.t(),
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

  @required_fields [:trace_id, :tenant_id, :installation_id, :subject_id, :execution_id]

  @spec new(map()) :: {:ok, t()} | {:error, {:missing_execution_lineage_fields, [atom()]}}
  def new(attrs) when is_map(attrs) do
    attrs = attrs |> Map.new() |> Map.update(:artifact_refs, [], &List.wrap/1)
    missing_fields = Enum.filter(@required_fields, &blank?(Map.get(attrs, &1)))

    case missing_fields do
      [] -> {:ok, struct!(__MODULE__, attrs)}
      missing_fields -> {:error, {:missing_execution_lineage_fields, missing_fields}}
    end
  end

  @spec new!(map()) :: t()
  def new!(attrs) when is_map(attrs) do
    case new(attrs) do
      {:ok, lineage} ->
        lineage

      {:error, {:missing_execution_lineage_fields, [field | _fields]}} ->
        raise ArgumentError, "missing required execution lineage field: #{field}"
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

defmodule Mezzanine.Audit.TenantScopedTraceJoin.Ref do
  @moduledoc """
  One source ref considered by `Platform.TenantScopedTraceJoin.v1`.
  """

  @enforce_keys [:source_ref, :source_family, :tenant_ref, :resource_ref, :trace_id]
  defstruct source_ref: nil,
            source_family: nil,
            tenant_ref: nil,
            resource_ref: nil,
            trace_id: nil,
            staleness_class: nil,
            exclusion_reason: nil

  @type t :: %__MODULE__{
          source_ref: String.t(),
          source_family: String.t(),
          tenant_ref: String.t(),
          resource_ref: String.t(),
          trace_id: String.t(),
          staleness_class: Mezzanine.Audit.Staleness.t() | nil,
          exclusion_reason: String.t() | nil
        }
end

defmodule Mezzanine.Audit.TenantScopedTraceJoin do
  @moduledoc """
  Tenant-scoped trace reconstruction contract.

  Contract: `Platform.TenantScopedTraceJoin.v1`.
  """

  alias Mezzanine.Audit.Staleness
  alias Mezzanine.Audit.TenantScopedTraceJoin.Ref

  @contract_name "Platform.TenantScopedTraceJoin.v1"
  @base_required_binary_fields [
    :tenant_ref,
    :installation_ref,
    :workspace_ref,
    :project_ref,
    :environment_ref,
    :resource_ref,
    :authority_packet_ref,
    :permission_decision_ref,
    :idempotency_key,
    :trace_id,
    :correlation_id,
    :release_manifest_ref,
    :trace_join_ref,
    :scope_proof_ref
  ]

  defstruct [
    :contract_name,
    :tenant_ref,
    :installation_ref,
    :workspace_ref,
    :project_ref,
    :environment_ref,
    :principal_ref,
    :system_actor_ref,
    :resource_ref,
    :authority_packet_ref,
    :permission_decision_ref,
    :idempotency_key,
    :trace_id,
    :correlation_id,
    :release_manifest_ref,
    :trace_join_ref,
    :resource_scope,
    :joined_ref_set,
    :excluded_ref_set,
    :scope_proof_ref
  ]

  @type t :: %__MODULE__{
          contract_name: String.t(),
          tenant_ref: String.t(),
          installation_ref: String.t(),
          workspace_ref: String.t(),
          project_ref: String.t(),
          environment_ref: String.t(),
          principal_ref: String.t() | nil,
          system_actor_ref: String.t() | nil,
          resource_ref: String.t(),
          authority_packet_ref: String.t(),
          permission_decision_ref: String.t(),
          idempotency_key: String.t(),
          trace_id: String.t(),
          correlation_id: String.t(),
          release_manifest_ref: String.t(),
          trace_join_ref: String.t(),
          resource_scope: [String.t()],
          joined_ref_set: [Ref.t()],
          excluded_ref_set: [Ref.t()],
          scope_proof_ref: String.t()
        }

  @spec contract_name() :: String.t()
  def contract_name, do: @contract_name

  @spec new(map() | keyword() | t()) ::
          {:ok, t()}
          | {:error, {:missing_required_fields, [atom()]}}
          | {:error, term()}
  def new(%__MODULE__{} = join), do: join |> Map.from_struct() |> new()

  def new(attrs) do
    with {:ok, attrs} <- normalize_attrs(attrs),
         [] <- missing_required_fields(attrs),
         {:ok, resource_scope} <- normalize_resource_scope(attrs),
         {:ok, joined_ref_set} <- normalize_joined_refs(attrs, resource_scope),
         {:ok, excluded_ref_set} <- normalize_excluded_refs(attrs) do
      {:ok, build(attrs, resource_scope, joined_ref_set, excluded_ref_set)}
    else
      fields when is_list(fields) -> {:error, {:missing_required_fields, fields}}
      {:error, reason} -> {:error, reason}
    end
  end

  defp build(attrs, resource_scope, joined_ref_set, excluded_ref_set) do
    actors = actor_fields(attrs)

    struct!(__MODULE__, %{
      contract_name: @contract_name,
      tenant_ref: value(attrs, :tenant_ref),
      installation_ref: value(attrs, :installation_ref),
      workspace_ref: value(attrs, :workspace_ref),
      project_ref: value(attrs, :project_ref),
      environment_ref: value(attrs, :environment_ref),
      principal_ref: actors.principal_ref,
      system_actor_ref: actors.system_actor_ref,
      resource_ref: value(attrs, :resource_ref),
      authority_packet_ref: value(attrs, :authority_packet_ref),
      permission_decision_ref: value(attrs, :permission_decision_ref),
      idempotency_key: value(attrs, :idempotency_key),
      trace_id: value(attrs, :trace_id),
      correlation_id: value(attrs, :correlation_id),
      release_manifest_ref: value(attrs, :release_manifest_ref),
      trace_join_ref: value(attrs, :trace_join_ref),
      resource_scope: resource_scope,
      joined_ref_set: joined_ref_set,
      excluded_ref_set: excluded_ref_set,
      scope_proof_ref: value(attrs, :scope_proof_ref)
    })
  end

  defp normalize_attrs(attrs) when is_list(attrs), do: {:ok, Map.new(attrs)}

  defp normalize_attrs(attrs) when is_map(attrs) do
    if Map.has_key?(attrs, :__struct__) do
      {:ok, Map.from_struct(attrs)}
    else
      {:ok, attrs}
    end
  end

  defp normalize_attrs(_attrs), do: {:error, :invalid_attrs}

  defp missing_required_fields(attrs) do
    binary_missing =
      @base_required_binary_fields
      |> Enum.reject(fn field -> present_binary?(value(attrs, field)) end)

    actor_missing =
      if present_binary?(value(attrs, :principal_ref)) or
           present_binary?(value(attrs, :system_actor_ref)) do
        []
      else
        [:principal_ref_or_system_actor_ref]
      end

    list_missing =
      [:resource_scope, :joined_ref_set]
      |> Enum.reject(fn field -> non_empty_list?(value(attrs, field)) end)

    excluded_missing =
      if is_list(value(attrs, :excluded_ref_set)), do: [], else: [:excluded_ref_set]

    binary_missing ++ actor_missing ++ list_missing ++ excluded_missing
  end

  defp normalize_resource_scope(attrs) do
    attrs
    |> value(:resource_scope)
    |> case do
      [_ | _] = refs ->
        if Enum.all?(refs, &present_binary?/1) do
          {:ok, refs}
        else
          {:error, :invalid_resource_scope}
        end

      _other ->
        {:error, :invalid_resource_scope}
    end
  end

  defp normalize_joined_refs(attrs, resource_scope) do
    attrs
    |> value(:joined_ref_set)
    |> Enum.reduce_while({:ok, []}, fn ref_attrs, {:ok, acc} ->
      ref_attrs
      |> normalize_joined_ref(attrs, resource_scope)
      |> reduce_normalized_ref(acc)
    end)
    |> case do
      {:ok, refs} -> {:ok, Enum.reverse(refs)}
      {:error, reason} -> {:error, reason}
    end
  end

  defp normalize_joined_ref(ref_attrs, attrs, resource_scope) do
    with {:ok, ref} <- normalize_ref(ref_attrs, require_staleness?: true),
         :ok <- validate_joined_ref_scope(ref, attrs, resource_scope) do
      {:ok, ref}
    end
  end

  defp validate_joined_ref_scope(ref, attrs, resource_scope) do
    cond do
      ref.tenant_ref != value(attrs, :tenant_ref) ->
        {:error, {:cross_tenant_join_ref, ref.source_ref}}

      ref.trace_id != value(attrs, :trace_id) ->
        {:error, {:trace_scope_violation, ref.source_ref}}

      ref.resource_ref not in resource_scope ->
        {:error, {:resource_scope_violation, ref.source_ref}}

      true ->
        :ok
    end
  end

  defp normalize_excluded_refs(attrs) do
    attrs
    |> value(:excluded_ref_set)
    |> Enum.reduce_while({:ok, []}, fn ref_attrs, {:ok, acc} ->
      ref_attrs
      |> normalize_ref(require_exclusion_reason?: true)
      |> reduce_normalized_ref(acc)
    end)
    |> case do
      {:ok, refs} -> {:ok, Enum.reverse(refs)}
      {:error, reason} -> {:error, reason}
    end
  end

  defp reduce_normalized_ref({:ok, ref}, acc), do: {:cont, {:ok, [ref | acc]}}
  defp reduce_normalized_ref({:error, reason}, _acc), do: {:halt, {:error, reason}}

  defp normalize_ref(ref_attrs, opts) when is_map(ref_attrs) do
    with :ok <- required_ref_fields(ref_attrs, opts),
         {:ok, staleness_class} <- normalize_staleness(value(ref_attrs, :staleness_class), opts) do
      {:ok,
       %Ref{
         source_ref: value(ref_attrs, :source_ref),
         source_family: value(ref_attrs, :source_family),
         tenant_ref: value(ref_attrs, :tenant_ref),
         resource_ref: value(ref_attrs, :resource_ref),
         trace_id: value(ref_attrs, :trace_id),
         staleness_class: staleness_class,
         exclusion_reason: value(ref_attrs, :exclusion_reason)
       }}
    end
  end

  defp normalize_ref(_ref_attrs, _opts), do: {:error, :invalid_trace_join_ref}

  defp required_ref_fields(ref_attrs, opts) do
    required =
      [:source_ref, :source_family, :tenant_ref, :resource_ref, :trace_id]
      |> maybe_add(:staleness_class, Keyword.get(opts, :require_staleness?, false))
      |> maybe_add(:exclusion_reason, Keyword.get(opts, :require_exclusion_reason?, false))

    missing = Enum.reject(required, fn field -> present_binary?(value(ref_attrs, field)) end)

    if missing == [] do
      :ok
    else
      {:error, {:missing_trace_join_ref_fields, missing}}
    end
  end

  defp maybe_add(fields, field, true), do: [field | fields]
  defp maybe_add(fields, _field, false), do: fields

  defp normalize_staleness(nil, opts) do
    if Keyword.get(opts, :require_staleness?, false) do
      {:error, {:missing_trace_join_ref_fields, [:staleness_class]}}
    else
      {:ok, nil}
    end
  end

  defp normalize_staleness(value, _opts) when is_atom(value) do
    if value in Staleness.classes() do
      {:ok, value}
    else
      {:error, {:invalid_staleness_class, value}}
    end
  end

  defp normalize_staleness(value, _opts) when is_binary(value) do
    case value do
      "authoritative_hot" -> {:ok, :authoritative_hot}
      "authoritative_archived" -> {:ok, :authoritative_archived}
      "lower_fresh" -> {:ok, :lower_fresh}
      "projection_stale" -> {:ok, :projection_stale}
      "diagnostic_only" -> {:ok, :diagnostic_only}
      "unavailable" -> {:ok, :unavailable}
      _other -> {:error, {:invalid_staleness_class, value}}
    end
  end

  defp normalize_staleness(value, _opts), do: {:error, {:invalid_staleness_class, value}}

  defp actor_fields(attrs) do
    %{
      principal_ref: value(attrs, :principal_ref),
      system_actor_ref: value(attrs, :system_actor_ref)
    }
  end

  defp value(map, key) do
    Map.get(map, key, Map.get(map, to_string(key)))
  end

  defp present_binary?(value), do: is_binary(value) and byte_size(String.trim(value)) > 0
  defp non_empty_list?([_ | _]), do: true
  defp non_empty_list?(_value), do: false
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
