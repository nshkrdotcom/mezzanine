defmodule Mezzanine.CostAttributionEngine do
  @moduledoc """
  Memory-default cost attribution ledger with bounded projections.
  """

  alias OuterBrain.TokenMeter.TokenMeterRef

  defmodule CostFact do
    @moduledoc "Append-only cost fact."
    @enforce_keys [
      :tenant_ref,
      :authority_ref,
      :installation_ref,
      :run_ref,
      :connector_instance_ref,
      :provider_account_ref,
      :capability_id,
      :operation_class,
      :model_ref,
      :persistence_profile_ref,
      :cost_class,
      :token_meter_ref,
      :amount_class,
      :recorded_at,
      :idempotency_key,
      :trace_id,
      :release_manifest_ref
    ]
    defstruct @enforce_keys

    @type t :: %__MODULE__{
            tenant_ref: String.t(),
            authority_ref: String.t(),
            installation_ref: String.t(),
            run_ref: String.t(),
            connector_instance_ref: String.t(),
            provider_account_ref: String.t(),
            capability_id: String.t(),
            operation_class: atom(),
            model_ref: String.t(),
            persistence_profile_ref: String.t(),
            cost_class: atom(),
            token_meter_ref: String.t(),
            amount_class: atom(),
            recorded_at: DateTime.t(),
            idempotency_key: String.t(),
            trace_id: String.t(),
            release_manifest_ref: String.t()
          }
  end

  defmodule Ledger do
    @moduledoc "Memory-default cost ledger."
    @enforce_keys [:tier, :facts]
    defstruct @enforce_keys

    @type t :: %__MODULE__{tier: atom(), facts: [CostFact.t()]}
  end

  defmodule CostBreakdownProjection do
    @moduledoc "Bounded cost projection."
    @enforce_keys [:projection_ref, :tenant_ref, :group_by, :groups, :redaction_posture]
    defstruct @enforce_keys

    @type t :: %__MODULE__{
            projection_ref: String.t(),
            tenant_ref: String.t(),
            group_by: atom(),
            groups: [map()],
            redaction_posture: String.t()
          }
  end

  @cost_classes [:production, :replay, :eval, :simulation, :infrastructure]
  @amount_classes [
    :production_native,
    :redacted_below_floor,
    :redacted_above_ceiling,
    :bounded_excerpt
  ]
  @operation_classes [
    :memory_write,
    :memory_read,
    :prompt_resolve,
    :guard_evaluate,
    :provider_effect,
    :replay_execute,
    :eval_run,
    :stream_chunk,
    :workflow_terminal
  ]
  @aggregate_keys [
    :tenant_ref,
    :run_ref,
    :connector_instance_ref,
    :provider_account_ref,
    :model_ref,
    :capability_id,
    :cost_class
  ]
  @required_strings [
    :tenant_ref,
    :authority_ref,
    :installation_ref,
    :run_ref,
    :connector_instance_ref,
    :provider_account_ref,
    :capability_id,
    :model_ref,
    :persistence_profile_ref,
    :idempotency_key,
    :trace_id,
    :release_manifest_ref
  ]
  @raw_keys [
    :amount,
    :amount_native,
    :cost_amount,
    :raw_amount,
    :provider_payload,
    :prompt_body,
    :memory_body,
    :body,
    :raw_body,
    "amount",
    "amount_native",
    "cost_amount",
    "raw_amount",
    "provider_payload",
    "prompt_body",
    "memory_body",
    "body",
    "raw_body"
  ]

  @spec cost_classes() :: [atom()]
  def cost_classes, do: @cost_classes

  @spec new_ledger(keyword()) :: {:ok, Ledger.t()} | {:error, term()}
  def new_ledger(opts \\ []) when is_list(opts) do
    case Keyword.get(opts, :tier, :memory) do
      :memory -> {:ok, %Ledger{tier: :memory, facts: []}}
      {:durable, :postgres} -> {:error, :cost_postgres_adapter_not_registered}
      _tier -> {:error, :unknown_cost_ledger_tier}
    end
  end

  @spec record(Ledger.t(), map()) :: {:ok, Ledger.t(), CostFact.t()} | {:error, term()}
  def record(%Ledger{} = ledger, attrs) when is_map(attrs) do
    with :ok <- reject_raw(attrs),
         :ok <- required_strings(attrs, @required_strings),
         {:ok, operation_class} <- member(attrs, :operation_class, @operation_classes),
         {:ok, cost_class} <- member(attrs, :cost_class, @cost_classes),
         {:ok, amount_class} <- member(attrs, :amount_class, @amount_classes),
         :ok <- replay_and_eval_never_production(attrs, cost_class),
         {:ok, meter_ref} <- token_meter_ref(attrs),
         {:ok, recorded_at} <- recorded_at(attrs) do
      fact =
        %CostFact{
          tenant_ref: fetch!(attrs, :tenant_ref),
          authority_ref: fetch!(attrs, :authority_ref),
          installation_ref: fetch!(attrs, :installation_ref),
          run_ref: fetch!(attrs, :run_ref),
          connector_instance_ref: fetch!(attrs, :connector_instance_ref),
          provider_account_ref: fetch!(attrs, :provider_account_ref),
          capability_id: fetch!(attrs, :capability_id),
          operation_class: operation_class,
          model_ref: fetch!(attrs, :model_ref),
          persistence_profile_ref: fetch!(attrs, :persistence_profile_ref),
          cost_class: cost_class,
          token_meter_ref: meter_ref,
          amount_class: amount_class,
          recorded_at: recorded_at,
          idempotency_key: fetch!(attrs, :idempotency_key),
          trace_id: fetch!(attrs, :trace_id),
          release_manifest_ref: fetch!(attrs, :release_manifest_ref)
        }

      append_fact(ledger, fact)
    end
  end

  def record(%Ledger{}, _attrs), do: {:error, :invalid_cost_fact}

  @spec aggregate(Ledger.t(), map()) :: {:ok, map()} | {:error, term()}
  def aggregate(%Ledger{} = ledger, filters) when is_map(filters) do
    with :ok <- reject_unknown_filters(filters),
         {:ok, tenant_ref} <- required_filter(filters, :tenant_ref),
         :ok <- tenant_authorized(filters, tenant_ref) do
      facts = filter_facts(ledger.facts, filters)

      {:ok,
       %{
         tenant_ref: tenant_ref,
         fact_count: length(facts),
         groups: group_counts(facts, Map.get(filters, :group_by, :cost_class))
       }}
    end
  end

  @spec project(Ledger.t(), map()) :: {:ok, CostBreakdownProjection.t()} | {:error, term()}
  def project(%Ledger{} = ledger, filters) when is_map(filters) do
    with {:ok, aggregate} <- aggregate(ledger, filters),
         {:ok, group_by} <- group_by(filters) do
      {:ok,
       %CostBreakdownProjection{
         projection_ref: projection_ref(aggregate.tenant_ref, group_by, aggregate.groups),
         tenant_ref: aggregate.tenant_ref,
         group_by: group_by,
         groups:
           Enum.map(aggregate.groups, fn group ->
             %{
               key: group.key,
               fact_count: group.fact_count,
               amount_class: :bounded_excerpt
             }
           end),
         redaction_posture: "bounded_amount_classes_only"
       }}
    end
  end

  defp append_fact(%Ledger{} = ledger, %CostFact{} = fact) do
    case Enum.find(ledger.facts, &(&1.idempotency_key == fact.idempotency_key)) do
      nil -> {:ok, %{ledger | facts: ledger.facts ++ [fact]}, fact}
      existing -> {:ok, ledger, existing}
    end
  end

  defp token_meter_ref(attrs) do
    case fetch(attrs, :token_meter_ref) do
      %TokenMeterRef{} = ref -> {:ok, ref.meter_id}
      value when is_binary(value) and value != "" -> {:ok, value}
      _value -> {:error, :missing_token_meter_ref}
    end
  end

  defp replay_and_eval_never_production(attrs, :production) do
    case {fetch(attrs, :replay_mode), fetch(attrs, :eval_mode)} do
      {nil, nil} -> :ok
      {_replay_mode, _eval_mode} -> {:error, :production_cost_for_replay_or_eval_forbidden}
    end
  end

  defp replay_and_eval_never_production(_attrs, _cost_class), do: :ok

  defp recorded_at(attrs) do
    case fetch(attrs, :recorded_at) do
      %DateTime{} = recorded_at -> {:ok, recorded_at}
      nil -> {:ok, DateTime.utc_now()}
      _value -> {:error, :invalid_recorded_at}
    end
  end

  defp filter_facts(facts, filters) do
    filter_keys = Enum.filter(@aggregate_keys, &Map.has_key?(filters, &1))

    Enum.filter(facts, fn fact ->
      Enum.all?(filter_keys, fn key -> Map.get(fact, key) == Map.get(filters, key) end)
    end)
  end

  defp group_counts(facts, group_by) when group_by in @aggregate_keys do
    facts
    |> Enum.group_by(&Map.get(&1, group_by))
    |> Enum.map(fn {key, grouped} -> %{key: key, fact_count: length(grouped)} end)
    |> Enum.sort_by(&to_string(&1.key))
  end

  defp group_counts(_facts, _group_by), do: []

  defp reject_unknown_filters(filters) do
    allowed = MapSet.new([:caller_tenant_ref, :group_by | @aggregate_keys])

    unknown =
      filters
      |> Map.keys()
      |> Enum.reject(&MapSet.member?(allowed, &1))

    if unknown == [], do: :ok, else: {:error, {:unsupported_cost_aggregation_filter, unknown}}
  end

  defp required_filter(filters, field) do
    case Map.get(filters, field) do
      value when is_binary(value) and value != "" -> {:ok, value}
      _value -> {:error, {:missing_cost_aggregation_filter, field}}
    end
  end

  defp tenant_authorized(filters, tenant_ref) do
    case Map.get(filters, :caller_tenant_ref, tenant_ref) do
      ^tenant_ref -> :ok
      _caller_tenant_ref -> {:error, :cross_tenant_cost_aggregation_forbidden}
    end
  end

  defp group_by(filters) do
    value = Map.get(filters, :group_by, :cost_class)
    if value in @aggregate_keys, do: {:ok, value}, else: {:error, :unsupported_cost_group_by}
  end

  defp required_strings(attrs, fields) do
    case Enum.find(fields, &(not present_string?(fetch(attrs, &1)))) do
      nil -> :ok
      field -> {:error, {:missing_cost_fact_ref, field}}
    end
  end

  defp member(attrs, field, allowed) do
    case fetch(attrs, field) do
      value when is_atom(value) -> member_atom(value, allowed, field)
      value when is_binary(value) -> member_string(value, allowed, field)
      _value -> {:error, {:unknown_cost_fact_enum, field}}
    end
  end

  defp member_atom(value, allowed, field) do
    if value in allowed do
      {:ok, value}
    else
      {:error, {:unknown_cost_fact_enum, field}}
    end
  end

  defp member_string(value, allowed, field) do
    case Enum.find(allowed, &(Atom.to_string(&1) == value)) do
      nil -> {:error, {:unknown_cost_fact_enum, field}}
      found -> {:ok, found}
    end
  end

  defp reject_raw(attrs) do
    case Enum.find(@raw_keys, &Map.has_key?(attrs, &1)) do
      nil -> :ok
      key -> {:error, {:raw_cost_payload_forbidden, key}}
    end
  end

  defp projection_ref(tenant_ref, group_by, groups) do
    source = tenant_ref <> "|" <> Atom.to_string(group_by) <> "|" <> inspect(groups)
    "cost-projection://" <> hash(source)
  end

  defp present_string?(value), do: is_binary(value) and String.trim(value) != ""
  defp fetch!(attrs, field), do: fetch(attrs, field)
  defp fetch(attrs, field), do: Map.get(attrs, field) || Map.get(attrs, Atom.to_string(field))
  defp hash(value), do: :crypto.hash(:sha256, value) |> Base.encode16(case: :lower)
end
