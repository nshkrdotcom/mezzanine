defmodule Mezzanine.Effects.EffectRecord do
  @moduledoc """
  Durable business record for one reviewed governed effect.

  The record binds authority and review before dispatch while keeping provider
  credentials, process handles, and raw effect payloads outside Mezzanine.
  """

  alias Mezzanine.AgentRuntime.Support

  @statuses ~w(authorized dispatching running completed failed cancelled ambiguous)
  @ambiguity_states ~w(dispatch_unknown outcome_unknown receipt_missing)
  @required [
    :contract_version,
    :effect_ref,
    :run_ref,
    :turn_ref,
    :command_ref,
    :decision_ref,
    :grant_ref,
    :review_ref,
    :idempotency_key,
    :target_ref,
    :status,
    :row_version
  ]
  @optional [
    :attempt_ref,
    :execution_ref,
    :external_ref,
    :receipt_ref,
    :ambiguity_state,
    :result_artifact_ref
  ]
  @fields @required ++ @optional
  @enforce_keys @required
  defstruct @fields

  @type t :: %__MODULE__{}

  @spec new(map() | keyword() | t()) :: {:ok, t()} | {:error, term()}
  def new(%__MODULE__{} = record), do: validate(record)

  def new(attrs) when is_map(attrs) or is_list(attrs) do
    with {:ok, attrs} <- Support.normalize_attrs(attrs),
         :ok <- reject_unknown(attrs, @fields),
         :ok <- Support.reject_unsafe(attrs, :invalid_effect_record) do
      record = %__MODULE__{
        contract_version: value(attrs, :contract_version, 1),
        effect_ref: value(attrs, :effect_ref),
        run_ref: value(attrs, :run_ref),
        turn_ref: value(attrs, :turn_ref),
        command_ref: value(attrs, :command_ref),
        decision_ref: value(attrs, :decision_ref),
        grant_ref: value(attrs, :grant_ref),
        review_ref: value(attrs, :review_ref),
        idempotency_key: value(attrs, :idempotency_key),
        target_ref: value(attrs, :target_ref),
        status: attrs |> value(:status) |> normalize_string(),
        row_version: value(attrs, :row_version, 1),
        attempt_ref: value(attrs, :attempt_ref),
        execution_ref: value(attrs, :execution_ref),
        external_ref: value(attrs, :external_ref),
        receipt_ref: value(attrs, :receipt_ref),
        ambiguity_state: attrs |> value(:ambiguity_state) |> normalize_string(),
        result_artifact_ref: value(attrs, :result_artifact_ref)
      }

      validate(record)
    end
  end

  def new(_attrs), do: {:error, :invalid_effect_record}

  @spec new!(map() | keyword() | t()) :: t()
  def new!(attrs) do
    case new(attrs) do
      {:ok, record} -> record
      {:error, reason} -> raise ArgumentError, "invalid effect record: #{inspect(reason)}"
    end
  end

  @spec dump(t()) :: map()
  def dump(%__MODULE__{} = record) do
    record
    |> Map.from_struct()
    |> Map.reject(fn {_key, nested} -> is_nil(nested) end)
    |> Support.dump_value()
  end

  def statuses, do: @statuses
  def ambiguity_states, do: @ambiguity_states

  defp validate(%__MODULE__{} = record) do
    refs = [
      record.effect_ref,
      record.run_ref,
      record.turn_ref,
      record.command_ref,
      record.decision_ref,
      record.grant_ref,
      record.review_ref,
      record.target_ref
    ]

    with true <- record.contract_version == 1,
         true <- Enum.all?(refs, &Support.safe_ref?/1),
         true <- present_string?(record.idempotency_key),
         true <- record.status in @statuses,
         true <- is_integer(record.row_version) and record.row_version > 0,
         true <- optional_refs?(record),
         true <- coherent_state?(record) do
      {:ok, record}
    else
      _other -> {:error, :invalid_effect_record}
    end
  end

  defp optional_refs?(record) do
    Enum.all?(@optional -- [:ambiguity_state], fn field ->
      case Map.fetch!(record, field) do
        nil -> true
        ref -> Support.safe_ref?(ref)
      end
    end)
  end

  defp coherent_state?(%__MODULE__{status: "authorized"} = record) do
    empty_runtime_refs?(record) and is_nil(record.ambiguity_state)
  end

  defp coherent_state?(%__MODULE__{status: "dispatching"} = record) do
    ref?(record.attempt_ref) and is_nil(record.execution_ref) and is_nil(record.receipt_ref) and
      is_nil(record.ambiguity_state) and is_nil(record.result_artifact_ref)
  end

  defp coherent_state?(%__MODULE__{status: "running"} = record) do
    ref?(record.attempt_ref) and ref?(record.execution_ref) and is_nil(record.receipt_ref) and
      is_nil(record.ambiguity_state) and is_nil(record.result_artifact_ref)
  end

  defp coherent_state?(%__MODULE__{status: "completed"} = record) do
    runtime_identity?(record) and ref?(record.receipt_ref) and
      ref?(record.result_artifact_ref) and is_nil(record.ambiguity_state)
  end

  defp coherent_state?(%__MODULE__{status: status} = record)
       when status in ~w(failed cancelled) do
    cancellation_identity?(record) and ref?(record.receipt_ref) and
      is_nil(record.ambiguity_state)
  end

  defp coherent_state?(%__MODULE__{status: "ambiguous"} = record) do
    ref?(record.attempt_ref) and record.ambiguity_state in @ambiguity_states and
      is_nil(record.result_artifact_ref)
  end

  defp empty_runtime_refs?(record) do
    Enum.all?(
      [:attempt_ref, :execution_ref, :external_ref, :receipt_ref, :result_artifact_ref],
      &is_nil(Map.fetch!(record, &1))
    )
  end

  defp runtime_identity?(record), do: ref?(record.attempt_ref) and ref?(record.execution_ref)

  defp cancellation_identity?(record) do
    (is_nil(record.attempt_ref) and is_nil(record.execution_ref)) or runtime_identity?(record)
  end

  defp ref?(value), do: Support.safe_ref?(value)

  defp reject_unknown(attrs, fields) do
    allowed = MapSet.new(Enum.flat_map(fields, &[&1, Atom.to_string(&1)]))

    if Enum.all?(Map.keys(attrs), &MapSet.member?(allowed, &1)),
      do: :ok,
      else: {:error, :invalid_effect_record}
  end

  defp value(attrs, key, default \\ nil),
    do: Map.get(attrs, key, Map.get(attrs, Atom.to_string(key), default))

  defp normalize_string(nil), do: nil
  defp normalize_string(value) when is_atom(value), do: Atom.to_string(value)
  defp normalize_string(value), do: value
  defp present_string?(value), do: is_binary(value) and String.trim(value) != ""
end

defmodule Mezzanine.Effects.Lifecycle do
  @moduledoc "Optimistic lifecycle validation for one durable governed effect record."

  alias Mezzanine.Effects.EffectRecord

  @transitions %{
    "authorized" => ~w(dispatching cancelled),
    "dispatching" => ~w(running failed cancelled ambiguous),
    "running" => ~w(completed failed cancelled ambiguous),
    "ambiguous" => ~w(completed failed cancelled),
    "completed" => [],
    "failed" => [],
    "cancelled" => []
  }
  @update_fields ~w(
    attempt_ref execution_ref external_ref receipt_ref ambiguity_state result_artifact_ref
  )a

  @spec transition(EffectRecord.t(), atom() | String.t(), map() | keyword()) ::
          {:ok, EffectRecord.t()} | {:error, term()}
  def transition(%EffectRecord{} = record, next_status, attrs)
      when is_map(attrs) or is_list(attrs) do
    attrs = Map.new(attrs)
    next_status = normalize_string(next_status)

    with true <- known_fields?(attrs),
         true <- value(attrs, :expected_row_version) == record.row_version,
         true <- next_status in Map.fetch!(@transitions, record.status) do
      updates =
        Enum.reduce(@update_fields, Map.from_struct(record), fn field, acc ->
          case fetch(attrs, field) do
            {:ok, nested} -> Map.put(acc, field, nested)
            :error -> acc
          end
        end)

      result =
        updates
        |> Map.put(:status, next_status)
        |> Map.put(:row_version, record.row_version + 1)
        |> EffectRecord.new()

      case result do
        {:ok, _record} = ok -> ok
        {:error, _reason} -> {:error, :invalid_effect_transition}
      end
    else
      false -> {:error, :invalid_effect_transition}
    end
  end

  def transition(%EffectRecord{}, _next_status, _attrs),
    do: {:error, :invalid_effect_transition}

  defp known_fields?(attrs) do
    fields = [:expected_row_version | @update_fields]
    allowed = MapSet.new(Enum.flat_map(fields, &[&1, Atom.to_string(&1)]))
    Enum.all?(Map.keys(attrs), &MapSet.member?(allowed, &1))
  end

  defp fetch(attrs, key) do
    case Map.fetch(attrs, key) do
      {:ok, value} -> {:ok, value}
      :error -> Map.fetch(attrs, Atom.to_string(key))
    end
  end

  defp value(attrs, key), do: Map.get(attrs, key, Map.get(attrs, Atom.to_string(key)))
  defp normalize_string(value) when is_atom(value), do: Atom.to_string(value)
  defp normalize_string(value), do: value
end
