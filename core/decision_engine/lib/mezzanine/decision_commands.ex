defmodule Mezzanine.DecisionCommands do
  @moduledoc """
  Decision mutation facade over the decision-ledger owner actions.

  The command module intentionally does not own SQL over `decision_records` or
  `audit_facts`. Terminal transitions use the `DecisionRecord` Ash owner actions
  and their optimistic row-version lock.
  """

  alias Mezzanine.Decisions.DecisionRecord

  @spec create_pending(map()) :: {:ok, DecisionRecord.t()} | {:error, term()}
  def create_pending(attrs) when is_map(attrs) do
    DecisionRecord.create_pending(%{
      installation_id: fetch_required!(attrs, :installation_id),
      subject_id: fetch_required!(attrs, :subject_id),
      execution_id: map_value(attrs, :execution_id),
      decision_kind: fetch_required!(attrs, :decision_kind),
      required_by: map_value(attrs, :required_by),
      trace_id: fetch_required!(attrs, :trace_id),
      causation_id: fetch_required!(attrs, :causation_id),
      actor_ref: normalize_map(fetch_required!(attrs, :actor_ref))
    })
  end

  @spec fetch_by_identity(map()) :: {:ok, DecisionRecord.t() | nil} | {:error, term()}
  def fetch_by_identity(attrs) when is_map(attrs) do
    installation_id = fetch_required!(attrs, :installation_id)
    subject_id = fetch_required!(attrs, :subject_id)
    execution_id = map_value(attrs, :execution_id)
    decision_kind = fetch_required!(attrs, :decision_kind)

    result =
      if is_nil(execution_id) do
        DecisionRecord.by_identity_without_execution(installation_id, subject_id, decision_kind)
      else
        DecisionRecord.by_identity(installation_id, subject_id, execution_id, decision_kind)
      end

    case result do
      {:ok, []} -> {:ok, nil}
      {:ok, [decision]} -> {:ok, decision}
      {:error, error} -> {:error, error}
    end
  end

  @spec decide(DecisionRecord.t() | String.t(), map()) ::
          {:ok, DecisionRecord.t()} | {:error, term()}
  def decide(decision_or_id, attrs) when is_map(attrs) do
    with {:ok, decision} <- load_current_decision(decision_or_id),
         :ok <- ensure_pending(decision) do
      DecisionRecord.decide(decision, %{
        decision_value: fetch_required!(attrs, :decision_value),
        reason: map_value(attrs, :reason),
        trace_id: fetch_required!(attrs, :trace_id),
        causation_id: fetch_required!(attrs, :causation_id),
        actor_ref: normalize_map(fetch_required!(attrs, :actor_ref))
      })
    end
  end

  @spec waive(DecisionRecord.t() | String.t(), map()) ::
          {:ok, DecisionRecord.t()} | {:error, term()}
  def waive(decision_or_id, attrs) when is_map(attrs) do
    with {:ok, decision} <- load_current_decision(decision_or_id),
         :ok <- ensure_pending(decision) do
      DecisionRecord.waive(decision, %{
        reason: map_value(attrs, :reason),
        trace_id: fetch_required!(attrs, :trace_id),
        causation_id: fetch_required!(attrs, :causation_id),
        actor_ref: normalize_map(fetch_required!(attrs, :actor_ref))
      })
    end
  end

  @spec expire(DecisionRecord.t() | String.t(), map(), keyword()) ::
          {:ok, DecisionRecord.t()} | {:error, term()}
  def expire(decision_or_id, attrs, opts \\ []) when is_map(attrs) and is_list(opts) do
    current_job_id = Keyword.get(opts, :current_job_id)

    with {:ok, decision} <- load_current_decision(decision_or_id),
         :ok <- ensure_pending(decision),
         :ok <- reject_legacy_expiry_job_ref(decision, current_job_id) do
      DecisionRecord.expire(decision, %{
        reason: map_value(attrs, :reason),
        trace_id: fetch_required!(attrs, :trace_id),
        causation_id: fetch_required!(attrs, :causation_id),
        actor_ref: normalize_map(fetch_required!(attrs, :actor_ref))
      })
    end
  end

  defp load_current_decision(%DecisionRecord{id: id}), do: load_current_decision(id)

  defp load_current_decision(id) when is_binary(id) do
    case Ash.get(DecisionRecord, id, authorize?: false, domain: Mezzanine.Decisions) do
      {:ok, nil} -> {:error, {:decision_not_found, id}}
      {:ok, %DecisionRecord{} = decision} -> {:ok, decision}
      {:error, error} -> {:error, error}
    end
  end

  defp ensure_pending(%DecisionRecord{lifecycle_state: "pending"}), do: :ok

  defp ensure_pending(%DecisionRecord{lifecycle_state: lifecycle_state}),
    do: {:error, {:decision_not_pending, lifecycle_state}}

  defp reject_legacy_expiry_job_ref(decision, current_job_id) do
    case Map.get(decision, :expiry_job_id) do
      nil ->
        :ok

      expiry_job_id ->
        {:error, {:legacy_decision_expiry_job_ref_present, expiry_job_id, current_job_id}}
    end
  end

  defp fetch_required!(attrs, key) when is_map(attrs) do
    case map_value(attrs, key) do
      nil -> raise ArgumentError, "missing required decision attribute #{inspect(key)}"
      value -> value
    end
  end

  defp map_value(map, key) when is_map(map),
    do: Map.get(map, key) || Map.get(map, to_string(key))

  defp normalize_map(value) when is_map(value) do
    value
    |> Enum.map(fn {key, nested} -> {to_string(key), normalize_value(nested)} end)
    |> Map.new()
  end

  defp normalize_map(_other), do: %{}

  defp normalize_value(%DateTime{} = datetime), do: DateTime.to_iso8601(datetime)
  defp normalize_value(value) when is_map(value), do: normalize_map(value)
  defp normalize_value(value) when is_list(value), do: Enum.map(value, &normalize_value/1)
  defp normalize_value(value) when is_atom(value), do: Atom.to_string(value)
  defp normalize_value(value), do: value
end
