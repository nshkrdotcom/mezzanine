defmodule Mezzanine.WorkflowRuntime.OutboxPersistence do
  @moduledoc """
  Persistence boundary for retained workflow start/signal outboxes.

  Oban workers remain local post-commit dispatchers. After a Temporal runtime
  outcome is known, the worker must record the new outbox row state before it
  acknowledges, snoozes, or fails the job.
  """

  @callback record_start_outcome(map(), map()) :: :ok | {:error, term()}
  @callback record_signal_outcome(map(), map()) :: :ok | {:error, term()}

  @doc "Records the final workflow-start dispatcher row state."
  @spec record_start_outcome(map(), map()) :: :ok | {:error, term()}
  def record_start_outcome(original_row, outcome_row) do
    store().record_start_outcome(original_row, outcome_row)
  end

  @doc "Records the final workflow-signal dispatcher row state."
  @spec record_signal_outcome(map(), map()) :: :ok | {:error, term()}
  def record_signal_outcome(original_row, outcome_row) do
    store().record_signal_outcome(original_row, outcome_row)
  end

  defp store do
    :mezzanine_workflow_runtime
    |> Mezzanine.RuntimeProfileStore.keyword_config(:outbox_persistence, [])
    |> Keyword.get(:store, Mezzanine.WorkflowRuntime.OutboxPersistence.Memory)
  end
end

defmodule Mezzanine.WorkflowRuntime.OutboxPersistence.Memory do
  @moduledoc """
  Memory-only outbox outcome persistence.

  This adapter is the default for Mickey Mouse mode and carries no restart
  durability claim.
  """

  @behaviour Mezzanine.WorkflowRuntime.OutboxPersistence

  alias Mezzanine.Persistence.MemoryStore

  @start_namespace :mezzanine_workflow_start_outbox_outcomes
  @signal_namespace :mezzanine_workflow_signal_outbox_outcomes

  @impl true
  def record_start_outcome(original_row, outcome_row) do
    original_row = normalize(original_row)
    outcome_row = normalize(outcome_row)
    outbox_id = fetch!(original_row, :outbox_id)

    attrs =
      outcome_row
      |> Map.put(:outbox_id, outbox_id)
      |> Map.put_new(:id, outbox_id)

    case MemoryStore.fetch(@start_namespace, outbox_id) do
      {:ok, _row} -> MemoryStore.update(@start_namespace, outbox_id, attrs)
      {:error, :not_found} -> MemoryStore.put(@start_namespace, attrs, id: outbox_id)
    end

    :ok
  end

  @impl true
  def record_signal_outcome(original_row, outcome_row) do
    original_row = normalize(original_row)
    outcome_row = normalize(outcome_row)
    outbox_id = fetch!(original_row, :outbox_id)

    attrs =
      outcome_row
      |> Map.put(:outbox_id, outbox_id)
      |> Map.put_new(:id, outbox_id)

    case MemoryStore.fetch(@signal_namespace, outbox_id) do
      {:ok, _row} -> MemoryStore.update(@signal_namespace, outbox_id, attrs)
      {:error, :not_found} -> MemoryStore.put(@signal_namespace, attrs, id: outbox_id)
    end

    :ok
  end

  def fetch_start_outcome(outbox_id), do: MemoryStore.fetch(@start_namespace, outbox_id)
  def fetch_signal_outcome(outbox_id), do: MemoryStore.fetch(@signal_namespace, outbox_id)

  def reset! do
    MemoryStore.reset!(@start_namespace)
    MemoryStore.reset!(@signal_namespace)
  end

  defp normalize(attrs) when is_list(attrs), do: attrs |> Map.new() |> normalize()
  defp normalize(%_{} = struct), do: struct |> Map.from_struct() |> normalize()

  defp normalize(map) when is_map(map) do
    Map.new(map, fn
      {key, value} when is_binary(key) -> {string_key(key), value}
      {key, value} -> {key, value}
    end)
  end

  defp string_key("dispatch_state"), do: :dispatch_state
  defp string_key("id"), do: :id
  defp string_key("outbox_id"), do: :outbox_id
  defp string_key("workflow_run_id"), do: :workflow_run_id
  defp string_key(key), do: key

  defp fetch!(map, key) do
    case Map.fetch(map, key) do
      {:ok, value} when value not in [nil, ""] -> value
      _missing -> raise ArgumentError, "missing required outbox field #{inspect(key)}"
    end
  end
end

defmodule Mezzanine.WorkflowRuntime.OutboxPersistence.SQL do
  @moduledoc """
  SQL-backed outbox outcome persistence.

  The execution repo owns the local Postgres transaction that inserts the rows.
  This store only updates row state after Temporal delivery outcomes.
  """

  @behaviour Mezzanine.WorkflowRuntime.OutboxPersistence

  alias Ecto.Adapters.SQL

  @start_outcome_sql """
  UPDATE workflow_start_outbox
  SET
    dispatch_state = $2,
    retry_count = $3,
    workflow_run_id = $4,
    last_error_class = $5,
    started_at = CASE
      WHEN $2 IN ('started', 'duplicate_started') THEN now()
      ELSE started_at
    END,
    row_version = row_version + 1,
    updated_at = now()
  WHERE outbox_id = $1
  """

  @signal_outcome_sql """
  UPDATE workflow_signal_outbox
  SET
    dispatch_state = $2,
    workflow_effect_state = $3,
    projection_state = $4,
    dispatch_attempt_count = $5,
    last_error_class = $6,
    row_version = row_version + 1,
    updated_at = now()
  WHERE outbox_id = $1
  """
  @normalizable_keys [
    :dispatch_attempt_count,
    :dispatch_state,
    :last_error_class,
    :outbox_id,
    :projection_state,
    :retry_count,
    :workflow_effect_state,
    :workflow_run_id
  ]
  @key_lookup Map.new(@normalizable_keys, &{Atom.to_string(&1), &1})

  @impl true
  def record_start_outcome(original_row, outcome_row) do
    original_row = normalize(original_row)
    outcome_row = normalize(outcome_row)

    params = [
      fetch!(original_row, :outbox_id),
      fetch!(outcome_row, :dispatch_state),
      Map.get(outcome_row, :retry_count, 0),
      Map.get(outcome_row, :workflow_run_id),
      normalize_error_class(Map.get(outcome_row, :last_error_class, "none"))
    ]

    query(@start_outcome_sql, params)
  end

  @impl true
  def record_signal_outcome(original_row, outcome_row) do
    original_row = normalize(original_row)
    outcome_row = normalize(outcome_row)

    params = [
      fetch!(original_row, :outbox_id),
      fetch!(outcome_row, :dispatch_state),
      Map.get(outcome_row, :workflow_effect_state, "pending_ack"),
      Map.get(outcome_row, :projection_state, "pending"),
      Map.get(outcome_row, :dispatch_attempt_count, 0),
      normalize_error_class(Map.get(outcome_row, :last_error_class, "none"))
    ]

    query(@signal_outcome_sql, params)
  end

  defp query(sql, params) do
    case SQL.query(repo(), sql, params) do
      {:ok, %{num_rows: 1}} -> :ok
      {:ok, %{num_rows: 0}} -> {:error, :outbox_row_not_found}
      {:ok, %{num_rows: count}} -> {:error, {:unexpected_outbox_update_count, count}}
      {:error, reason} -> {:error, reason}
    end
  end

  defp repo do
    :mezzanine_workflow_runtime
    |> Mezzanine.RuntimeProfileStore.keyword_config(:outbox_persistence, [])
    |> Keyword.get(:repo, Mezzanine.Execution.Repo)
  end

  defp normalize(attrs) when is_list(attrs), do: attrs |> Map.new() |> normalize_keys()
  defp normalize(%_{} = struct), do: struct |> Map.from_struct() |> normalize_keys()
  defp normalize(map) when is_map(map), do: normalize_keys(map)

  defp normalize_keys(map) do
    map
    |> Enum.map(fn {key, value} -> {normalize_key(key), value} end)
    |> Map.new()
  end

  defp normalize_key(key) when is_atom(key), do: key
  defp normalize_key(key) when is_binary(key), do: Map.get(@key_lookup, key, key)

  defp fetch!(map, key) do
    case Map.fetch(map, key) do
      {:ok, value} when value not in [nil, ""] -> value
      _missing -> raise ArgumentError, "missing required outbox field #{inspect(key)}"
    end
  end

  defp normalize_error_class(nil), do: "none"
  defp normalize_error_class(value) when is_binary(value), do: value
  defp normalize_error_class(value) when is_atom(value), do: Atom.to_string(value)
  defp normalize_error_class(value), do: inspect(value)
end
