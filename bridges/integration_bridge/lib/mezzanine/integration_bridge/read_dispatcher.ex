defmodule Mezzanine.IntegrationBridge.ReadDispatcher do
  @moduledoc """
  Typed generic lower-read dispatch for Mezzanine-authorized execution lineage.

  The public lookup posture stays substrate-owned:

  - callers provide actor, installation, and execution context
  - `ExecutionLineageStore` resolves the lower identifiers
  - generic lower facts route only through `Jido.Integration.V2.LowerFacts`
  - caller-supplied lower ids never become the primary public lookup key
  """

  alias Jido.Integration.V2.LowerFacts
  alias Mezzanine.Audit.{ExecutionLineage, ExecutionLineageStore, Freshness}
  alias Mezzanine.Intent.ReadIntent

  @lower_facts LowerFacts
  @fetch_lineage &ExecutionLineageStore.fetch/1
  @known_operations [
    :fetch_submission_receipt,
    :fetch_run,
    :attempts,
    :fetch_attempt,
    :events,
    :fetch_artifact,
    :run_artifacts
  ]
  @identifier_query_keys [:submission_key, :run_id, :attempt_id, :artifact_id]

  @spec dispatch_read(ReadIntent.t(), keyword()) :: {:ok, map()} | {:error, term()}
  def dispatch_read(%ReadIntent{} = intent, opts \\ []) when is_list(opts) do
    lower_facts = Keyword.get(opts, :lower_facts, @lower_facts)
    fetch_lineage = Keyword.get(opts, :fetch_lineage, @fetch_lineage)

    with :ok <- supported_read_type(intent),
         {:ok, authorization} <- authorization_context(intent),
         {:ok, operation} <- operation(intent),
         :ok <- operation_supported(lower_facts, operation),
         :ok <- reject_primary_lower_identifier_overrides(operation, intent.query),
         {:ok, lineage} <- fetch_execution_lineage(fetch_lineage, authorization.execution_id),
         :ok <- authorize_lineage(lineage, authorization),
         {:ok, result, source} <-
           dispatch_lower_fact(lower_facts, operation, lineage, intent.query),
         :ok <- enforce_lineage_guard(operation, result, lineage, intent.query) do
      freshness = Freshness.classify_source(source)

      {:ok,
       %{
         operation: operation,
         source: source,
         freshness: freshness,
         operator_actionable?: Freshness.operator_actionable?(freshness),
         lineage: ExecutionLineage.public_lookup(lineage),
         result: result
       }}
    end
  end

  defp supported_read_type(%ReadIntent{read_type: read_type})
       when read_type in [:lower_fact, "lower_fact"],
       do: :ok

  defp supported_read_type(_intent), do: {:error, :unsupported_read_intent}

  defp authorization_context(%ReadIntent{} = intent) do
    subject = normalize_map(intent.subject)
    metadata = normalize_map(intent.metadata)

    with {:ok, actor_id} <- required_context([subject, metadata], :actor_id),
         {:ok, installation_id} <- required_context([subject, metadata], :installation_id),
         {:ok, execution_id} <- required_context([subject, metadata], :execution_id) do
      {:ok,
       %{
         actor_id: actor_id,
         installation_id: installation_id,
         execution_id: execution_id
       }}
    end
  end

  defp operation(%ReadIntent{} = intent) do
    query = normalize_map(intent.query)

    case fetch_value(query, :operation) do
      operation when operation in @known_operations -> {:ok, operation}
      operation when is_binary(operation) -> operation_from_string(operation)
      _other -> {:error, :unsupported_read_intent}
    end
  end

  defp operation_supported(lower_facts, operation) do
    if function_exported?(lower_facts, :operation_supported?, 1) and
         lower_facts.operation_supported?(operation) do
      :ok
    else
      {:error, {:unsupported_lower_facts_operation, operation}}
    end
  end

  defp reject_primary_lower_identifier_overrides(operation, query) do
    query = normalize_map(query)
    allowed_keys = allowed_query_identifier_keys(operation)

    case Enum.find(@identifier_query_keys, &(present?(query, &1) and &1 not in allowed_keys)) do
      nil -> :ok
      key -> {:error, {:lower_identifier_override_forbidden, key}}
    end
  end

  defp fetch_execution_lineage(fetch_lineage, execution_id) when is_function(fetch_lineage, 1) do
    case fetch_lineage.(execution_id) do
      {:ok, %ExecutionLineage{} = lineage} -> {:ok, lineage}
      {:error, _reason} -> {:error, :unknown_execution_lineage}
      other -> {:error, {:unexpected_lineage_fetch_result, other}}
    end
  end

  defp authorize_lineage(%ExecutionLineage{} = lineage, authorization) do
    if lineage.installation_id == authorization.installation_id do
      :ok
    else
      {:error, :unauthorized_lower_read}
    end
  end

  defp dispatch_lower_fact(lower_facts, operation, %ExecutionLineage{} = lineage, query) do
    query = normalize_map(query)

    case operation do
      :fetch_submission_receipt ->
        with {:ok, submission_key} <- lineage_identifier(lineage, :ji_submission_key),
             {:ok, receipt} <-
               fetch_one(lower_facts.fetch_submission_receipt(submission_key), operation) do
          {:ok, receipt, :lower_run_status}
        end

      :fetch_run ->
        with {:ok, run_id} <- lineage_identifier(lineage, :lower_run_id),
             {:ok, run} <- fetch_one(lower_facts.fetch_run(run_id), operation) do
          {:ok, run, :lower_run_status}
        end

      :attempts ->
        with {:ok, run_id} <- lineage_identifier(lineage, :lower_run_id) do
          {:ok, lower_facts.attempts(run_id), :lower_attempt_status}
        end

      :fetch_attempt ->
        with {:ok, attempt_id} <- attempt_id(query, lineage),
             {:ok, attempt} <- fetch_one(lower_facts.fetch_attempt(attempt_id), operation) do
          {:ok, attempt, :lower_attempt_status}
        end

      :events ->
        with {:ok, run_id} <- lineage_identifier(lineage, :lower_run_id) do
          {:ok, lower_facts.events(run_id), :lower_run_status}
        end

      :fetch_artifact ->
        with {:ok, artifact_id} <- artifact_id(query),
             {:ok, artifact} <- fetch_one(lower_facts.fetch_artifact(artifact_id), operation) do
          {:ok, artifact, :lower_artifact_status}
        end

      :run_artifacts ->
        with {:ok, run_id} <- lineage_identifier(lineage, :lower_run_id) do
          {:ok, lower_facts.run_artifacts(run_id), :lower_artifact_status}
        end
    end
  end

  defp enforce_lineage_guard(
         :fetch_submission_receipt,
         receipt,
         %ExecutionLineage{} = lineage,
         _query
       ) do
    ensure_match(
      :fetch_submission_receipt,
      :submission_key,
      receipt,
      :submission_key,
      lineage.ji_submission_key
    )
  end

  defp enforce_lineage_guard(:fetch_run, run, %ExecutionLineage{} = lineage, _query) do
    ensure_match(:fetch_run, :run_id, run, :run_id, lineage.lower_run_id)
  end

  defp enforce_lineage_guard(:attempts, attempts, %ExecutionLineage{} = lineage, _query)
       when is_list(attempts) do
    Enum.reduce_while(attempts, :ok, fn attempt, :ok ->
      case ensure_match(:attempts, :run_id, attempt, :run_id, lineage.lower_run_id) do
        :ok -> {:cont, :ok}
        error -> {:halt, error}
      end
    end)
  end

  defp enforce_lineage_guard(:fetch_attempt, attempt, %ExecutionLineage{} = lineage, query) do
    with :ok <- ensure_match(:fetch_attempt, :run_id, attempt, :run_id, lineage.lower_run_id),
         {:ok, expected_attempt_id} <- attempt_id(normalize_map(query), lineage) do
      ensure_match(:fetch_attempt, :attempt_id, attempt, :attempt_id, expected_attempt_id)
    end
  end

  defp enforce_lineage_guard(:events, events, %ExecutionLineage{} = lineage, _query)
       when is_list(events) do
    Enum.reduce_while(events, :ok, fn event, :ok ->
      case ensure_match(:events, :run_id, event, :run_id, lineage.lower_run_id) do
        :ok -> {:cont, :ok}
        error -> {:halt, error}
      end
    end)
  end

  defp enforce_lineage_guard(:fetch_artifact, artifact, %ExecutionLineage{} = lineage, query) do
    with :ok <- ensure_match(:fetch_artifact, :run_id, artifact, :run_id, lineage.lower_run_id),
         {:ok, expected_artifact_id} <- artifact_id(normalize_map(query)) do
      ensure_match(:fetch_artifact, :artifact_id, artifact, :artifact_id, expected_artifact_id)
    end
  end

  defp enforce_lineage_guard(:run_artifacts, artifacts, %ExecutionLineage{} = lineage, _query)
       when is_list(artifacts) do
    Enum.reduce_while(artifacts, :ok, fn artifact, :ok ->
      case ensure_match(:run_artifacts, :run_id, artifact, :run_id, lineage.lower_run_id) do
        :ok -> {:cont, :ok}
        error -> {:halt, error}
      end
    end)
  end

  defp required_context(sources, key) do
    case Enum.find_value(sources, &fetch_value(&1, key)) do
      value when is_binary(value) and value != "" -> {:ok, value}
      _other -> {:error, :missing_authorization_context}
    end
  end

  defp operation_from_string(operation) do
    case Enum.find(@known_operations, &(Atom.to_string(&1) == operation)) do
      nil -> {:error, :unsupported_read_intent}
      mapped -> {:ok, mapped}
    end
  end

  defp allowed_query_identifier_keys(:fetch_attempt), do: [:attempt_id]
  defp allowed_query_identifier_keys(:fetch_artifact), do: [:artifact_id]
  defp allowed_query_identifier_keys(_operation), do: []

  defp lineage_identifier(%ExecutionLineage{} = lineage, field) do
    case Map.get(lineage, field) do
      value when is_binary(value) and value != "" -> {:ok, value}
      _other -> {:error, {:missing_lineage_identifier, field}}
    end
  end

  defp attempt_id(query, %ExecutionLineage{} = lineage) do
    case fetch_value(query, :attempt_id) do
      value when is_binary(value) and value != "" -> {:ok, value}
      _other -> lineage_identifier(lineage, :lower_attempt_id)
    end
  end

  defp artifact_id(query) do
    case fetch_value(query, :artifact_id) do
      value when is_binary(value) and value != "" -> {:ok, value}
      _other -> {:error, {:missing_query_identifier, :artifact_id}}
    end
  end

  defp fetch_one({:ok, record}, _operation), do: {:ok, record}
  defp fetch_one(:error, operation), do: {:error, {:lower_fact_not_found, operation}}

  defp fetch_one(other, operation),
    do: {:error, {:unexpected_lower_fact_result, operation, other}}

  defp ensure_match(operation, field, record, record_key, expected) do
    actual = fetch_value(record, record_key)

    if actual == expected do
      :ok
    else
      {:error,
       {:mismatched_lower_fact,
        %{
          operation: operation,
          field: field,
          expected: expected,
          actual: actual
        }}}
    end
  end

  defp normalize_map(value) when is_map(value), do: value
  defp normalize_map(_value), do: %{}

  defp fetch_value(map, key) when is_map(map) do
    Map.get(map, key) || Map.get(map, Atom.to_string(key))
  end

  defp present?(map, key) when is_map(map) do
    case fetch_value(map, key) do
      nil -> false
      "" -> false
      _value -> true
    end
  end
end
