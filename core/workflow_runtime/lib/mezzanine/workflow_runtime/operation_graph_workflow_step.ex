defmodule Mezzanine.WorkflowRuntime.OperationGraphWorkflowStep do
  @moduledoc """
  Pure workflow-step reducer for operation graph execution.

  Temporal workflows can use this module to keep the deterministic order of a
  graph step explicit: record the activity result fact, then derive ready
  activity intents from the updated fact set.
  """

  alias Mezzanine.WorkflowRuntime.OperationGraphExecutor
  alias Mezzanine.WorkflowRuntime.WorkflowRetryEvent

  defmodule StepResult do
    @moduledoc "Deterministic result of one operation graph workflow step."

    @enforce_keys [
      :recorded_fact,
      :facts,
      :ready_activity_intents,
      :state
    ]

    defstruct @enforce_keys ++ [retry_timer_event: nil, metadata: %{}]

    @type state :: :dispatching | :waiting | :terminal

    @type t :: %__MODULE__{
            recorded_fact: OperationGraphExecutor.ActivityResultFact.t(),
            facts: map(),
            ready_activity_intents: [OperationGraphExecutor.ActivityIntent.t()],
            state: state(),
            retry_timer_event: WorkflowRetryEvent.t() | nil,
            metadata: map()
          }
  end

  @type step_result :: StepResult.t()

  @spec apply_activity_result(
          map(),
          map(),
          OperationGraphExecutor.ActivityIntent.t(),
          map(),
          map()
        ) ::
          {:ok, step_result()} | {:error, term()}
  def apply_activity_result(graph, facts, activity_intent, result_attrs, schedule_attrs)
      when is_map(graph) and is_map(facts) and is_map(result_attrs) and is_map(schedule_attrs) do
    with {:ok, controlled_result_attrs} <-
           apply_retry_policy(activity_intent, result_attrs),
         {:ok, {recorded_fact, updated_facts}} <-
           OperationGraphExecutor.record_activity_result(
             graph,
             facts,
             activity_intent,
             controlled_result_attrs
           ),
         {:ok, ready_activity_intents} <-
           OperationGraphExecutor.ready_activity_intents(graph, updated_facts, schedule_attrs),
         {:ok, retry_timer_event} <-
           retry_timer_event(
             recorded_fact,
             activity_intent,
             controlled_result_attrs,
             schedule_attrs
           ) do
      {:ok,
       %StepResult{
         recorded_fact: recorded_fact,
         facts: updated_facts,
         ready_activity_intents: ready_activity_intents,
         state: step_state(graph, updated_facts, ready_activity_intents),
         retry_timer_event: retry_timer_event,
         metadata: %{
           graph_ref: graph.graph_ref,
           completed_node_ref: recorded_fact.node_ref
         }
       }}
    end
  end

  defp apply_retry_policy(%OperationGraphExecutor.ActivityIntent{} = intent, result_attrs) do
    if retryable_failure?(result_attrs) and retry_slot(result_attrs) >= max_attempts(intent) do
      {:ok,
       result_attrs
       |> put_attr(:retryable?, false)
       |> put_attr(:retry_exhausted?, true)}
    else
      {:ok, result_attrs}
    end
  end

  defp retry_timer_event(recorded_fact, intent, result_attrs, schedule_attrs) do
    cond do
      retry_exhausted?(result_attrs) ->
        build_retry_event(:exhausted, recorded_fact, intent, result_attrs, schedule_attrs)

      recorded_fact.retryable? ->
        build_retry_event(:backoff, recorded_fact, intent, result_attrs, schedule_attrs)

      true ->
        {:ok, nil}
    end
  end

  defp build_retry_event(kind, recorded_fact, intent, result_attrs, schedule_attrs) do
    with {:ok, workflow_id} <- required_string(schedule_attrs, :workflow_id),
         {:ok, workflow_version} <- required_string(schedule_attrs, :workflow_version),
         {:ok, idempotency_key} <- required_string(schedule_attrs, :idempotency_key),
         {:ok, occurred_at} <- required_value(schedule_attrs, result_attrs, :occurred_at) do
      attrs = %{
        workflow_id: workflow_id,
        workflow_run_id: attr(schedule_attrs, :workflow_run_id),
        workflow_type: attr(schedule_attrs, :workflow_type, "operation_graph_run"),
        workflow_version: workflow_version,
        attempt: retry_slot(result_attrs),
        retry_slot: retry_slot(result_attrs),
        max_retry_slots: max_attempts(intent),
        idempotency_key: idempotency_key <> ":node:" <> recorded_fact.node_ref,
        reason: retry_reason(recorded_fact),
        backoff_ms: backoff_ms(intent, result_attrs),
        occurred_at: occurred_at,
        metadata: %{
          node_ref: recorded_fact.node_ref,
          activity_intent_ref: recorded_fact.activity_intent_ref,
          operation_context_ref: recorded_fact.operation_context_ref,
          operation_plan_ref: recorded_fact.operation_plan_ref,
          retry_timer_ref:
            retry_timer_ref(workflow_id, recorded_fact.node_ref, retry_slot(result_attrs))
        }
      }

      create_retry_event(kind, attrs)
    end
  end

  defp create_retry_event(:backoff, attrs),
    do: WorkflowRetryEvent.abnormal_backoff_retry(attrs)

  defp create_retry_event(:exhausted, attrs),
    do: WorkflowRetryEvent.retry_slot_exhausted(attrs)

  defp retryable_failure?(attrs) do
    attr(attrs, :status) in [:failed, "failed"] and retryable?(attrs)
  end

  defp retryable?(attrs), do: attr(attrs, :retryable?, false) == true
  defp retry_exhausted?(attrs), do: attr(attrs, :retry_exhausted?, false) == true

  defp retry_slot(attrs) do
    attrs
    |> attr(:retry_slot, attr(attrs, :attempt, 1))
    |> positive_integer_or_default(1)
  end

  defp max_attempts(%OperationGraphExecutor.ActivityIntent{retry_policy: retry_policy}) do
    retry_policy
    |> attr(:max_attempts, 3)
    |> positive_integer_or_default(3)
  end

  defp backoff_ms(
         %OperationGraphExecutor.ActivityIntent{retry_policy: retry_policy},
         result_attrs
       ) do
    result_attrs
    |> attr(
      :retry_after_ms,
      attr(retry_policy, :backoff_ms, attr(retry_policy, :initial_backoff_ms, 1_000))
    )
    |> positive_integer_or_default(1_000)
  end

  defp retry_reason(recorded_fact) do
    case recorded_fact.error_class do
      nil -> "retryable_operation_graph_activity_failure"
      reason -> to_string(reason)
    end
  end

  defp retry_timer_ref(workflow_id, node_ref, retry_slot) do
    "workflow-retry-timer://#{workflow_id}/#{node_ref}/slot-#{retry_slot}"
  end

  defp required_string(attrs, key) do
    case attr(attrs, key) do
      value when is_binary(value) and value != "" -> {:ok, value}
      _missing -> {:error, {:missing_required_operation_graph_retry_timer_field, key}}
    end
  end

  defp required_value(primary_attrs, secondary_attrs, key) do
    case attr(primary_attrs, key, attr(secondary_attrs, key)) do
      nil -> {:error, {:missing_required_operation_graph_retry_timer_field, key}}
      value -> {:ok, value}
    end
  end

  defp put_attr(attrs, key, value) when is_map(attrs), do: Map.put(attrs, key, value)

  defp attr(attrs, key, default \\ nil)

  defp attr(attrs, key, default) when is_map(attrs) do
    case Map.fetch(attrs, key) do
      {:ok, value} -> value
      :error -> Map.get(attrs, Atom.to_string(key), default)
    end
  end

  defp positive_integer_or_default(value, _default) when is_integer(value) and value > 0,
    do: value

  defp positive_integer_or_default(_value, default), do: default

  defp step_state(_graph, _facts, [_ | _]), do: :dispatching

  defp step_state(graph, facts, []) do
    if terminal?(graph, facts), do: :terminal, else: :waiting
  end

  defp terminal?(graph, facts) do
    completed_node_refs = completed_node_refs(facts)
    retry_node_refs = fact_refs(facts, :retry_node_refs)
    active_node_refs = fact_refs(facts, :active_node_refs)

    Enum.all?(graph.nodes, &(&1.node_ref in completed_node_refs)) and
      retry_node_refs == [] and active_node_refs == []
  end

  defp completed_node_refs(facts) do
    [:succeeded_node_refs, :degraded_node_refs, :failed_node_refs, :canceled_node_refs]
    |> Enum.flat_map(&fact_refs(facts, &1))
    |> Enum.uniq()
  end

  defp fact_refs(facts, key), do: facts |> Map.get(key, []) |> List.wrap()
end
