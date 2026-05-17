defmodule Mezzanine.WorkflowRuntime.OperationGraphWorkflowStep do
  @moduledoc """
  Pure workflow-step reducer for operation graph execution.

  Temporal workflows can use this module to keep the deterministic order of a
  graph step explicit: record the activity result fact, then derive ready
  activity intents from the updated fact set.
  """

  alias Mezzanine.WorkflowRuntime.OperationGraphExecutor

  defmodule StepResult do
    @moduledoc "Deterministic result of one operation graph workflow step."

    @enforce_keys [
      :recorded_fact,
      :facts,
      :ready_activity_intents,
      :state
    ]

    defstruct @enforce_keys ++ [metadata: %{}]

    @type state :: :dispatching | :waiting | :terminal

    @type t :: %__MODULE__{
            recorded_fact: OperationGraphExecutor.ActivityResultFact.t(),
            facts: map(),
            ready_activity_intents: [OperationGraphExecutor.ActivityIntent.t()],
            state: state(),
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
    with {:ok, {recorded_fact, updated_facts}} <-
           OperationGraphExecutor.record_activity_result(
             graph,
             facts,
             activity_intent,
             result_attrs
           ),
         {:ok, ready_activity_intents} <-
           OperationGraphExecutor.ready_activity_intents(graph, updated_facts, schedule_attrs) do
      {:ok,
       %StepResult{
         recorded_fact: recorded_fact,
         facts: updated_facts,
         ready_activity_intents: ready_activity_intents,
         state: step_state(graph, updated_facts, ready_activity_intents),
         metadata: %{
           graph_ref: graph.graph_ref,
           completed_node_ref: recorded_fact.node_ref
         }
       }}
    end
  end

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
