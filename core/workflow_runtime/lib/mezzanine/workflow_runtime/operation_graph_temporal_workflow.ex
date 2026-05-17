defmodule Mezzanine.WorkflowRuntime.OperationGraphTemporalWorkflow do
  @moduledoc """
  Input, payload, and result helpers for the generic operation graph workflow.

  The workflow stays provider-neutral: product/provider facts are carried as
  operation plan refs and activity result data. Activities own lower-boundary
  side effects and return deterministic result facts to the workflow.
  """

  alias Mezzanine.WorkflowRuntime.OperationGraphExecutor

  defmodule WorkflowInput do
    @moduledoc "Normalized operation graph workflow input."

    @enforce_keys [:graph, :facts, :schedule_attrs]
    defstruct @enforce_keys ++ [activity_result_attrs_by_node_ref: %{}, metadata: %{}]
  end

  @type workflow_input :: %WorkflowInput{
          graph: map(),
          facts: map(),
          schedule_attrs: map(),
          activity_result_attrs_by_node_ref: %{String.t() => map()},
          metadata: map()
        }

  @spec new_input(map()) :: {:ok, workflow_input()} | {:error, term()}
  def new_input(input) when is_map(input) do
    with {:ok, graph} <- required_map(input, :operation_graph),
         {:ok, schedule_attrs} <- required_map(input, :schedule_attrs),
         {:ok, normalized_graph} <- normalize_graph(graph),
         {:ok, normalized_schedule_attrs} <- normalize_schedule_attrs(schedule_attrs) do
      {:ok,
       %WorkflowInput{
         graph: normalized_graph,
         facts: normalize_facts(value(input, :facts, %{})),
         schedule_attrs: normalized_schedule_attrs,
         activity_result_attrs_by_node_ref:
           normalize_result_attrs_by_node_ref(
             value(input, :activity_result_attrs_by_node_ref, %{})
           ),
         metadata: value(input, :metadata, %{})
       }}
    end
  end

  @spec activity_payload(workflow_input(), OperationGraphExecutor.ActivityIntent.t()) :: map()
  def activity_payload(
        %WorkflowInput{} = input,
        %OperationGraphExecutor.ActivityIntent{} = intent
      ) do
    %{
      graph_ref: input.graph.graph_ref,
      activity_intent: activity_intent_payload(intent),
      activity_result_attrs_by_node_ref: input.activity_result_attrs_by_node_ref
    }
  end

  @spec default_activity_result_attrs(map()) :: {:ok, map()} | {:error, term()}
  def default_activity_result_attrs(input) when is_map(input) do
    with {:ok, graph_ref} <- required_string(input, :graph_ref),
         {:ok, intent} <- required_map(input, :activity_intent),
         {:ok, node_ref} <- required_string(intent, :node_ref) do
      scripted =
        input
        |> value(:activity_result_attrs_by_node_ref, %{})
        |> result_attrs_for(node_ref)

      attrs =
        scripted
        |> Map.put_new(:event_ref, event_ref(graph_ref, node_ref, scripted))
        |> Map.put_new(:status, :succeeded)
        |> Map.put_new(:result_ref, result_ref(graph_ref, node_ref))

      {:ok, normalize_result_attrs(attrs)}
    end
  end

  @spec mark_active(map(), String.t()) :: map()
  def mark_active(facts, node_ref) when is_map(facts) and is_binary(node_ref) do
    Map.update(facts, :active_node_refs, [node_ref], fn refs ->
      refs
      |> List.wrap()
      |> then(&[node_ref | &1])
      |> Enum.uniq()
    end)
  end

  @spec workflow_result(workflow_input(), map(), [OperationGraphExecutor.ActivityResultFact.t()]) ::
          map()
  def workflow_result(%WorkflowInput{} = input, facts, recorded_facts) do
    recorded_facts = Enum.reverse(recorded_facts)

    %{
      workflow_type: :operation_graph_run,
      workflow_state: workflow_state(input.graph, facts),
      graph_ref: input.graph.graph_ref,
      operation_context_ref: input.schedule_attrs.operation_context_ref,
      facts: public_facts(facts),
      completed_node_refs: completed_node_refs(facts),
      retry_node_refs: fact_refs(facts, :retry_node_refs),
      activity_result_event_refs: Enum.map(recorded_facts, & &1.event_ref),
      activity_result_refs: recorded_facts |> Enum.map(& &1.result_ref) |> Enum.reject(&is_nil/1),
      recorded_node_refs: Enum.map(recorded_facts, & &1.node_ref),
      terminal_event_refs: event_ref_entries(facts, :terminal_event_refs_by_node_ref),
      metadata: public_metadata(input.metadata)
    }
  end

  @spec activity_task_queue(OperationGraphExecutor.ActivityIntent.t()) :: String.t()
  def activity_task_queue(%OperationGraphExecutor.ActivityIntent{}), do: "mezzanine.hazmat"

  @spec activity_timeout_ms(OperationGraphExecutor.ActivityIntent.t()) :: pos_integer()
  def activity_timeout_ms(%OperationGraphExecutor.ActivityIntent{timeout_policy: timeout_policy}) do
    case Map.get(timeout_policy, :start_to_close_timeout_ms) ||
           Map.get(timeout_policy, "start_to_close_timeout_ms") ||
           Map.get(timeout_policy, :timeout_ms) ||
           Map.get(timeout_policy, "timeout_ms") do
      value when is_integer(value) and value > 0 -> value
      _default -> 30_000
    end
  end

  defp normalize_graph(graph) do
    with {:ok, graph_ref} <- required_string(graph, :graph_ref),
         {:ok, nodes} <- required_list(graph, :nodes),
         {:ok, dependencies} <- required_list(graph, :dependencies) do
      {:ok,
       %{
         graph_ref: graph_ref,
         nodes: Enum.map(nodes, &normalize_node!/1),
         dependencies: Enum.map(dependencies, &normalize_dependency!/1)
       }}
    end
  end

  defp normalize_node!(node) do
    %{
      node_ref: fetch_string!(node, :node_ref),
      operation_role_ref: fetch_string!(node, :operation_role_ref),
      operation_class: value(node, :operation_class),
      projection_order_key: fetch_integer!(node, :projection_order_key)
    }
  end

  defp normalize_dependency!(dependency) do
    %{
      dependency_ref: fetch_string!(dependency, :dependency_ref),
      from_node_ref: fetch_string!(dependency, :from_node_ref),
      to_node_ref: fetch_string!(dependency, :to_node_ref),
      relation: relation!(value(dependency, :relation)),
      completion_policy: completion_policy!(value(dependency, :completion_policy, :required))
    }
  end

  defp normalize_schedule_attrs(attrs) do
    with {:ok, operation_context_ref} <- required_string(attrs, :operation_context_ref),
         {:ok, operation_plans_by_node_ref} <-
           required_map(attrs, :operation_plans_by_node_ref) do
      {:ok,
       %{
         workflow_run_ref: value(attrs, :workflow_run_ref, "workflow-run://unknown"),
         operation_context_ref: operation_context_ref,
         operation_plans_by_node_ref: operation_plans_by_node_ref,
         retry_policies_by_node_ref: value(attrs, :retry_policies_by_node_ref, %{}),
         timeout_policies_by_node_ref: value(attrs, :timeout_policies_by_node_ref, %{}),
         cancellation_policies_by_node_ref: value(attrs, :cancellation_policies_by_node_ref, %{})
       }}
    end
  end

  defp normalize_facts(facts) when is_map(facts) do
    Map.new(facts, fn {key, fact_value} ->
      {known_fact_key(key), fact_value}
    end)
  end

  defp normalize_result_attrs(attrs) when is_map(attrs) do
    Map.new(attrs, fn {key, result_value} ->
      {known_result_key(key), result_value}
    end)
  end

  defp normalize_result_attrs_by_node_ref(attrs_by_node_ref) when is_map(attrs_by_node_ref) do
    Map.new(attrs_by_node_ref, fn {node_ref, attrs} ->
      {node_ref, normalize_result_attrs(attrs)}
    end)
  end

  defp result_attrs_for(attrs_by_node_ref, node_ref) do
    Map.get(attrs_by_node_ref, node_ref) || Map.get(attrs_by_node_ref, to_string(node_ref), %{})
  end

  defp activity_intent_payload(%OperationGraphExecutor.ActivityIntent{} = intent) do
    %{
      activity_intent_ref: intent.activity_intent_ref,
      node_ref: intent.node_ref,
      operation_context_ref: intent.operation_context_ref,
      operation_plan_ref: intent.operation_plan_ref,
      predecessor_event_refs: intent.predecessor_event_refs,
      retry_policy: intent.retry_policy,
      timeout_policy: intent.timeout_policy,
      cancellation_policy: intent.cancellation_policy,
      metadata: intent.metadata
    }
  end

  defp public_facts(facts) do
    facts
    |> Map.take([
      :succeeded_node_refs,
      :reviewed_node_refs,
      :confirmed_node_refs,
      :degraded_node_refs,
      :failed_node_refs,
      :canceled_node_refs,
      :active_node_refs,
      :retry_node_refs,
      :canceling_node_refs,
      :compensated_node_refs,
      :cancellation_requested_ref,
      :cancellation_reason
    ])
    |> Map.put(:terminal_event_refs, event_ref_entries(facts, :terminal_event_refs_by_node_ref))
    |> Map.put(:review_event_refs, event_ref_entries(facts, :review_event_refs_by_node_ref))
    |> Map.put(
      :confirmation_event_refs,
      event_ref_entries(facts, :confirmation_event_refs_by_node_ref)
    )
  end

  defp event_ref_entries(facts, key) do
    facts
    |> Map.get(key, %{})
    |> Enum.map(fn {node_ref, event_ref} ->
      %{node_ref: node_ref, event_ref: event_ref}
    end)
    |> Enum.sort_by(& &1.node_ref)
  end

  defp public_metadata(metadata) when is_map(metadata) do
    metadata
    |> Enum.map(fn {key, value} ->
      %{metadata_key: to_string(key), metadata_value: public_metadata_value(value)}
    end)
    |> Enum.sort_by(& &1.metadata_key)
  end

  defp public_metadata(_metadata), do: []

  defp public_metadata_value(value) when is_map(value), do: public_metadata(value)

  defp public_metadata_value(value) when is_list(value),
    do: Enum.map(value, &public_metadata_value/1)

  defp public_metadata_value(value)
       when is_binary(value) or is_number(value) or is_boolean(value) or is_nil(value),
       do: value

  defp public_metadata_value(value), do: inspect(value)

  defp event_ref(graph_ref, node_ref, attrs) do
    value(attrs, :event_ref, "operation-graph-event://#{graph_ref}/#{node_ref}/succeeded")
  end

  defp result_ref(graph_ref, node_ref),
    do: "operation-graph-result://#{graph_ref}/#{node_ref}"

  defp workflow_state(graph, facts) do
    cond do
      all_nodes_completed?(graph, facts) and fact_refs(facts, :failed_node_refs) != [] ->
        "terminal_with_failures"

      all_nodes_completed?(graph, facts) and fact_refs(facts, :canceled_node_refs) != [] ->
        "terminal_with_cancellations"

      all_nodes_completed?(graph, facts) ->
        "completed"

      fact_refs(facts, :retry_node_refs) != [] ->
        "waiting_for_retry"

      true ->
        "waiting_for_predecessor_facts"
    end
  end

  defp all_nodes_completed?(graph, facts) do
    completed = completed_node_refs(facts)
    Enum.all?(graph.nodes, &(&1.node_ref in completed))
  end

  defp completed_node_refs(facts) do
    [:succeeded_node_refs, :degraded_node_refs, :failed_node_refs, :canceled_node_refs]
    |> Enum.flat_map(&fact_refs(facts, &1))
    |> Enum.uniq()
  end

  defp fact_refs(facts, key), do: facts |> Map.get(key, []) |> List.wrap()

  defp relation!(relation) when relation in [:before, :after, :blocks_on_success],
    do: relation

  defp relation!(:blocks_on_review), do: :blocks_on_review
  defp relation!(:blocks_on_confirmation), do: :blocks_on_confirmation
  defp relation!(:parallel_allowed), do: :parallel_allowed
  defp relation!("before"), do: :before
  defp relation!("after"), do: :after
  defp relation!("blocks_on_success"), do: :blocks_on_success
  defp relation!("blocks_on_review"), do: :blocks_on_review
  defp relation!("blocks_on_confirmation"), do: :blocks_on_confirmation
  defp relation!("parallel_allowed"), do: :parallel_allowed

  defp completion_policy!(policy) when policy in [:required, :optional], do: policy
  defp completion_policy!("required"), do: :required
  defp completion_policy!("optional"), do: :optional

  defp known_fact_key("succeeded_node_refs"), do: :succeeded_node_refs
  defp known_fact_key("reviewed_node_refs"), do: :reviewed_node_refs
  defp known_fact_key("confirmed_node_refs"), do: :confirmed_node_refs
  defp known_fact_key("degraded_node_refs"), do: :degraded_node_refs
  defp known_fact_key("failed_node_refs"), do: :failed_node_refs
  defp known_fact_key("canceled_node_refs"), do: :canceled_node_refs
  defp known_fact_key("active_node_refs"), do: :active_node_refs
  defp known_fact_key("retry_node_refs"), do: :retry_node_refs
  defp known_fact_key("terminal_event_refs_by_node_ref"), do: :terminal_event_refs_by_node_ref
  defp known_fact_key("review_event_refs_by_node_ref"), do: :review_event_refs_by_node_ref

  defp known_fact_key("confirmation_event_refs_by_node_ref"),
    do: :confirmation_event_refs_by_node_ref

  defp known_fact_key(key), do: key

  defp known_result_key("event_ref"), do: :event_ref
  defp known_result_key("status"), do: :status
  defp known_result_key("result_ref"), do: :result_ref
  defp known_result_key("retryable?"), do: :retryable?
  defp known_result_key("retryable"), do: :retryable?
  defp known_result_key("error_class"), do: :error_class
  defp known_result_key("metadata"), do: :metadata
  defp known_result_key(key), do: key

  defp required_map(attrs, key) do
    case value(attrs, key) do
      map when is_map(map) -> {:ok, map}
      _missing -> {:error, {:missing_required_operation_graph_workflow_field, key}}
    end
  end

  defp required_list(attrs, key) do
    case value(attrs, key) do
      list when is_list(list) -> {:ok, list}
      _missing -> {:error, {:missing_required_operation_graph_workflow_field, key}}
    end
  end

  defp required_string(attrs, key) do
    case value(attrs, key) do
      string when is_binary(string) and string != "" ->
        {:ok, string}

      _missing ->
        {:error, {:missing_required_operation_graph_workflow_field, key}}
    end
  end

  defp fetch_string!(attrs, key) do
    case required_string(attrs, key) do
      {:ok, value} -> value
      {:error, reason} -> raise ArgumentError, inspect(reason)
    end
  end

  defp fetch_integer!(attrs, key) do
    case value(attrs, key) do
      integer when is_integer(integer) ->
        integer

      _missing ->
        raise ArgumentError,
              inspect({:missing_required_operation_graph_workflow_field, key})
    end
  end

  defp value(attrs, key, default \\ nil)

  defp value(attrs, key, default) when is_map(attrs) do
    case Map.fetch(attrs, key) do
      {:ok, attr_value} -> attr_value
      :error -> Map.get(attrs, Atom.to_string(key), default)
    end
  end
end

defmodule Mezzanine.Workflows.OperationGraphRun do
  @moduledoc """
  Temporal workflow wrapper for generic operation graph execution.

  The workflow schedules one ready operation-node activity at a time from
  deterministic graph facts. The node activity is generic and receives only
  captured operation plan refs, predecessor refs, and policies.
  """

  use Temporalex.Workflow, task_queue: "mezzanine.agentic"

  alias Mezzanine.WorkflowRuntime.{
    OperationGraphExecutor,
    OperationGraphTemporalWorkflow,
    OperationGraphTemporalWorkflow.WorkflowInput,
    OperationGraphWorkflowStep
  }

  @impl Temporalex.Workflow
  def run(input) do
    with {:ok, workflow_input} <- OperationGraphTemporalWorkflow.new_input(input),
         {:ok, result} <- drain_graph(workflow_input, workflow_input.facts, []) do
      set_state(result)
      {:ok, result}
    end
  end

  @impl Temporalex.Workflow
  def handle_query("operation_graph.state.v1", _args, state), do: {:reply, state || %{}}

  def handle_query("status", _args, state), do: {:reply, state || %{}}

  defp drain_graph(%WorkflowInput{} = input, facts, recorded_facts) do
    set_graph_state(input, facts, recorded_facts)

    case OperationGraphExecutor.ready_activity_intents(input.graph, facts, input.schedule_attrs) do
      {:ok, []} ->
        {:ok, OperationGraphTemporalWorkflow.workflow_result(input, facts, recorded_facts)}

      {:ok, [intent | _remaining_ready]} ->
        run_intent(input, facts, recorded_facts, intent)

      {:error, reason} ->
        {:error, reason}
    end
  end

  defp run_intent(%WorkflowInput{} = input, facts, recorded_facts, intent) do
    payload = OperationGraphTemporalWorkflow.activity_payload(input, intent)
    active_facts = OperationGraphTemporalWorkflow.mark_active(facts, intent.node_ref)
    set_graph_state(input, active_facts, recorded_facts)

    with {:ok, result_attrs} <-
           execute_activity(Mezzanine.Activities.ExecuteOperationGraphNode, payload,
             task_queue: OperationGraphTemporalWorkflow.activity_task_queue(intent),
             start_to_close_timeout: OperationGraphTemporalWorkflow.activity_timeout_ms(intent)
           ),
         {:ok, step} <-
           OperationGraphWorkflowStep.apply_activity_result(
             input.graph,
             active_facts,
             intent,
             result_attrs,
             input.schedule_attrs
           ) do
      drain_graph(input, step.facts, [step.recorded_fact | recorded_facts])
    end
  end

  defp set_graph_state(%WorkflowInput{} = input, facts, recorded_facts) do
    input
    |> OperationGraphTemporalWorkflow.workflow_result(facts, recorded_facts)
    |> set_state()
  end
end

defmodule Mezzanine.Activities.ExecuteOperationGraphNode do
  @moduledoc """
  Generic operation graph node activity.

  Production cutovers replace scripted result attrs with governed operation
  invocation at this boundary. The workflow still receives only deterministic
  activity result facts.
  """

  use Temporalex.Activity,
    task_queue: "mezzanine.hazmat",
    start_to_close_timeout: 30_000,
    retry_policy: [max_attempts: 3]

  alias Mezzanine.WorkflowRuntime.OperationGraphTemporalWorkflow

  @impl Temporalex.Activity
  def perform(input), do: OperationGraphTemporalWorkflow.default_activity_result_attrs(input)
end
