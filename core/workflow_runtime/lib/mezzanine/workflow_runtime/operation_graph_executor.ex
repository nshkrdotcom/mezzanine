defmodule Mezzanine.WorkflowRuntime.OperationGraphExecutor do
  @moduledoc """
  Deterministic operation graph scheduling support for Temporal workflows.

  This module stays pure: it reads recorded graph facts and returns activity
  intents. Activities own lower-boundary I/O; workflows only schedule these
  intents from captured operation plans and recorded predecessor facts.
  """

  defmodule ActivityIntent do
    @moduledoc "Workflow-safe intent for one ready operation node."

    @enforce_keys [
      :activity_intent_ref,
      :node_ref,
      :operation_context_ref,
      :operation_plan_ref,
      :predecessor_event_refs,
      :retry_policy,
      :timeout_policy,
      :cancellation_policy
    ]

    defstruct @enforce_keys ++ [metadata: %{}]

    @type t :: %__MODULE__{
            activity_intent_ref: String.t(),
            node_ref: String.t(),
            operation_context_ref: String.t(),
            operation_plan_ref: String.t(),
            predecessor_event_refs: [String.t()],
            retry_policy: map(),
            timeout_policy: map(),
            cancellation_policy: map(),
            metadata: map()
          }
  end

  defmodule ActivityResultFact do
    @moduledoc "Recorded workflow fact derived from one activity result."

    @enforce_keys [
      :event_ref,
      :node_ref,
      :activity_intent_ref,
      :operation_context_ref,
      :operation_plan_ref,
      :status,
      :terminal?
    ]

    defstruct @enforce_keys ++
                [
                  result_ref: nil,
                  retryable?: false,
                  error_class: nil,
                  metadata: %{}
                ]

    @type status :: :succeeded | :failed | :degraded | :canceled

    @type t :: %__MODULE__{
            event_ref: String.t(),
            node_ref: String.t(),
            activity_intent_ref: String.t(),
            operation_context_ref: String.t(),
            operation_plan_ref: String.t(),
            status: status(),
            terminal?: boolean(),
            result_ref: String.t() | nil,
            retryable?: boolean(),
            error_class: atom() | String.t() | nil,
            metadata: map()
          }
  end

  defmodule CancellationRequestFact do
    @moduledoc "Recorded workflow fact derived from one operation graph cancellation request."

    @enforce_keys [
      :event_ref,
      :reason,
      :canceling_node_refs,
      :terminal?
    ]

    defstruct @enforce_keys ++ [metadata: %{}]

    @type t :: %__MODULE__{
            event_ref: String.t(),
            reason: String.t(),
            canceling_node_refs: [String.t()],
            terminal?: boolean(),
            metadata: map()
          }
  end

  defmodule CancellationIntent do
    @moduledoc "Workflow-safe intent for cancelling one active operation graph node."

    @enforce_keys [
      :cancel_intent_ref,
      :node_ref,
      :operation_context_ref,
      :operation_plan_ref,
      :cancellation_policy,
      :reason,
      :idempotency_key
    ]

    defstruct @enforce_keys ++ [metadata: %{}]

    @type t :: %__MODULE__{
            cancel_intent_ref: String.t(),
            node_ref: String.t(),
            operation_context_ref: String.t(),
            operation_plan_ref: String.t(),
            cancellation_policy: map(),
            reason: String.t(),
            idempotency_key: String.t(),
            metadata: map()
          }
  end

  defmodule CompensationIntent do
    @moduledoc "Workflow-safe intent for compensating one completed operation graph node."

    @enforce_keys [
      :compensation_intent_ref,
      :node_ref,
      :operation_context_ref,
      :operation_plan_ref,
      :predecessor_event_ref,
      :compensation_policy,
      :reason,
      :idempotency_key
    ]

    defstruct @enforce_keys ++ [metadata: %{}]

    @type t :: %__MODULE__{
            compensation_intent_ref: String.t(),
            node_ref: String.t(),
            operation_context_ref: String.t(),
            operation_plan_ref: String.t(),
            predecessor_event_ref: String.t(),
            compensation_policy: map(),
            reason: String.t(),
            idempotency_key: String.t(),
            metadata: map()
          }
  end

  @type activity_intent :: ActivityIntent.t()
  @type activity_result_fact :: ActivityResultFact.t()
  @type cancellation_request_fact :: CancellationRequestFact.t()
  @type cancellation_intent :: CancellationIntent.t()
  @type compensation_intent :: CompensationIntent.t()

  @spec ready_node_refs(map(), map()) :: [String.t()]
  def ready_node_refs(graph, facts) when is_map(graph) and is_map(facts) do
    normalized_facts = normalize_facts(facts)

    if cancellation_requested?(normalized_facts) do
      []
    else
      graph.nodes
      |> Enum.reject(
        &(completed?(normalized_facts, &1.node_ref) or in_progress?(normalized_facts, &1.node_ref))
      )
      |> Enum.filter(&predecessors_satisfied?(graph.dependencies, &1.node_ref, normalized_facts))
      |> Enum.sort_by(&stable_node_key(graph, &1))
      |> Enum.map(& &1.node_ref)
    end
  end

  @spec ready_activity_intents(map(), map(), map() | keyword()) ::
          {:ok, [activity_intent()]} | {:error, term()}
  def ready_activity_intents(graph, facts, attrs)
      when is_map(facts) and (is_map(attrs) or is_list(attrs)) do
    with {:ok, context_ref} <- required_string(attrs, :operation_context_ref),
         {:ok, plans_by_node} <- required_map(attrs, :operation_plans_by_node_ref) do
      node_lookup = Map.new(graph.nodes, &{&1.node_ref, &1})

      build_ready_activity_intents(graph, facts, attrs, context_ref, plans_by_node, node_lookup)
    end
  end

  @spec record_activity_result(map(), map(), activity_intent(), map() | keyword()) ::
          {:ok, {activity_result_fact(), map()}} | {:error, term()}
  def record_activity_result(graph, facts, %ActivityIntent{} = intent, result_attrs)
      when is_map(graph) and is_map(facts) and (is_map(result_attrs) or is_list(result_attrs)) do
    with :ok <- known_node_ref(graph, intent.node_ref),
         {:ok, event_ref} <- required_result_string(result_attrs, :event_ref),
         {:ok, status} <- required_status(result_attrs) do
      fact =
        activity_result_fact(intent, event_ref, status, result_attrs)

      {:ok, {fact, apply_activity_result_fact(facts, fact)}}
    end
  end

  @spec apply_cancellation_request(map(), map(), map() | keyword()) ::
          {:ok, {cancellation_request_fact(), map()}} | {:error, term()}
  def apply_cancellation_request(graph, facts, attrs)
      when is_map(graph) and is_map(facts) and (is_map(attrs) or is_list(attrs)) do
    with {:ok, event_ref} <- required_cancellation_string(attrs, :event_ref) do
      normalized_facts = normalize_facts(facts)
      canceling_node_refs = cancelable_node_refs(graph, normalized_facts)

      fact = %CancellationRequestFact{
        event_ref: event_ref,
        reason: cancellation_reason(attrs),
        canceling_node_refs: canceling_node_refs,
        terminal?: canceling_node_refs == [],
        metadata: get_attr(attrs, :metadata, %{})
      }

      {:ok, {fact, apply_cancellation_request_fact(facts, fact)}}
    end
  end

  @spec cancellation_intents(map(), map(), map() | keyword()) ::
          {:ok, [cancellation_intent()]} | {:error, term()}
  def cancellation_intents(graph, facts, attrs)
      when is_map(graph) and is_map(facts) and (is_map(attrs) or is_list(attrs)) do
    node_lookup = Map.new(graph.nodes, &{&1.node_ref, &1})

    with {:ok, context_ref} <- required_string(attrs, :operation_context_ref),
         {:ok, plans_by_node} <- required_map(attrs, :operation_plans_by_node_ref),
         {:ok, cancellation_request_ref} <- cancellation_request_ref(facts),
         {:ok, canceling_node_refs} <-
           sort_node_refs_by_graph(graph, node_lookup, fact_refs(facts, :canceling_node_refs)) do
      build_cancellation_intents(
        canceling_node_refs,
        graph,
        attrs,
        context_ref,
        plans_by_node,
        node_lookup,
        cancellation_request_ref
      )
    end
  end

  @spec compensation_intents(map(), map(), map() | keyword()) ::
          {:ok, [compensation_intent()]} | {:error, term()}
  def compensation_intents(graph, facts, attrs)
      when is_map(graph) and is_map(facts) and (is_map(attrs) or is_list(attrs)) do
    node_lookup = Map.new(graph.nodes, &{&1.node_ref, &1})

    with {:ok, context_ref} <- required_string(attrs, :operation_context_ref),
         {:ok, plans_by_node} <- required_map(attrs, :operation_plans_by_node_ref),
         {:ok, cancellation_request_ref} <- cancellation_request_ref(facts),
         {:ok, compensatable_node_refs} <-
           sort_node_refs_by_graph(
             graph,
             node_lookup,
             compensatable_node_refs(graph, facts, attrs),
             :desc
           ) do
      build_compensation_intents(
        compensatable_node_refs,
        graph,
        facts,
        attrs,
        context_ref,
        plans_by_node,
        node_lookup,
        cancellation_request_ref
      )
    end
  end

  defp build_ready_activity_intents(graph, facts, attrs, context_ref, plans_by_node, node_lookup) do
    graph
    |> ready_node_refs(facts)
    |> Enum.reduce_while({:ok, []}, fn node_ref, acc ->
      reduce_activity_intent(
        graph,
        facts,
        attrs,
        context_ref,
        plans_by_node,
        node_lookup,
        node_ref,
        acc
      )
    end)
    |> case do
      {:ok, intents} -> {:ok, Enum.reverse(intents)}
      {:error, reason} -> {:error, reason}
    end
  end

  defp sort_node_refs_by_graph(graph, node_lookup, node_refs, direction \\ :asc) do
    node_refs
    |> Enum.reduce_while({:ok, []}, fn node_ref, {:ok, nodes} ->
      case fetch_node(node_lookup, node_ref) do
        {:ok, node} -> {:cont, {:ok, [node | nodes]}}
        {:error, reason} -> {:halt, {:error, reason}}
      end
    end)
    |> case do
      {:ok, nodes} ->
        sorted_refs =
          nodes
          |> Enum.sort_by(&stable_node_key(graph, &1), direction)
          |> Enum.map(& &1.node_ref)

        {:ok, sorted_refs}

      {:error, reason} ->
        {:error, reason}
    end
  end

  defp build_cancellation_intents(
         node_refs,
         graph,
         attrs,
         context_ref,
         plans_by_node,
         node_lookup,
         cancellation_request_ref
       ) do
    node_refs
    |> Enum.reduce_while({:ok, []}, fn node_ref, {:ok, intents} ->
      case cancellation_intent(
             graph,
             attrs,
             context_ref,
             plans_by_node,
             node_lookup,
             node_ref,
             cancellation_request_ref
           ) do
        {:ok, intent} -> {:cont, {:ok, [intent | intents]}}
        {:error, reason} -> {:halt, {:error, reason}}
      end
    end)
    |> reverse_intents()
  end

  defp build_compensation_intents(
         node_refs,
         graph,
         facts,
         attrs,
         context_ref,
         plans_by_node,
         node_lookup,
         cancellation_request_ref
       ) do
    node_refs
    |> Enum.reduce_while({:ok, []}, fn node_ref, {:ok, intents} ->
      case compensation_intent(
             graph,
             facts,
             attrs,
             context_ref,
             plans_by_node,
             node_lookup,
             node_ref,
             cancellation_request_ref
           ) do
        {:ok, intent} -> {:cont, {:ok, [intent | intents]}}
        {:error, reason} -> {:halt, {:error, reason}}
      end
    end)
    |> reverse_intents()
  end

  defp reverse_intents({:ok, intents}), do: {:ok, Enum.reverse(intents)}
  defp reverse_intents({:error, reason}), do: {:error, reason}

  defp cancellation_intent(
         graph,
         attrs,
         context_ref,
         plans,
         node_lookup,
         node_ref,
         cancellation_request_ref
       ) do
    with {:ok, node} <- fetch_node(node_lookup, node_ref),
         {:ok, operation_plan_ref} <- operation_plan_ref(plans, node_ref) do
      {:ok,
       %CancellationIntent{
         cancel_intent_ref: cancellation_intent_ref(attrs, node_ref),
         node_ref: node_ref,
         operation_context_ref: context_ref,
         operation_plan_ref: operation_plan_ref,
         cancellation_policy: policy_for(attrs, :cancellation_policies_by_node_ref, node_ref),
         reason: cancellation_reason(attrs),
         idempotency_key: idempotency_key(attrs, "cancel", node_ref),
         metadata: %{
           graph_ref: graph.graph_ref,
           cancellation_request_ref: cancellation_request_ref,
           operation_role_ref: node.operation_role_ref,
           operation_class: node.operation_class,
           projection_order_key: node.projection_order_key
         }
       }}
    end
  end

  defp compensation_intent(
         graph,
         facts,
         attrs,
         context_ref,
         plans,
         node_lookup,
         node_ref,
         cancellation_request_ref
       ) do
    with {:ok, node} <- fetch_node(node_lookup, node_ref),
         {:ok, operation_plan_ref} <- operation_plan_ref(plans, node_ref),
         {:ok, predecessor_event_ref} <- terminal_event_ref(facts, node_ref) do
      {:ok,
       %CompensationIntent{
         compensation_intent_ref: compensation_intent_ref(attrs, node_ref),
         node_ref: node_ref,
         operation_context_ref: context_ref,
         operation_plan_ref: operation_plan_ref,
         predecessor_event_ref: predecessor_event_ref,
         compensation_policy: policy_for(attrs, :compensation_policies_by_node_ref, node_ref),
         reason: cancellation_reason(attrs),
         idempotency_key: idempotency_key(attrs, "compensate", node_ref),
         metadata: %{
           graph_ref: graph.graph_ref,
           cancellation_request_ref: cancellation_request_ref,
           operation_role_ref: node.operation_role_ref,
           operation_class: node.operation_class,
           projection_order_key: node.projection_order_key
         }
       }}
    end
  end

  defp reduce_activity_intent(
         graph,
         facts,
         attrs,
         context_ref,
         plans_by_node,
         node_lookup,
         node_ref,
         {:ok, intents}
       ) do
    node = Map.fetch!(node_lookup, node_ref)

    case build_activity_intent(graph, facts, attrs, node, context_ref, plans_by_node) do
      {:ok, intent} -> {:cont, {:ok, [intent | intents]}}
      {:error, reason} -> {:halt, {:error, reason}}
    end
  end

  defp build_activity_intent(graph, facts, attrs, node, context_ref, plans) do
    with {:ok, operation_plan_ref} <- operation_plan_ref(plans, node.node_ref),
         {:ok, predecessor_event_refs} <- predecessor_event_refs(graph, facts, node.node_ref) do
      {:ok,
       %ActivityIntent{
         activity_intent_ref: activity_intent_ref(attrs, node.node_ref),
         node_ref: node.node_ref,
         operation_context_ref: context_ref,
         operation_plan_ref: operation_plan_ref,
         predecessor_event_refs: predecessor_event_refs,
         retry_policy: policy_for(attrs, :retry_policies_by_node_ref, node.node_ref),
         timeout_policy: policy_for(attrs, :timeout_policies_by_node_ref, node.node_ref),
         cancellation_policy:
           policy_for(attrs, :cancellation_policies_by_node_ref, node.node_ref),
         metadata: %{
           graph_ref: graph.graph_ref,
           operation_role_ref: node.operation_role_ref,
           operation_class: node.operation_class,
           projection_order_key: node.projection_order_key
         }
       }}
    end
  end

  defp known_node_ref(graph, node_ref) do
    if Enum.any?(graph.nodes, &(&1.node_ref == node_ref)) do
      :ok
    else
      {:error, {:unknown_operation_graph_node, %{node_ref: node_ref}}}
    end
  end

  defp fetch_node(node_lookup, node_ref) do
    case Map.fetch(node_lookup, node_ref) do
      {:ok, node} -> {:ok, node}
      :error -> {:error, {:unknown_operation_graph_node, %{node_ref: node_ref}}}
    end
  end

  defp required_status(attrs) do
    case get_attr(attrs, :status) do
      status when status in [:succeeded, :failed, :degraded, :canceled] ->
        {:ok, status}

      "succeeded" ->
        {:ok, :succeeded}

      "failed" ->
        {:ok, :failed}

      "degraded" ->
        {:ok, :degraded}

      "canceled" ->
        {:ok, :canceled}

      _missing_or_unknown ->
        {:error, {:missing_required_activity_result_field, :status}}
    end
  end

  defp required_result_string(attrs, key) do
    case get_attr(attrs, key) do
      value when is_binary(value) and value != "" -> {:ok, value}
      _missing -> {:error, {:missing_required_activity_result_field, key}}
    end
  end

  defp required_cancellation_string(attrs, key) do
    case get_attr(attrs, key) do
      value when is_binary(value) and value != "" -> {:ok, value}
      _missing -> {:error, {:missing_required_cancellation_request_field, key}}
    end
  end

  defp activity_result_fact(intent, event_ref, status, attrs) do
    %ActivityResultFact{
      event_ref: event_ref,
      node_ref: intent.node_ref,
      activity_intent_ref: intent.activity_intent_ref,
      operation_context_ref: intent.operation_context_ref,
      operation_plan_ref: intent.operation_plan_ref,
      status: status,
      terminal?: terminal_status?(status, attrs),
      result_ref: get_attr(attrs, :result_ref),
      retryable?: get_attr(attrs, :retryable?, false),
      error_class: get_attr(attrs, :error_class),
      metadata: get_attr(attrs, :metadata, %{})
    }
  end

  defp terminal_status?(:failed, attrs), do: get_attr(attrs, :retryable?, false) == false
  defp terminal_status?(_status, _attrs), do: true

  defp apply_activity_result_fact(facts, %ActivityResultFact{terminal?: false} = fact) do
    facts
    |> delete_fact_ref(:active_node_refs, fact.node_ref)
    |> delete_fact_ref(:canceling_node_refs, fact.node_ref)
    |> put_fact_ref(:retry_node_refs, fact.node_ref)
  end

  defp apply_activity_result_fact(facts, %ActivityResultFact{} = fact) do
    facts
    |> delete_fact_ref(:active_node_refs, fact.node_ref)
    |> delete_fact_ref(:canceling_node_refs, fact.node_ref)
    |> delete_fact_ref(:retry_node_refs, fact.node_ref)
    |> put_fact_ref(fact_status_key(fact.status), fact.node_ref)
    |> put_terminal_event_ref(fact.node_ref, fact.event_ref)
  end

  defp apply_cancellation_request_fact(facts, %CancellationRequestFact{} = fact) do
    facts
    |> put_attr(:cancellation_requested_ref, fact.event_ref)
    |> put_attr(:cancellation_reason, fact.reason)
    |> put_fact_refs(:canceling_node_refs, fact.canceling_node_refs)
  end

  defp fact_status_key(:succeeded), do: :succeeded_node_refs
  defp fact_status_key(:failed), do: :failed_node_refs
  defp fact_status_key(:degraded), do: :degraded_node_refs
  defp fact_status_key(:canceled), do: :canceled_node_refs

  defp put_fact_ref(facts, key, node_ref) do
    Map.update(facts, key, [node_ref], fn refs ->
      refs
      |> List.wrap()
      |> then(&[node_ref | &1])
      |> Enum.uniq()
    end)
  end

  defp put_fact_refs(facts, key, node_refs) do
    Enum.reduce(node_refs, facts, &put_fact_ref(&2, key, &1))
  end

  defp delete_fact_ref(facts, key, node_ref) do
    Map.update(facts, key, [], fn refs -> refs |> List.wrap() |> List.delete(node_ref) end)
  end

  defp put_attr(attrs, key, value), do: Map.put(attrs, key, value)

  defp put_terminal_event_ref(facts, node_ref, event_ref) do
    Map.update(facts, :terminal_event_refs_by_node_ref, %{node_ref => event_ref}, fn refs ->
      Map.put(refs, node_ref, event_ref)
    end)
  end

  defp stable_node_key(graph, node) do
    {
      node_depth(graph, node.node_ref, []),
      node.projection_order_key,
      node.operation_role_ref,
      node.node_ref
    }
  end

  defp node_depth(graph, node_ref, seen) do
    if node_ref in seen do
      0
    else
      graph.dependencies
      |> Enum.reject(&(&1.relation == :parallel_allowed))
      |> Enum.filter(&(&1.to_node_ref == node_ref))
      |> case do
        [] ->
          0

        dependencies ->
          next_seen = [node_ref | seen]

          1 +
            (dependencies
             |> Enum.map(&node_depth(graph, &1.from_node_ref, next_seen))
             |> Enum.max())
      end
    end
  end

  defp predecessor_event_refs(graph, facts, node_ref) do
    terminal_event_refs = Map.get(facts, :terminal_event_refs_by_node_ref, %{})

    graph.dependencies
    |> Enum.reject(&(&1.relation == :parallel_allowed))
    |> Enum.filter(&(&1.to_node_ref == node_ref))
    |> Enum.reduce_while({:ok, []}, fn dependency, {:ok, refs} ->
      case predecessor_event_ref(dependency, facts, terminal_event_refs) do
        {:ok, event_ref} when is_binary(event_ref) ->
          {:cont, {:ok, [event_ref | refs]}}

        _missing ->
          {:halt,
           {:error,
            {:missing_predecessor_event_ref,
             %{node_ref: node_ref, predecessor_node_ref: dependency.from_node_ref}}}}
      end
    end)
    |> case do
      {:ok, refs} -> {:ok, refs |> Enum.reverse() |> Enum.uniq()}
      {:error, reason} -> {:error, reason}
    end
  end

  defp predecessor_event_ref(
         %{relation: :blocks_on_review, from_node_ref: from_node_ref},
         facts,
         _terminal_event_refs
       ) do
    event_ref_from(facts, :review_event_refs_by_node_ref, from_node_ref)
  end

  defp predecessor_event_ref(
         %{relation: :blocks_on_confirmation, from_node_ref: from_node_ref},
         facts,
         _terminal_event_refs
       ) do
    event_ref_from(facts, :confirmation_event_refs_by_node_ref, from_node_ref)
  end

  defp predecessor_event_ref(%{from_node_ref: from_node_ref}, _facts, terminal_event_refs) do
    Map.fetch(terminal_event_refs, from_node_ref)
  end

  defp event_ref_from(facts, event_map_key, node_ref) do
    facts
    |> Map.get(event_map_key, %{})
    |> Map.fetch(node_ref)
  end

  defp operation_plan_ref(plans_by_node, node_ref) do
    case Map.fetch(plans_by_node, node_ref) do
      {:ok, plan_ref} when is_binary(plan_ref) ->
        {:ok, plan_ref}

      _missing ->
        {:error, {:missing_operation_plan_ref, %{node_ref: node_ref}}}
    end
  end

  defp terminal_event_ref(facts, node_ref) do
    facts
    |> get_attr(:terminal_event_refs_by_node_ref, %{})
    |> Map.fetch(node_ref)
    |> case do
      {:ok, event_ref} when is_binary(event_ref) -> {:ok, event_ref}
      _missing -> {:error, {:missing_terminal_event_ref, %{node_ref: node_ref}}}
    end
  end

  defp activity_intent_ref(attrs, node_ref) do
    workflow_ref = get_attr(attrs, :workflow_run_ref, "workflow-run://unknown")
    "activity-intent://#{workflow_ref}/#{node_ref}"
  end

  defp cancellation_intent_ref(attrs, node_ref) do
    workflow_ref = get_attr(attrs, :workflow_run_ref, "workflow-run://unknown")
    "cancel-intent://#{workflow_ref}/#{node_ref}"
  end

  defp compensation_intent_ref(attrs, node_ref) do
    workflow_ref = get_attr(attrs, :workflow_run_ref, "workflow-run://unknown")
    "compensation-intent://#{workflow_ref}/#{node_ref}"
  end

  defp idempotency_key(attrs, action, node_ref) do
    base = get_attr(attrs, :idempotency_key, "operation-graph")
    "#{base}:#{action}:#{node_ref}"
  end

  defp policy_for(attrs, key, node_ref) do
    attrs
    |> get_attr(key, %{})
    |> Map.get(node_ref, %{})
  end

  defp cancellation_reason(attrs), do: get_attr(attrs, :reason, "operation_graph_cancellation")

  defp cancellation_request_ref(facts) do
    case get_attr(facts, :cancellation_requested_ref) do
      value when is_binary(value) and value != "" -> {:ok, value}
      _missing -> {:error, :missing_cancellation_request_ref}
    end
  end

  defp required_string(attrs, key) do
    case get_attr(attrs, key) do
      value when is_binary(value) and value != "" -> {:ok, value}
      _missing -> {:error, {:missing_required_activity_intent_field, key}}
    end
  end

  defp required_map(attrs, key) do
    case get_attr(attrs, key) do
      value when is_map(value) -> {:ok, value}
      _missing -> {:error, {:missing_required_activity_intent_field, key}}
    end
  end

  defp get_attr(attrs, key, default \\ nil)

  defp get_attr(attrs, key, default) when is_map(attrs) do
    case Map.fetch(attrs, key) do
      {:ok, value} -> value
      :error -> Map.get(attrs, Atom.to_string(key), default)
    end
  end

  defp get_attr(attrs, key, default) when is_list(attrs), do: Keyword.get(attrs, key, default)

  defp predecessors_satisfied?(dependencies, node_ref, facts) do
    dependencies
    |> Enum.filter(&(&1.to_node_ref == node_ref))
    |> Enum.all?(&dependency_satisfied?(&1, facts))
  end

  defp dependency_satisfied?(%{relation: :parallel_allowed}, _facts), do: true

  defp dependency_satisfied?(dependency, facts) do
    optional_terminal?(dependency, facts) or relation_satisfied?(dependency, facts)
  end

  defp relation_satisfied?(%{relation: relation, from_node_ref: from_node_ref}, facts)
       when relation in [:before, :after, :blocks_on_success] do
    from_node_ref in facts.succeeded_node_refs
  end

  defp relation_satisfied?(%{relation: :blocks_on_review, from_node_ref: from_node_ref}, facts),
    do: from_node_ref in facts.reviewed_node_refs

  defp relation_satisfied?(
         %{relation: :blocks_on_confirmation, from_node_ref: from_node_ref},
         facts
       ),
       do: from_node_ref in facts.confirmed_node_refs

  defp optional_terminal?(%{completion_policy: :optional, from_node_ref: from_node_ref}, facts) do
    from_node_ref in facts.succeeded_node_refs or
      from_node_ref in facts.degraded_node_refs or
      from_node_ref in facts.failed_node_refs or
      from_node_ref in facts.canceled_node_refs
  end

  defp optional_terminal?(_dependency, _facts), do: false

  defp completed?(facts, node_ref) do
    node_ref in facts.succeeded_node_refs or
      node_ref in facts.degraded_node_refs or
      node_ref in facts.failed_node_refs or
      node_ref in facts.canceled_node_refs
  end

  defp in_progress?(facts, node_ref) do
    node_ref in facts.active_node_refs or node_ref in facts.retry_node_refs or
      node_ref in facts.canceling_node_refs
  end

  defp cancellation_requested?(facts), do: is_binary(facts.cancellation_requested_ref)

  defp cancelable_node_refs(graph, facts) do
    candidate_refs =
      facts.active_node_refs ++
        facts.retry_node_refs ++
        facts.canceling_node_refs

    graph.nodes
    |> Enum.filter(&(&1.node_ref in candidate_refs))
    |> Enum.reject(&completed?(facts, &1.node_ref))
    |> Enum.sort_by(&stable_node_key(graph, &1))
    |> Enum.map(& &1.node_ref)
  end

  defp compensatable_node_refs(graph, facts, attrs) do
    facts = normalize_facts(facts)
    compensated_node_refs = fact_refs(facts, :compensated_node_refs)

    graph.nodes
    |> Enum.filter(&compensatable_node?(&1, facts, attrs, compensated_node_refs))
    |> Enum.map(& &1.node_ref)
  end

  defp compensatable_node?(node, facts, attrs, compensated_node_refs) do
    policy = policy_for(attrs, :compensation_policies_by_node_ref, node.node_ref)

    node.node_ref in compensatable_completed_node_refs(facts) and
      node.node_ref not in compensated_node_refs and
      policy != %{} and
      policy_enabled?(policy)
  end

  defp compensatable_completed_node_refs(facts) do
    facts.succeeded_node_refs ++ facts.degraded_node_refs
  end

  defp policy_enabled?(policy), do: get_attr(policy, :enabled?, true) != false

  defp normalize_facts(facts) do
    terminal = fact_refs(facts, :terminal_node_refs)

    %{
      succeeded_node_refs: Enum.uniq(terminal ++ fact_refs(facts, :succeeded_node_refs)),
      reviewed_node_refs: fact_refs(facts, :reviewed_node_refs),
      confirmed_node_refs: fact_refs(facts, :confirmed_node_refs),
      degraded_node_refs: fact_refs(facts, :degraded_node_refs),
      failed_node_refs: fact_refs(facts, :failed_node_refs),
      canceled_node_refs: fact_refs(facts, :canceled_node_refs),
      active_node_refs: fact_refs(facts, :active_node_refs),
      retry_node_refs: fact_refs(facts, :retry_node_refs),
      canceling_node_refs: fact_refs(facts, :canceling_node_refs),
      compensated_node_refs: fact_refs(facts, :compensated_node_refs),
      cancellation_requested_ref: get_attr(facts, :cancellation_requested_ref)
    }
  end

  defp fact_refs(facts, key), do: facts |> get_attr(key, []) |> List.wrap()
end
