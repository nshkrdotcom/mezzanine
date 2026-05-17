defmodule Mezzanine.Substrate.OperationNode do
  @moduledoc "Operation graph node."
  use Mezzanine.Substrate.StructSupport,
    required: [:node_ref, :operation_role_ref, :operation_class, :projection_order_key],
    optional: [completion_policy: :required, failure_policy: :fail_closed, metadata: %{}]
end

defmodule Mezzanine.Substrate.OperationDependency do
  @moduledoc "Operation graph dependency edge."
  use Mezzanine.Substrate.StructSupport,
    required: [:dependency_ref, :from_node_ref, :to_node_ref, :relation, :completion_policy],
    optional: [failure_policy: :fail_closed, metadata: %{}]
end

defmodule Mezzanine.Substrate.OperationGraph do
  @moduledoc "Pure operation dependency graph."

  alias Mezzanine.Substrate.Builder
  alias Mezzanine.Substrate.OperationDependency
  alias Mezzanine.Substrate.OperationNode

  @enforce_keys [:graph_ref, :nodes, :dependencies]
  defstruct @enforce_keys ++ [metadata: %{}]

  @type t :: %__MODULE__{}

  @relations [
    :before,
    :after,
    :parallel_allowed,
    :blocks_on_success,
    :blocks_on_review,
    :blocks_on_confirmation
  ]

  @spec fields() :: [atom()]
  def fields, do: @enforce_keys ++ [:metadata]

  @spec required_fields() :: [atom()]
  def required_fields, do: @enforce_keys

  @spec new(map() | keyword()) :: {:ok, t()} | {:error, term()}
  def new(attrs),
    do: Builder.build(__MODULE__, attrs, @enforce_keys, validate: [&validate_graph/1])

  @spec ready_node_refs(t(), map()) :: [String.t()]
  def ready_node_refs(%__MODULE__{} = graph, facts) when is_map(facts) do
    normalized_facts = normalize_facts(facts)

    graph.nodes
    |> Enum.reject(fn node ->
      completed?(normalized_facts, node.node_ref) or in_progress?(normalized_facts, node.node_ref)
    end)
    |> Enum.filter(&predecessors_satisfied?(graph.dependencies, &1.node_ref, normalized_facts))
    |> Enum.sort_by(&{&1.projection_order_key, &1.operation_role_ref, &1.node_ref})
    |> Enum.map(& &1.node_ref)
  end

  defp validate_graph(%{nodes: nodes, dependencies: dependencies})
       when is_list(nodes) and is_list(dependencies) do
    with :ok <- validate_nodes(nodes) do
      validate_dependencies(dependencies, Enum.map(nodes, & &1.node_ref))
    end
  end

  defp validate_graph(_attrs), do: {:error, :invalid_operation_graph}

  defp validate_nodes(nodes) do
    if Enum.all?(nodes, &match?(%OperationNode{}, &1)) do
      :ok
    else
      {:error, :invalid_operation_node}
    end
  end

  defp validate_dependencies(dependencies, node_refs) do
    cond do
      not Enum.all?(dependencies, &match?(%OperationDependency{}, &1)) ->
        {:error, :invalid_operation_dependency}

      Enum.any?(dependencies, &(&1.relation not in @relations)) ->
        {:error, :unsupported_dependency_relation}

      Enum.any?(
        dependencies,
        &(&1.from_node_ref not in node_refs or &1.to_node_ref not in node_refs)
      ) ->
        {:error, :unknown_dependency_node}

      true ->
        :ok
    end
  end

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
    node_ref in facts.active_node_refs or node_ref in facts.retry_node_refs
  end

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
      retry_node_refs: fact_refs(facts, :retry_node_refs)
    }
  end

  defp fact_refs(facts, key), do: facts |> Map.get(key, []) |> List.wrap()
end
