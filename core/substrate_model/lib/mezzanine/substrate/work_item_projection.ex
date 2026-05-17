defmodule Mezzanine.Substrate.WorkItemProjection do
  @moduledoc "Pure WorkItem projection from workflow and operation graph facts."

  alias Mezzanine.Substrate.OperationGraph
  alias Mezzanine.Substrate.WorkflowRun
  alias Mezzanine.Substrate.WorkItem

  @spec project(WorkItem.t(), WorkflowRun.t(), OperationGraph.t(), map()) ::
          {:ok, WorkItem.t()} | {:error, term()}
  def project(
        %WorkItem{} = work_item,
        %WorkflowRun{} = workflow,
        %OperationGraph{} = graph,
        facts
      )
      when is_map(facts) do
    normalized_facts = normalize_facts(facts)
    state = projected_state(workflow.state, graph, normalized_facts)
    metadata = projection_metadata(graph, normalized_facts)
    {:ok, %{work_item | state: state, metadata: Map.merge(work_item.metadata, metadata)}}
  end

  defp projected_state(workflow_state, graph, facts) do
    terminal_projected_state(workflow_state) ||
      immediate_projected_state(facts) ||
      graph_wait_projected_state(graph, facts) ||
      workflow_projected_state(workflow_state, graph, facts)
  end

  defp terminal_projected_state(state) when state in [:completed, :failed, :cancelled, :archived],
    do: state

  defp terminal_projected_state(_state), do: nil

  defp immediate_projected_state(%{active_node_refs: [_ | _]}), do: :in_flight
  defp immediate_projected_state(%{retry_node_refs: [_ | _]}), do: :retry_scheduled

  defp immediate_projected_state(facts),
    do: if(Map.has_key?(facts, :review_ref), do: :awaiting_review)

  defp graph_wait_projected_state(graph, facts) do
    cond do
      required_failed_node_refs(graph, facts) != [] -> :failed
      required_canceled_node_refs(graph, facts) != [] -> :cancelled
      review_pending_node_refs(graph, facts) != [] -> :awaiting_review
      confirmation_pending_node_refs(graph, facts) != [] -> :blocked
      true -> nil
    end
  end

  defp workflow_projected_state(state, _graph, _facts) when state in [:planned, :admitted],
    do: :queued

  defp workflow_projected_state(:running, graph, facts), do: running_state(graph, facts)
  defp workflow_projected_state(state, _graph, _facts), do: state

  defp running_state(graph, facts) do
    case OperationGraph.ready_node_refs(graph, facts) do
      [] -> :in_flight
      _ready -> :queued
    end
  end

  defp projection_metadata(graph, facts) do
    fact_metadata =
      facts
      |> Map.take([
        :active_node_refs,
        :succeeded_node_refs,
        :terminal_node_refs,
        :degraded_node_refs,
        :failed_node_refs,
        :canceled_node_refs,
        :retry_node_refs
      ])
      |> Map.new(fn {key, value} -> {key, List.wrap(value)} end)

    Map.merge(fact_metadata, %{
      ready_node_refs: OperationGraph.ready_node_refs(graph, facts),
      waiting_review_node_refs: review_pending_node_refs(graph, facts),
      waiting_confirmation_node_refs: confirmation_pending_node_refs(graph, facts),
      required_failed_node_refs: required_failed_node_refs(graph, facts),
      required_canceled_node_refs: required_canceled_node_refs(graph, facts)
    })
  end

  defp normalize_facts(facts) do
    terminal_node_refs = fact_refs(facts, :terminal_node_refs)

    facts
    |> Map.put(:terminal_node_refs, terminal_node_refs)
    |> Map.put(
      :succeeded_node_refs,
      Enum.uniq(terminal_node_refs ++ fact_refs(facts, :succeeded_node_refs))
    )
    |> Map.put(:reviewed_node_refs, fact_refs(facts, :reviewed_node_refs))
    |> Map.put(:confirmed_node_refs, fact_refs(facts, :confirmed_node_refs))
    |> Map.put(:degraded_node_refs, fact_refs(facts, :degraded_node_refs))
    |> Map.put(:failed_node_refs, fact_refs(facts, :failed_node_refs))
    |> Map.put(:canceled_node_refs, fact_refs(facts, :canceled_node_refs))
    |> Map.put(:active_node_refs, fact_refs(facts, :active_node_refs))
    |> Map.put(:retry_node_refs, fact_refs(facts, :retry_node_refs))
  end

  defp review_pending_node_refs(graph, facts) do
    graph.dependencies
    |> Enum.filter(&(&1.relation == :blocks_on_review and completed?(facts, &1.from_node_ref)))
    |> Enum.reject(&(&1.from_node_ref in facts.reviewed_node_refs))
    |> Enum.map(& &1.from_node_ref)
    |> Enum.uniq()
    |> Enum.sort()
  end

  defp confirmation_pending_node_refs(graph, facts) do
    graph.dependencies
    |> Enum.filter(
      &(&1.relation == :blocks_on_confirmation and completed?(facts, &1.from_node_ref))
    )
    |> Enum.reject(&(&1.from_node_ref in facts.confirmed_node_refs))
    |> Enum.map(& &1.from_node_ref)
    |> Enum.uniq()
    |> Enum.sort()
  end

  defp required_failed_node_refs(graph, facts) do
    required_blocking_node_refs(graph, facts.failed_node_refs)
  end

  defp required_canceled_node_refs(graph, facts) do
    required_blocking_node_refs(graph, facts.canceled_node_refs)
  end

  defp required_blocking_node_refs(graph, node_refs) do
    node_refs
    |> Enum.filter(&required_blocking_node_ref?(graph, &1))
    |> Enum.uniq()
    |> Enum.sort()
  end

  defp required_blocking_node_ref?(graph, node_ref) do
    outgoing_dependencies = Enum.filter(graph.dependencies, &(&1.from_node_ref == node_ref))

    outgoing_dependencies == [] or
      Enum.any?(outgoing_dependencies, &(&1.completion_policy != :optional))
  end

  defp completed?(facts, node_ref) do
    node_ref in facts.succeeded_node_refs or
      node_ref in facts.degraded_node_refs or
      node_ref in facts.failed_node_refs or
      node_ref in facts.canceled_node_refs
  end

  defp fact_refs(facts, key), do: facts |> Map.get(key, []) |> List.wrap()
end
