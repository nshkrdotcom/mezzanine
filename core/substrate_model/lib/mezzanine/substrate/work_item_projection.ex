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
    state = projected_state(workflow.state, graph, facts)
    metadata = projection_metadata(facts)
    {:ok, %{work_item | state: state, metadata: Map.merge(work_item.metadata, metadata)}}
  end

  defp projected_state(:completed, _graph, _facts), do: :completed
  defp projected_state(:failed, _graph, _facts), do: :failed
  defp projected_state(:cancelled, _graph, _facts), do: :cancelled
  defp projected_state(:archived, _graph, _facts), do: :archived

  defp projected_state(_workflow_state, _graph, %{active_node_refs: active}) when active != [],
    do: :in_flight

  defp projected_state(_workflow_state, _graph, %{review_ref: _review_ref}), do: :awaiting_review
  defp projected_state(_workflow_state, _graph, %{retry_node_refs: [_ | _]}), do: :retry_scheduled
  defp projected_state(:planned, _graph, _facts), do: :queued
  defp projected_state(:admitted, _graph, _facts), do: :queued

  defp projected_state(:running, graph, facts) do
    case OperationGraph.ready_node_refs(graph, facts) do
      [] -> :in_flight
      _ready -> :queued
    end
  end

  defp projected_state(state, _graph, _facts), do: state

  defp projection_metadata(facts) do
    facts
    |> Map.take([:active_node_refs, :terminal_node_refs, :degraded_node_refs, :retry_node_refs])
    |> Map.new(fn {key, value} -> {key, List.wrap(value)} end)
  end
end
