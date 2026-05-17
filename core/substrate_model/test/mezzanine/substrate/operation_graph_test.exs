defmodule Mezzanine.Substrate.OperationGraphTest do
  use ExUnit.Case, async: true

  alias Mezzanine.Substrate.OperationDependency
  alias Mezzanine.Substrate.OperationGraph
  alias Mezzanine.Substrate.OperationNode
  alias Mezzanine.Substrate.WorkItemProjection
  alias Mezzanine.Substrate.WorkflowRun
  alias Mezzanine.Substrate.WorkItem

  test "calculates ready nodes in deterministic order" do
    graph = graph!()

    assert OperationGraph.ready_node_refs(graph, %{}) == [
             "node://source",
             "node://runtime",
             "node://evidence"
           ]

    facts = %{terminal_node_refs: ["node://source", "node://runtime", "node://evidence"]}
    assert OperationGraph.ready_node_refs(graph, facts) == ["node://publication"]
  end

  test "join waits for required predecessors and respects degraded optional branches" do
    graph = graph!()

    facts = %{terminal_node_refs: ["node://source", "node://runtime"]}
    assert OperationGraph.ready_node_refs(graph, facts) == ["node://evidence"]

    facts = %{
      terminal_node_refs: ["node://source", "node://runtime"],
      degraded_node_refs: ["node://evidence"]
    }

    assert OperationGraph.ready_node_refs(graph, facts) == ["node://publication"]
  end

  test "review and confirmation relations wait for explicit gate facts" do
    graph =
      graph!(
        dependencies: [
          dependency!("node://source", "node://publication", :blocks_on_confirmation, :required),
          dependency!("node://runtime", "node://publication", :blocks_on_review, :required),
          dependency!("node://evidence", "node://publication", :parallel_allowed, :optional)
        ]
      )

    facts = %{terminal_node_refs: ["node://source", "node://runtime", "node://evidence"]}
    assert OperationGraph.ready_node_refs(graph, facts) == []

    facts = %{
      terminal_node_refs: ["node://source", "node://runtime", "node://evidence"],
      reviewed_node_refs: ["node://runtime"],
      confirmed_node_refs: ["node://source"]
    }

    assert OperationGraph.ready_node_refs(graph, facts) == ["node://publication"]
  end

  test "required failure and cancellation block while optional branches allow partial progress" do
    graph = graph!()

    facts = %{
      succeeded_node_refs: ["node://source", "node://runtime"],
      failed_node_refs: ["node://evidence"]
    }

    assert OperationGraph.ready_node_refs(graph, facts) == ["node://publication"]

    facts = %{
      succeeded_node_refs: ["node://source"],
      failed_node_refs: ["node://runtime"],
      canceled_node_refs: ["node://evidence"]
    }

    assert OperationGraph.ready_node_refs(graph, facts) == []
  end

  test "active and retrying nodes are not emitted as ready" do
    graph = graph!()

    assert OperationGraph.ready_node_refs(graph, %{
             active_node_refs: ["node://source"],
             retry_node_refs: ["node://runtime"]
           }) == ["node://evidence"]
  end

  test "work item projection is derived from workflow and graph facts" do
    {:ok, work_item} =
      WorkItem.new(%{
        work_item_ref: "work-item://tenant-a/work-a",
        operation_context_ref: "operation-context://tenant-a/request-a",
        workflow_run_ref: "workflow-run://tenant-a/run-a",
        state: :queued
      })

    {:ok, workflow} =
      WorkflowRun.new(%{
        workflow_run_ref: "workflow-run://tenant-a/run-a",
        operation_context_ref: "operation-context://tenant-a/request-a",
        work_item_ref: work_item.work_item_ref,
        state: :running
      })

    assert {:ok, projected} =
             WorkItemProjection.project(work_item, workflow, graph!(), %{
               active_node_refs: ["node://runtime"]
             })

    assert projected.state == :in_flight
    assert projected.metadata.active_node_refs == ["node://runtime"]

    assert {:ok, completed} =
             WorkItemProjection.project(work_item, %{workflow | state: :completed}, graph!(), %{})

    assert completed.state == :completed

    assert {:ok, review_wait} =
             WorkItemProjection.project(work_item, %{workflow | state: :running}, graph!(), %{
               review_ref: "review://tenant-a/review-a"
             })

    assert review_wait.state == :awaiting_review

    assert {:ok, retry_wait} =
             WorkItemProjection.project(work_item, %{workflow | state: :running}, graph!(), %{
               retry_node_refs: ["node://runtime"]
             })

    assert retry_wait.state == :retry_scheduled

    assert {:ok, degraded} =
             WorkItemProjection.project(work_item, %{workflow | state: :degraded}, graph!(), %{
               degraded_node_refs: ["node://evidence"]
             })

    assert degraded.state == :degraded
    assert degraded.metadata.degraded_node_refs == ["node://evidence"]

    assert {:ok, cancelled} =
             WorkItemProjection.project(work_item, %{workflow | state: :cancelled}, graph!(), %{})

    assert cancelled.state == :cancelled

    assert {:ok, expired} =
             WorkItemProjection.project(work_item, %{workflow | state: :expired}, graph!(), %{})

    assert expired.state == :expired

    assert {:ok, rework} =
             WorkItemProjection.project(
               work_item,
               %{workflow | state: :rework_requested},
               graph!(),
               %{}
             )

    assert rework.state == :rework_requested
  end

  test "work item projection makes graph waits and branch failures operator-visible" do
    {:ok, work_item} =
      WorkItem.new(%{
        work_item_ref: "work-item://tenant-a/work-a",
        operation_context_ref: "operation-context://tenant-a/request-a",
        workflow_run_ref: "workflow-run://tenant-a/run-a",
        state: :queued
      })

    {:ok, workflow} =
      WorkflowRun.new(%{
        workflow_run_ref: "workflow-run://tenant-a/run-a",
        operation_context_ref: "operation-context://tenant-a/request-a",
        work_item_ref: work_item.work_item_ref,
        state: :running
      })

    review_graph =
      graph!(
        dependencies: [
          dependency!("node://runtime", "node://publication", :blocks_on_review, :required),
          dependency!(
            "node://source",
            "node://publication",
            :blocks_on_confirmation,
            :required
          ),
          dependency!("node://evidence", "node://publication", :blocks_on_success, :optional)
        ]
      )

    assert {:ok, review_wait} =
             WorkItemProjection.project(work_item, workflow, review_graph, %{
               succeeded_node_refs: ["node://source", "node://runtime", "node://evidence"],
               confirmed_node_refs: ["node://source"]
             })

    assert review_wait.state == :awaiting_review
    assert review_wait.metadata.waiting_review_node_refs == ["node://runtime"]
    assert review_wait.metadata.waiting_confirmation_node_refs == []
    assert review_wait.metadata.ready_node_refs == []

    assert {:ok, confirmation_wait} =
             WorkItemProjection.project(work_item, workflow, review_graph, %{
               succeeded_node_refs: ["node://source", "node://runtime", "node://evidence"],
               reviewed_node_refs: ["node://runtime"]
             })

    assert confirmation_wait.state == :blocked
    assert confirmation_wait.metadata.waiting_confirmation_node_refs == ["node://source"]
    assert confirmation_wait.metadata.ready_node_refs == []

    assert {:ok, failed_required_branch} =
             WorkItemProjection.project(work_item, workflow, graph!(), %{
               succeeded_node_refs: ["node://source"],
               failed_node_refs: ["node://runtime"]
             })

    assert failed_required_branch.state == :failed
    assert failed_required_branch.metadata.required_failed_node_refs == ["node://runtime"]

    assert {:ok, optional_branch_failure} =
             WorkItemProjection.project(work_item, workflow, graph!(), %{
               succeeded_node_refs: ["node://source", "node://runtime"],
               failed_node_refs: ["node://evidence"]
             })

    assert optional_branch_failure.state == :queued
    assert optional_branch_failure.metadata.required_failed_node_refs == []
    assert optional_branch_failure.metadata.ready_node_refs == ["node://publication"]
  end

  defp graph!(opts \\ []) do
    nodes = [
      node!("node://source", :source, 1),
      node!("node://runtime", :runtime_operation, 2),
      node!("node://evidence", :evidence, 3),
      node!("node://publication", :publication, 4)
    ]

    dependencies =
      Keyword.get(opts, :dependencies, [
        dependency!("node://source", "node://publication", :blocks_on_success, :required),
        dependency!("node://runtime", "node://publication", :blocks_on_success, :required),
        dependency!("node://evidence", "node://publication", :blocks_on_success, :optional)
      ])

    {:ok, graph} =
      OperationGraph.new(%{
        graph_ref: "operation-graph://tenant-a/run-a",
        nodes: nodes,
        dependencies: dependencies
      })

    graph
  end

  defp node!(node_ref, operation_class, order_key) do
    {:ok, node} =
      OperationNode.new(%{
        node_ref: node_ref,
        operation_role_ref: "operation-role://#{Atom.to_string(operation_class)}",
        operation_class: operation_class,
        projection_order_key: order_key
      })

    node
  end

  defp dependency!(from, to, relation, completion_policy) do
    {:ok, dependency} =
      OperationDependency.new(%{
        dependency_ref: "dependency://#{from}/#{to}",
        from_node_ref: from,
        to_node_ref: to,
        relation: relation,
        completion_policy: completion_policy
      })

    dependency
  end
end
