defmodule Mezzanine.WorkflowRuntime.OperationGraphWorkflowStepTest do
  use ExUnit.Case, async: true

  alias Mezzanine.WorkflowRuntime.{OperationGraphExecutor, OperationGraphWorkflowStep}

  test "records result fact before deriving dependent activity intents" do
    graph =
      graph!(
        nodes: [
          node!("node://source", "role://source", :source_read, 1),
          node!("node://publication", "role://publication", :source_write, 2)
        ],
        dependencies: [
          dependency!("node://source", "node://publication", :blocks_on_success)
        ]
      )

    assert {:ok, step} =
             OperationGraphWorkflowStep.apply_activity_result(
               graph,
               %{active_node_refs: ["node://source"]},
               activity_intent!("node://source", "operation-plan://tenant/run-a/source"),
               %{
                 event_ref: "event://source/succeeded",
                 status: :succeeded,
                 result_ref: "result://source/1"
               },
               schedule_attrs(%{
                 "node://publication" => "operation-plan://tenant/run-a/publication"
               })
             )

    assert step.recorded_fact.node_ref == "node://source"
    assert step.recorded_fact.event_ref == "event://source/succeeded"
    assert step.facts.succeeded_node_refs == ["node://source"]
    assert step.state == :dispatching

    assert [next_intent] = step.ready_activity_intents
    assert next_intent.node_ref == "node://publication"
    assert next_intent.operation_plan_ref == "operation-plan://tenant/run-a/publication"
    assert next_intent.predecessor_event_refs == ["event://source/succeeded"]
  end

  test "retryable failure records retry facts and leaves the workflow waiting" do
    graph =
      graph!(
        nodes: [
          node!("node://source", "role://source", :source_read, 1),
          node!("node://publication", "role://publication", :source_write, 2)
        ],
        dependencies: [
          dependency!("node://source", "node://publication", :blocks_on_success)
        ]
      )

    assert {:ok, step} =
             OperationGraphWorkflowStep.apply_activity_result(
               graph,
               %{active_node_refs: ["node://source"]},
               activity_intent!("node://source", "operation-plan://tenant/run-a/source"),
               %{
                 event_ref: "event://source/retry",
                 status: :failed,
                 retryable?: true,
                 error_class: :temporary_unavailable
               },
               schedule_attrs(%{
                 "node://publication" => "operation-plan://tenant/run-a/publication"
               })
             )

    assert step.recorded_fact.status == :failed
    refute step.recorded_fact.terminal?
    assert step.facts.retry_node_refs == ["node://source"]
    assert step.ready_activity_intents == []
    assert step.state == :waiting
  end

  test "terminal final-node result marks the step terminal" do
    graph =
      graph!(
        nodes: [
          node!("node://publication", "role://publication", :source_write, 1)
        ],
        dependencies: []
      )

    assert {:ok, step} =
             OperationGraphWorkflowStep.apply_activity_result(
               graph,
               %{active_node_refs: ["node://publication"]},
               activity_intent!(
                 "node://publication",
                 "operation-plan://tenant/run-a/publication"
               ),
               %{
                 event_ref: "event://publication/succeeded",
                 status: :succeeded,
                 result_ref: "result://publication/1"
               },
               schedule_attrs(%{})
             )

    assert step.state == :terminal
    assert step.ready_activity_intents == []
    assert step.facts.succeeded_node_refs == ["node://publication"]
  end

  defp graph!(opts) do
    %{
      graph_ref: "operation-graph://tenant/run-a",
      nodes: Keyword.fetch!(opts, :nodes),
      dependencies: Keyword.fetch!(opts, :dependencies)
    }
  end

  defp node!(node_ref, role_ref, operation_class, order_key) do
    %{
      node_ref: node_ref,
      operation_role_ref: role_ref,
      operation_class: operation_class,
      projection_order_key: order_key
    }
  end

  defp dependency!(from, to, relation) do
    %{
      dependency_ref: "dependency://#{from}/#{to}/#{Atom.to_string(relation)}",
      from_node_ref: from,
      to_node_ref: to,
      relation: relation,
      completion_policy: :required
    }
  end

  defp activity_intent!(node_ref, operation_plan_ref) do
    %OperationGraphExecutor.ActivityIntent{
      activity_intent_ref: "activity-intent://tenant/run-a/#{node_ref}",
      node_ref: node_ref,
      operation_context_ref: "operation-context://tenant/request-a",
      operation_plan_ref: operation_plan_ref,
      predecessor_event_refs: [],
      retry_policy: %{},
      timeout_policy: %{},
      cancellation_policy: %{},
      metadata: %{}
    }
  end

  defp schedule_attrs(operation_plans_by_node_ref) do
    %{
      workflow_run_ref: "workflow-run://tenant/run-a",
      operation_context_ref: "operation-context://tenant/request-a",
      operation_plans_by_node_ref: operation_plans_by_node_ref
    }
  end
end
