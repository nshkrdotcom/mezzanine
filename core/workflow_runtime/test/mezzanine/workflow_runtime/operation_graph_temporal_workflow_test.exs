defmodule Mezzanine.WorkflowRuntime.OperationGraphTemporalWorkflowTest do
  use ExUnit.Case, async: false
  use Temporalex.Testing

  alias Mezzanine.WorkflowRuntime.OperationGraphTemporalWorkflow

  test "Temporal workflow drains generic operation graph nodes through one activity boundary" do
    assert {:ok, result} =
             run_workflow(Mezzanine.Workflows.OperationGraphRun, workflow_input(),
               activities: %{
                 Mezzanine.Activities.ExecuteOperationGraphNode =>
                   &Mezzanine.Activities.ExecuteOperationGraphNode.perform/1
               }
             )

    assert result.workflow_state == "completed"
    assert result.graph_ref == "operation-graph://tenant/run-a"
    assert result.operation_context_ref == "operation-context://tenant/request-a"

    assert result.recorded_node_refs == [
             "node://source",
             "node://review",
             "node://publication"
           ]

    assert result.activity_result_event_refs == [
             "event://source/succeeded",
             "event://review/succeeded",
             "event://publication/succeeded"
           ]

    assert result.facts.succeeded_node_refs == [
             "node://publication",
             "node://review",
             "node://source"
           ]

    assert_activity_called(Mezzanine.Activities.ExecuteOperationGraphNode)
    assert get_workflow_state().workflow_state == "completed"

    calls = get_activity_calls()
    assert length(calls) == 3

    {_activity, first_payload} = hd(calls)
    assert first_payload.activity_intent.node_ref == "node://source"
    assert first_payload.activity_intent.operation_plan_ref == "operation-plan://tenant/source"
    assert first_payload.activity_intent.predecessor_event_refs == []
  end

  test "normalizes JSON-shaped workflow input and activity result attrs" do
    assert {:ok, input} =
             OperationGraphTemporalWorkflow.new_input(json_shaped_workflow_input())

    assert input.graph.graph_ref == "operation-graph://tenant/run-json"

    assert [%{relation: :blocks_on_success, completion_policy: :required}] =
             input.graph.dependencies

    assert input.schedule_attrs.operation_context_ref ==
             "operation-context://tenant/request-json"

    assert input.activity_result_attrs_by_node_ref["node://json-source"].status == "succeeded"

    assert {:ok, attrs} =
             OperationGraphTemporalWorkflow.default_activity_result_attrs(%{
               "graph_ref" => input.graph.graph_ref,
               "activity_intent" => %{"node_ref" => "node://json-source"},
               "activity_result_attrs_by_node_ref" => input.activity_result_attrs_by_node_ref
             })

    assert attrs.status == "succeeded"
    assert attrs.event_ref == "event://json-source/succeeded"
  end

  test "generic node activity supplies deterministic default result attrs" do
    assert {:ok, attrs} =
             Mezzanine.Activities.ExecuteOperationGraphNode.perform(%{
               graph_ref: "operation-graph://tenant/run-default",
               activity_intent: %{node_ref: "node://default"},
               activity_result_attrs_by_node_ref: %{}
             })

    assert attrs.status == :succeeded

    assert attrs.event_ref ==
             "operation-graph-event://operation-graph://tenant/run-default/node://default/succeeded"

    assert attrs.result_ref ==
             "operation-graph-result://operation-graph://tenant/run-default/node://default"
  end

  defp workflow_input do
    %{
      operation_graph: %{
        graph_ref: "operation-graph://tenant/run-a",
        nodes: [
          node!("node://source", "role://source", :source_read, 1),
          node!("node://review", "role://review", :runtime_operation, 2),
          node!("node://publication", "role://publication", :source_write, 3)
        ],
        dependencies: [
          dependency!("node://source", "node://review", :blocks_on_success),
          dependency!("node://review", "node://publication", :blocks_on_success)
        ]
      },
      facts: %{},
      schedule_attrs: %{
        workflow_run_ref: "workflow-run://tenant/run-a",
        operation_context_ref: "operation-context://tenant/request-a",
        operation_plans_by_node_ref: %{
          "node://source" => "operation-plan://tenant/source",
          "node://review" => "operation-plan://tenant/review",
          "node://publication" => "operation-plan://tenant/publication"
        }
      },
      activity_result_attrs_by_node_ref: %{
        "node://source" => %{
          event_ref: "event://source/succeeded",
          status: :succeeded,
          result_ref: "result://source/1"
        },
        "node://review" => %{
          event_ref: "event://review/succeeded",
          status: :succeeded,
          result_ref: "result://review/1"
        },
        "node://publication" => %{
          event_ref: "event://publication/succeeded",
          status: :succeeded,
          result_ref: "result://publication/1"
        }
      }
    }
  end

  defp json_shaped_workflow_input do
    %{
      "operation_graph" => %{
        "graph_ref" => "operation-graph://tenant/run-json",
        "nodes" => [
          %{
            "node_ref" => "node://json-source",
            "operation_role_ref" => "role://json-source",
            "operation_class" => "source_read",
            "projection_order_key" => 1
          },
          %{
            "node_ref" => "node://json-publication",
            "operation_role_ref" => "role://json-publication",
            "operation_class" => "source_write",
            "projection_order_key" => 2
          }
        ],
        "dependencies" => [
          %{
            "dependency_ref" => "dependency://json-source/json-publication",
            "from_node_ref" => "node://json-source",
            "to_node_ref" => "node://json-publication",
            "relation" => "blocks_on_success",
            "completion_policy" => "required"
          }
        ]
      },
      "schedule_attrs" => %{
        "workflow_run_ref" => "workflow-run://tenant/run-json",
        "operation_context_ref" => "operation-context://tenant/request-json",
        "operation_plans_by_node_ref" => %{
          "node://json-source" => "operation-plan://tenant/json-source",
          "node://json-publication" => "operation-plan://tenant/json-publication"
        }
      },
      "activity_result_attrs_by_node_ref" => %{
        "node://json-source" => %{
          "event_ref" => "event://json-source/succeeded",
          "status" => "succeeded"
        }
      }
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
end
