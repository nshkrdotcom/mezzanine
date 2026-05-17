defmodule Mezzanine.WorkflowRuntime.OperationGraphExecutorTest do
  use ExUnit.Case, async: true

  alias Mezzanine.WorkflowRuntime.OperationGraphExecutor

  test "orders ready nodes by graph depth, projection order, role ref, and node ref" do
    graph =
      graph!(
        nodes: [
          node!("node://late-root-b", "role://b", :source_read, 3),
          node!("node://late-root-a", "role://a", :source_read, 3),
          node!("node://first-root", "role://first", :source_read, 1),
          node!("node://dependent", "role://dependent", :source_write, 1)
        ],
        dependencies: [
          dependency!("node://first-root", "node://dependent", :blocks_on_success)
        ]
      )

    facts = %{
      succeeded_node_refs: ["node://first-root"],
      terminal_event_refs_by_node_ref: %{"node://first-root" => "event://first/succeeded"}
    }

    assert OperationGraphExecutor.ready_node_refs(graph, facts) == [
             "node://late-root-a",
             "node://late-root-b",
             "node://dependent"
           ]
  end

  test "review and confirmation gates use recorded deterministic graph facts" do
    graph =
      graph!(
        nodes: [
          node!("node://review", "role://review", :runtime_operation, 1),
          node!("node://publication", "role://publication", :source_write, 2)
        ],
        dependencies: [
          dependency!("node://review", "node://publication", :blocks_on_review),
          dependency!("node://review", "node://publication", :blocks_on_confirmation)
        ]
      )

    assert OperationGraphExecutor.ready_node_refs(graph, %{
             succeeded_node_refs: ["node://review"],
             reviewed_node_refs: ["node://review"]
           }) == []

    assert OperationGraphExecutor.ready_node_refs(graph, %{
             succeeded_node_refs: ["node://review"],
             reviewed_node_refs: ["node://review"],
             confirmed_node_refs: ["node://review"]
           }) == ["node://publication"]
  end

  test "ready activity intents carry operation context, captured plans, policies, and predecessors" do
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

    facts = %{
      succeeded_node_refs: ["node://source"],
      terminal_event_refs_by_node_ref: %{"node://source" => "event://source/succeeded"}
    }

    assert {:ok, [intent]} =
             OperationGraphExecutor.ready_activity_intents(graph, facts, %{
               workflow_run_ref: "workflow-run://tenant/run-a",
               operation_context_ref: "operation-context://tenant/request-a",
               operation_plans_by_node_ref: %{
                 "node://publication" => "operation-plan://tenant/run-a/publication"
               },
               retry_policies_by_node_ref: %{"node://publication" => %{max_attempts: 2}},
               timeout_policies_by_node_ref: %{"node://publication" => %{timeout_ms: 30_000}},
               cancellation_policies_by_node_ref: %{
                 "node://publication" => %{cancel_ref: "cancel://tenant/run-a"}
               }
             })

    assert intent.activity_intent_ref ==
             "activity-intent://workflow-run://tenant/run-a/node://publication"

    assert intent.node_ref == "node://publication"
    assert intent.operation_context_ref == "operation-context://tenant/request-a"
    assert intent.operation_plan_ref == "operation-plan://tenant/run-a/publication"
    assert intent.predecessor_event_refs == ["event://source/succeeded"]
    assert intent.retry_policy == %{max_attempts: 2}
    assert intent.timeout_policy == %{timeout_ms: 30_000}
    assert intent.cancellation_policy == %{cancel_ref: "cancel://tenant/run-a"}
    assert intent.metadata.operation_class == :source_write
  end

  test "activity intents carry relation-specific review and confirmation event refs" do
    graph =
      graph!(
        nodes: [
          node!("node://review", "role://review", :runtime_operation, 1),
          node!("node://publication", "role://publication", :source_write, 2)
        ],
        dependencies: [
          dependency!("node://review", "node://publication", :blocks_on_review),
          dependency!("node://review", "node://publication", :blocks_on_confirmation)
        ]
      )

    facts = %{
      succeeded_node_refs: ["node://review"],
      reviewed_node_refs: ["node://review"],
      confirmed_node_refs: ["node://review"],
      review_event_refs_by_node_ref: %{"node://review" => "event://review/reviewed"},
      confirmation_event_refs_by_node_ref: %{
        "node://review" => "event://review/confirmed"
      }
    }

    assert {:ok, [intent]} =
             OperationGraphExecutor.ready_activity_intents(graph, facts, %{
               workflow_run_ref: "workflow-run://tenant/run-a",
               operation_context_ref: "operation-context://tenant/request-a",
               operation_plans_by_node_ref: %{
                 "node://publication" => "operation-plan://tenant/run-a/publication"
               }
             })

    assert intent.predecessor_event_refs == [
             "event://review/reviewed",
             "event://review/confirmed"
           ]
  end

  test "activity intents fail closed when predecessor event refs are absent" do
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

    assert {:error,
            {:missing_predecessor_event_ref,
             %{node_ref: "node://publication", predecessor_node_ref: "node://source"}}} =
             OperationGraphExecutor.ready_activity_intents(
               graph,
               %{succeeded_node_refs: ["node://source"]},
               %{
                 operation_context_ref: "operation-context://tenant/request-a",
                 operation_plans_by_node_ref: %{
                   "node://publication" => "operation-plan://tenant/run-a/publication"
                 }
               }
             )
  end

  test "recorded activity success facts advance dependent joins deterministically" do
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

    intent =
      activity_intent!(
        node_ref: "node://source",
        operation_plan_ref: "operation-plan://tenant/run-a/source"
      )

    assert {:ok, {fact, updated_facts}} =
             OperationGraphExecutor.record_activity_result(
               graph,
               %{active_node_refs: ["node://source"]},
               intent,
               %{
                 event_ref: "event://source/succeeded",
                 status: :succeeded,
                 result_ref: "result://source/1"
               }
             )

    assert fact.event_ref == "event://source/succeeded"
    assert fact.node_ref == "node://source"
    assert fact.status == :succeeded
    assert fact.terminal?
    assert fact.result_ref == "result://source/1"

    assert updated_facts.succeeded_node_refs == ["node://source"]
    assert updated_facts.active_node_refs == []

    assert updated_facts.terminal_event_refs_by_node_ref == %{
             "node://source" => "event://source/succeeded"
           }

    assert OperationGraphExecutor.ready_node_refs(graph, updated_facts) == [
             "node://publication"
           ]
  end

  test "retryable activity failure records retry facts without advancing joins" do
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

    intent =
      activity_intent!(
        node_ref: "node://source",
        operation_plan_ref: "operation-plan://tenant/run-a/source"
      )

    assert {:ok, {fact, updated_facts}} =
             OperationGraphExecutor.record_activity_result(
               graph,
               %{active_node_refs: ["node://source"]},
               intent,
               %{
                 event_ref: "event://source/retryable-failure",
                 status: :failed,
                 retryable?: true,
                 error_class: :rate_limited
               }
             )

    refute fact.terminal?
    assert fact.retryable?
    assert fact.error_class == :rate_limited
    assert updated_facts.retry_node_refs == ["node://source"]
    assert updated_facts.active_node_refs == []
    refute Map.has_key?(updated_facts, :terminal_event_refs_by_node_ref)
    assert OperationGraphExecutor.ready_node_refs(graph, updated_facts) == []
  end

  test "terminal optional failure facts allow optional dependent joins" do
    graph =
      graph!(
        nodes: [
          node!("node://evidence", "role://evidence", :evidence_collection, 1),
          node!("node://publication", "role://publication", :source_write, 2)
        ],
        dependencies: [
          %{
            dependency_ref: "dependency://evidence/publication/optional",
            from_node_ref: "node://evidence",
            to_node_ref: "node://publication",
            relation: :blocks_on_success,
            completion_policy: :optional
          }
        ]
      )

    intent =
      activity_intent!(
        node_ref: "node://evidence",
        operation_plan_ref: "operation-plan://tenant/run-a/evidence"
      )

    assert {:ok, {fact, updated_facts}} =
             OperationGraphExecutor.record_activity_result(graph, %{}, intent, %{
               event_ref: "event://evidence/terminal-failure",
               status: :failed,
               retryable?: false,
               error_class: :not_found
             })

    assert fact.terminal?
    assert fact.status == :failed
    assert updated_facts.failed_node_refs == ["node://evidence"]

    assert updated_facts.terminal_event_refs_by_node_ref == %{
             "node://evidence" => "event://evidence/terminal-failure"
           }

    assert OperationGraphExecutor.ready_node_refs(graph, updated_facts) == [
             "node://publication"
           ]
  end

  test "recorded activity results fail closed without event refs" do
    graph =
      graph!(
        nodes: [
          node!("node://source", "role://source", :source_read, 1)
        ],
        dependencies: []
      )

    intent =
      activity_intent!(
        node_ref: "node://source",
        operation_plan_ref: "operation-plan://tenant/run-a/source"
      )

    assert {:error, {:missing_required_activity_result_field, :event_ref}} =
             OperationGraphExecutor.record_activity_result(graph, %{}, intent, %{
               status: :succeeded
             })
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

  defp activity_intent!(opts) do
    %OperationGraphExecutor.ActivityIntent{
      activity_intent_ref: "activity-intent://tenant/run-a/#{Keyword.fetch!(opts, :node_ref)}",
      node_ref: Keyword.fetch!(opts, :node_ref),
      operation_context_ref: "operation-context://tenant/request-a",
      operation_plan_ref: Keyword.fetch!(opts, :operation_plan_ref),
      predecessor_event_refs: [],
      retry_policy: %{},
      timeout_policy: %{},
      cancellation_policy: %{},
      metadata: %{}
    }
  end
end
