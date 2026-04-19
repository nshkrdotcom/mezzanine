defmodule Mezzanine.WorkflowRuntime.WorkflowFanoutFaninTest do
  use ExUnit.Case, async: true

  alias Mezzanine.WorkflowRuntime.WorkflowFanoutFanin
  alias Mezzanine.Workflows.JoinBarrier

  test "declares child-workflow fan-out/fan-in contract and required scope fields" do
    assert %{
             contract_name: "Mezzanine.WorkflowFanoutFanin.v1",
             owner_repo: :mezzanine,
             topology: :child_workflows,
             parent_workflow: Mezzanine.Workflows.JoinBarrier,
             join_signal: %{name: "child.completed", version: "child-completed.v1"},
             query: %{name: "fanout.branch_state", version: "fanout-branch-state.v1"}
           } = WorkflowFanoutFanin.contract()

    assert WorkflowFanoutFanin.child_workflow_selection_rule() ==
             :independent_durable_branch_lifecycle

    required = WorkflowFanoutFanin.required_branch_fields()

    for field <- [
          :tenant_ref,
          :resource_ref,
          :trace_id,
          :parent_workflow_ref,
          :child_workflow_ref,
          :idempotency_scope,
          :authority_context,
          :release_manifest_ref
        ] do
      assert field in required
    end
  end

  test "fan-in closes exactly once under duplicate child completion" do
    state = WorkflowFanoutFanin.new_parent_state!(parent_input())

    assert {:ok, state, [%{event_type: :branch_completed}]} =
             WorkflowFanoutFanin.apply_completion(state, completion("branch-a", "complete-a-1"))

    assert {:ok, state, [%{event_type: :branch_completed}, %{event_type: :fan_in_closed}]} =
             WorkflowFanoutFanin.apply_completion(state, completion("branch-b", "complete-b-1"))

    assert state.status == :closed
    assert state.close_count == 1
    assert state.close_event_ref == "fanout-group:fanout-1:close:1"

    assert {:ok, duplicate_state, [%{event_type: :duplicate_completion_suppressed}]} =
             WorkflowFanoutFanin.apply_completion(state, completion("branch-b", "complete-b-1"))

    assert duplicate_state.status == :closed
    assert duplicate_state.close_count == 1
    assert duplicate_state.close_event_ref == state.close_event_ref
    assert duplicate_state.duplicate_completion_count == 1
  end

  test "operator query exposes branch state without raw child payloads" do
    state =
      parent_input()
      |> WorkflowFanoutFanin.new_parent_state!()
      |> elem_apply(completion("branch-a", "complete-a-1"))

    assert %{
             contract_name: "Mezzanine.WorkflowFanoutFanin.v1",
             status: :waiting,
             parent_workflow_ref: %{workflow_id: "parent-workflow-1"},
             branches: branches,
             failure_summary: %{failed_count: 0},
             raw_payload?: false
           } = WorkflowFanoutFanin.operator_query(state)

    assert %{status: :completed, completion_ref: "complete-a-1"} = branches["branch-a"]
    assert %{status: :pending, completion_ref: nil} = branches["branch-b"]
  end

  test "cancellation propagation targets unfinished child workflow refs with authority context" do
    state =
      parent_input()
      |> WorkflowFanoutFanin.new_parent_state!()
      |> elem_apply(completion("branch-a", "complete-a-1"))

    assert [
             %{
               signal_name: "operator.cancel.child",
               signal_version: "operator-cancel-child.v1",
               branch_ref: "branch-b",
               child_workflow_ref: %{workflow_id: "child-workflow-b"},
               tenant_ref: "tenant-1",
               resource_ref: "resource-1",
               trace_id: "trace-1",
               authority_context: %{authority_packet_ref: "authority-1"}
             }
           ] = WorkflowFanoutFanin.cancellation_propagation(state, %{reason: "operator_cancel"})
  end

  test "failure aggregation remains operator explainable" do
    state =
      parent_input()
      |> WorkflowFanoutFanin.new_parent_state!()
      |> elem_apply(completion("branch-a", "complete-a-1", :failed))
      |> elem_apply(completion("branch-b", "complete-b-1"))

    assert %{
             failed_count: 1,
             failed_branches: ["branch-a"],
             failure_classes: [:child_workflow_failed]
           } = WorkflowFanoutFanin.failure_summary(state)
  end

  test "join barrier workflow delegates to the fanout fanin contract" do
    assert {:ok,
            %{
              contract_name: "Mezzanine.WorkflowFanoutFanin.v1",
              status: :closed,
              close_count: 1,
              duplicate_completion_count: 1
            }} =
             JoinBarrier.run(
               Map.put(parent_input(), :completions, [
                 completion("branch-a", "complete-a-1"),
                 completion("branch-b", "complete-b-1"),
                 completion("branch-b", "complete-b-1")
               ])
             )
  end

  defp elem_apply(state, completion) do
    {:ok, next_state, _events} = WorkflowFanoutFanin.apply_completion(state, completion)
    next_state
  end

  defp parent_input do
    %{
      tenant_ref: "tenant-1",
      resource_ref: "resource-1",
      trace_id: "trace-1",
      fanout_group_ref: "fanout-1",
      release_manifest_ref: "phase4-v6-milestone30-workflow-fanout-fanin",
      parent_workflow_ref: %{
        workflow_id: "parent-workflow-1",
        workflow_run_id: "parent-run-1",
        workflow_version: "join-barrier.v1"
      },
      authority_context: %{
        authority_packet_ref: "authority-1",
        permission_decision_ref: "decision-1"
      },
      idempotency_scope: "tenant-1:fanout-1",
      branches: [
        branch("branch-a", "child-workflow-a"),
        branch("branch-b", "child-workflow-b")
      ]
    }
  end

  defp branch(branch_ref, workflow_id) do
    %{
      branch_ref: branch_ref,
      tenant_ref: "tenant-1",
      resource_ref: "resource-1",
      trace_id: "trace-1",
      parent_workflow_ref: %{workflow_id: "parent-workflow-1"},
      child_workflow_ref: %{workflow_id: workflow_id},
      idempotency_scope: "tenant-1:fanout-1:#{branch_ref}",
      authority_context: %{authority_packet_ref: "authority-1"},
      release_manifest_ref: "phase4-v6-milestone30-workflow-fanout-fanin"
    }
  end

  defp completion(branch_ref, completion_ref, status \\ :completed) do
    %{
      branch_ref: branch_ref,
      child_workflow_ref: %{workflow_id: "child-workflow-#{String.last(branch_ref)}"},
      completion_ref: completion_ref,
      completion_idempotency_key: completion_ref,
      status: status,
      result_ref: "result-#{completion_ref}",
      failure_class: if(status == :failed, do: :child_workflow_failed, else: nil)
    }
  end
end
