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
             query: %{name: "fanout.branch_state", version: "fanout-branch-state.v1"},
             supported_join_policies: supported_policies,
             policy_fields: policy_fields
           } = WorkflowFanoutFanin.contract()

    assert WorkflowFanoutFanin.child_workflow_selection_rule() ==
             :independent_durable_branch_lifecycle

    assert supported_policies == [
             :all_required,
             :k_of_n,
             :at_least_one,
             :best_effort_with_required,
             :fail_fast
           ]

    for field <- [
          :fanout_group_ref,
          :parent_workflow_ref,
          :workflow_version,
          :join_policy,
          :required_success_count,
          :required_branch_refs,
          :optional_branch_refs,
          :timeout_policy,
          :late_completion_policy,
          :heterogeneous_failure_actions,
          :close_decision,
          :quorum_result,
          :branch_counts,
          :failure_classes,
          :compensation_refs,
          :close_event_ref,
          :release_manifest_ref
        ] do
      assert field in policy_fields
    end

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
    assert state.close_decision == :succeeded
    assert state.close_event_ref == "fanout-group:fanout-1:close:1"

    assert {:ok, duplicate_state, [%{event_type: :duplicate_completion_suppressed}]} =
             WorkflowFanoutFanin.apply_completion(state, completion("branch-b", "complete-b-1"))

    assert duplicate_state.status == :closed
    assert duplicate_state.close_count == 1
    assert duplicate_state.close_decision == :succeeded
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
             close_decision: nil,
             join_policy: :all_required,
             branch_counts: %{completed: 1, failed: 0, pending: 1, terminal: 1, total: 2},
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
             failure_classes: [:child_workflow_failed],
             failure_class_counts: %{child_workflow_failed: 1},
             failures: [
               %{
                 branch_ref: "branch-a",
                 count: 1,
                 failure_class: :child_workflow_failed,
                 safe_action: :operator_review_required,
                 compensation_ref: nil
               }
             ]
           } = WorkflowFanoutFanin.failure_summary(state)
  end

  test "k-of-n closes once and records late completion evidence without changing decision" do
    state =
      parent_input(%{
        join_policy: :k_of_n,
        required_success_count: 1
      })
      |> WorkflowFanoutFanin.new_parent_state!()

    assert {:ok, state,
            [
              %{event_type: :branch_completed},
              %{
                event_type: :fan_in_closed,
                close_decision: :succeeded,
                quorum_result: %{
                  mode: :k_of_n,
                  met?: true,
                  required_success_count: 1,
                  completed_success_count: 1,
                  total_branch_count: 2
                },
                branch_counts: %{completed: 1, failed: 0, pending: 1, terminal: 1, total: 2}
              }
            ]} =
             WorkflowFanoutFanin.apply_completion(state, completion("branch-a", "complete-a-1"))

    assert state.status == :closed
    assert state.close_count == 1
    assert state.close_decision == :succeeded

    late_completion =
      completion("branch-b", "complete-b-late", :failed)
      |> Map.merge(%{
        failure_class: :child_workflow_timeout,
        safe_action: :retry_optional_branch,
        compensation_ref: "compensation://fanout-1/branch-b"
      })

    assert {:ok, late_state,
            [
              %{
                event_type: :late_completion_evidence,
                branch_ref: "branch-b",
                attempted_status: :failed,
                failure_class: :child_workflow_timeout,
                safe_action: :retry_optional_branch,
                compensation_ref: "compensation://fanout-1/branch-b",
                close_decision: :succeeded,
                close_count: 1
              }
            ]} = WorkflowFanoutFanin.apply_completion(state, late_completion)

    assert late_state.close_count == 1
    assert late_state.close_decision == :succeeded
    assert late_state.close_event_ref == state.close_event_ref
    assert late_state.late_completion_count == 1
    assert late_state.branches["branch-b"].status == :pending
  end

  test "best-effort with required branches can close partial success with heterogeneous failure summary" do
    state =
      parent_input(%{
        join_policy: :best_effort_with_required,
        required_branch_refs: ["branch-a"],
        branches: [
          branch("branch-a", "child-workflow-a"),
          branch("branch-b", "child-workflow-b"),
          branch("branch-c", "child-workflow-c")
        ]
      })
      |> WorkflowFanoutFanin.new_parent_state!()
      |> elem_apply(
        completion("branch-b", "complete-b-1", :failed)
        |> Map.merge(%{
          failure_class: :child_workflow_timeout,
          safe_action: :retry_optional_branch,
          compensation_ref: "compensation://fanout-1/branch-b"
        })
      )

    assert {:ok, closed_state,
            [
              %{event_type: :branch_completed},
              %{
                event_type: :fan_in_closed,
                close_decision: :partial_success,
                compensation_refs: ["compensation://fanout-1/branch-b"]
              }
            ]} =
             WorkflowFanoutFanin.apply_completion(state, completion("branch-a", "complete-a-1"))

    assert closed_state.status == :closed
    assert closed_state.close_count == 1
    assert closed_state.close_decision == :partial_success
    assert closed_state.required_branch_refs == ["branch-a"]
    assert closed_state.optional_branch_refs == ["branch-b", "branch-c"]

    assert %{
             failed_count: 1,
             failed_branches: ["branch-b"],
             failure_class_counts: %{child_workflow_timeout: 1},
             failures: [
               %{
                 branch_ref: "branch-b",
                 failure_class: :child_workflow_timeout,
                 count: 1,
                 safe_action: :retry_optional_branch,
                 compensation_ref: "compensation://fanout-1/branch-b"
               }
             ],
             compensation_refs: ["compensation://fanout-1/branch-b"]
           } = WorkflowFanoutFanin.failure_summary(closed_state)
  end

  test "join barrier workflow delegates to the fanout fanin contract" do
    assert {:ok,
            %{
              contract_name: "Mezzanine.WorkflowFanoutFanin.v1",
              status: :closed,
              close_count: 1,
              duplicate_completion_count: 1,
              close_decision: :succeeded
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

  defp parent_input(overrides \\ %{}) do
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
    |> Map.merge(overrides)
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
