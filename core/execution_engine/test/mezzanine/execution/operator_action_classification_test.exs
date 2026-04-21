defmodule Mezzanine.Execution.OperatorActionClassificationTest do
  use ExUnit.Case, async: true

  alias Mezzanine.Execution.OperatorActionClassification

  test "profile declares only workflow signals and declared local mutations as operator effects" do
    profile = OperatorActionClassification.profile()

    assert profile.allowed_action_kinds == [:workflow_signal, :declared_local_mutation]
    assert :raw_sql_write in profile.forbidden_action_kinds
    assert :oban_saga_job in profile.forbidden_action_kinds
    assert :lower_cancel_worker in profile.forbidden_action_kinds
    assert profile.signal_boundary == "Mezzanine.WorkflowRuntime.signal_workflow/1"
    assert profile.signal_registry.cancel.signal_version == "operator-cancel.v1"
    assert profile.local_mutation_owners.execution_cancel.owner == :execution_ledger
  end

  test "workflow signal classification builds a Temporal-owned operator effect" do
    assert {:ok, signal} =
             OperatorActionClassification.workflow_signal(:cancel, execution(), context())

    assert signal.kind == :workflow_signal
    assert signal.signal_name == "operator.cancel"
    assert signal.signal_version == "operator-cancel.v1"
    assert signal.signal_contract == "Mezzanine.OperatorWorkflowSignal.v1"
    assert signal.boundary == "Mezzanine.WorkflowRuntime.signal_workflow/1"
    assert signal.workflow_id == "tenant:tenant-1:execution:exec-1:attempt:2"
    assert signal.workflow_run_id == "run-1"
    assert signal.target_ref == "workflow-signal://operator.cancel/exec-1"
  end

  test "declared local mutation classification names the bounded-context owner" do
    assert {:ok, mutation} =
             OperatorActionClassification.declared_local_mutation(
               :execution_cancel,
               :record_operator_cancelled,
               "execution://exec-1",
               context()
             )

    assert mutation.kind == :declared_local_mutation
    assert mutation.owner == :execution_ledger
    assert mutation.owner_module == "Mezzanine.Execution.ExecutionRecord"
    assert mutation.owner_action == :record_operator_cancelled
    assert mutation.target_ref == "execution://exec-1"
  end

  test "legacy refs and undeclared effects fail closed" do
    assert {:error,
            {:unclassified_operator_action_ref, "workflow-signal://operator.cancel/exec-1"}} =
             OperatorActionClassification.validate("workflow-signal://operator.cancel/exec-1")

    assert {:error, {:forbidden_operator_action_kind, :raw_sql_write}} =
             OperatorActionClassification.validate(%{kind: :raw_sql_write})

    assert {:error, {:undeclared_local_mutation_owner, :unknown_owner}} =
             OperatorActionClassification.declared_local_mutation(
               :unknown_owner,
               :mutate,
               "unknown://target",
               context()
             )

    assert {:error, {:unsupported_operator_signal_action, :archive}} =
             OperatorActionClassification.workflow_signal(:archive, execution(), context())
  end

  defp execution do
    %{
      id: "exec-1",
      tenant_id: "tenant-1",
      subject_id: "subject-1",
      dispatch_state: :accepted_active,
      dispatch_attempt_count: 1,
      submission_ref: %{"id" => "sub-1"},
      lower_receipt: %{"run_id" => "run-1"}
    }
  end

  defp context do
    %{
      action: :cancel,
      trace_id: "trace-operator",
      causation_id: "cause-operator",
      actor_ref: %{kind: :operator},
      reason: "operator requested cancel"
    }
  end
end
