defmodule Mezzanine.ExecutionReceiptWorkerTest do
  use Mezzanine.LifecycleEngine.DataCase, async: false

  alias Mezzanine.ExecutionReceiptWorker
  alias Mezzanine.JoinAdvanceWorker
  alias Mezzanine.LifecycleContinuationWorker

  test "receipt worker is a retired M31 tombstone, not an Oban worker" do
    assert ExecutionReceiptWorker.retired?()
    refute function_exported?(ExecutionReceiptWorker, :perform, 1)

    assert %{
             signal: "Mezzanine.WorkflowReceiptSignal.v1",
             workflow: "Mezzanine.Workflows.ExecutionAttempt",
             handler:
               "Mezzanine.WorkflowRuntime.ExecutionLifecycleWorkflow.apply_receipt_signal/2"
           } = ExecutionReceiptWorker.replacement()
  end

  test "join advance worker is a retired M31 tombstone, not an Oban worker" do
    assert JoinAdvanceWorker.retired?()
    refute function_exported?(JoinAdvanceWorker, :perform, 1)

    assert %{
             workflow: "Mezzanine.Workflows.JoinBarrier",
             contract: "Mezzanine.WorkflowFanoutFanin.v1",
             signal: "child.completed@child-completed.v1"
           } = JoinAdvanceWorker.replacement()
  end

  test "lifecycle continuation worker is a retired M31 tombstone, not an Oban worker" do
    assert LifecycleContinuationWorker.retired?()
    refute function_exported?(LifecycleContinuationWorker, :perform, 1)

    assert %{
             workflow: "Mezzanine.Workflows.ExecutionAttempt",
             contract: "Mezzanine.WorkflowExecutionLifecycleInput.v1"
           } = LifecycleContinuationWorker.replacement()
  end
end
