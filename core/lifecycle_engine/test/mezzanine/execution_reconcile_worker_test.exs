defmodule Mezzanine.ExecutionReconcileWorkerTest do
  use Mezzanine.LifecycleEngine.DataCase, async: false

  alias Mezzanine.ExecutionReconcileWorker

  test "reconcile worker is a retired M31 tombstone, not an Oban worker" do
    assert ExecutionReconcileWorker.retired?()
    refute function_exported?(ExecutionReconcileWorker, :perform, 1)

    assert %{
             activity: "Mezzanine.Activities.ReconcileLowerRun",
             workflow: "Mezzanine.Workflows.ExecutionAttempt",
             envelope: "Mezzanine.WorkflowExecutionLifecycleInput.v1"
           } = ExecutionReconcileWorker.replacement()
  end
end
