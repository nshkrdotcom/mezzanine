defmodule Mezzanine.ExecutionReconcileWorker do
  @moduledoc """
  Retired M31 Oban reconcile-worker tombstone.

  Lower outcome reconciliation is now a Temporal activity concern. This module
  intentionally is not an Oban worker and must not be enqueued.
  """

  @spec retired?() :: true
  def retired?, do: true

  @spec replacement() :: map()
  def replacement do
    %{
      activity: "Mezzanine.Activities.ReconcileLowerRun",
      workflow: "Mezzanine.Workflows.ExecutionAttempt",
      envelope: "Mezzanine.WorkflowExecutionLifecycleInput.v1"
    }
  end
end
