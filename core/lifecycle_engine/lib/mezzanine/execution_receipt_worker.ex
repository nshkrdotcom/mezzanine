defmodule Mezzanine.ExecutionReceiptWorker do
  @moduledoc """
  Retired M31 Oban receipt-worker tombstone.

  Terminal lower receipts now enter the execution workflow as durable receipt
  signals through `Mezzanine.WorkflowRuntime`. This module intentionally is not
  an Oban worker and must not be enqueued.
  """

  @spec retired?() :: true
  def retired?, do: true

  @spec replacement() :: map()
  def replacement do
    %{
      signal: "Mezzanine.WorkflowReceiptSignal.v1",
      workflow: "Mezzanine.Workflows.ExecutionAttempt",
      handler: "Mezzanine.WorkflowRuntime.ExecutionLifecycleWorkflow.apply_receipt_signal/2"
    }
  end
end
