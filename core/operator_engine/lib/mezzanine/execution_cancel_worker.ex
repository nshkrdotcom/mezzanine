defmodule Mezzanine.ExecutionCancelWorker do
  @moduledoc """
  Retired M31 Oban cancellation-worker tombstone.

  Operator cancellation is now an authorized workflow signal delivered through
  `Mezzanine.WorkflowRuntime`, not a lower-cancel Oban job.
  """

  @spec retired?() :: true
  def retired?, do: true

  @spec replacement() :: map()
  def replacement do
    %{
      signal: "operator.cancel@operator-cancel.v1",
      contract: "Mezzanine.OperatorWorkflowSignal.v1",
      boundary: "Mezzanine.WorkflowRuntime.signal_workflow/1"
    }
  end
end
