defmodule Mezzanine.LifecycleContinuationWorker do
  @moduledoc """
  Retired M31 Oban lifecycle-continuation tombstone.

  Post-commit lifecycle continuation is now a Temporal workflow transition,
  not a retrying Oban worker chain.
  """

  @spec retired?() :: true
  def retired?, do: true

  @spec replacement() :: map()
  def replacement do
    %{
      workflow: "Mezzanine.Workflows.ExecutionAttempt",
      contract: "Mezzanine.WorkflowExecutionLifecycleInput.v1"
    }
  end
end
