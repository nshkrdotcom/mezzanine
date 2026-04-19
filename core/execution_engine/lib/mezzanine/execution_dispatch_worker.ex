defmodule Mezzanine.ExecutionDispatchWorker do
  @moduledoc """
  Retired M31 Oban dispatch worker tombstone.

  Lower dispatch is now owned by Temporal workflow/activity execution through
  `Mezzanine.WorkflowRuntime`. This module intentionally is not an Oban worker.
  """

  @spec retired?() :: true
  def retired?, do: true

  @spec replacement() :: map()
  def replacement do
    %{
      workflow: "Mezzanine.Workflows.ExecutionAttempt",
      activity: "Mezzanine.Activities.SubmitJidoLowerActivity",
      envelope: "Mezzanine.WorkflowExecutionLifecycleInput.v1"
    }
  end
end
