defmodule Mezzanine.JoinAdvanceWorker do
  @moduledoc """
  Retired M31 Oban join-advance tombstone.

  Fan-out/fan-in barrier closure is now owned by
  `Mezzanine.Workflows.JoinBarrier` and the `Mezzanine.WorkflowFanoutFanin.v1`
  contract. This module intentionally is not an Oban worker.
  """

  @spec retired?() :: true
  def retired?, do: true

  @spec replacement() :: map()
  def replacement do
    %{
      workflow: "Mezzanine.Workflows.JoinBarrier",
      contract: "Mezzanine.WorkflowFanoutFanin.v1",
      signal: "child.completed@child-completed.v1"
    }
  end
end
