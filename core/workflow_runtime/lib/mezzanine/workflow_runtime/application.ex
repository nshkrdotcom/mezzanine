defmodule Mezzanine.WorkflowRuntime.Application do
  @moduledoc false

  use Application

  alias Mezzanine.WorkflowRuntime.TemporalSupervisor

  @impl true
  def start(_type, _args) do
    Supervisor.start_link(
      TemporalSupervisor.child_specs(),
      strategy: :one_for_one,
      name: Mezzanine.WorkflowRuntime.Supervisor
    )
  end
end
