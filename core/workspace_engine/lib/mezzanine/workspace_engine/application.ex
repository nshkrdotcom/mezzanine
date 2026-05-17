defmodule Mezzanine.WorkspaceEngine.Application do
  @moduledoc false

  use Application

  @impl Application
  def start(_type, _args) do
    children = [
      {Task.Supervisor, name: Mezzanine.WorkspaceEngine.HookTaskSupervisor}
    ]

    Supervisor.start_link(children,
      name: Mezzanine.WorkspaceEngine.Supervisor,
      strategy: :one_for_one
    )
  end
end
