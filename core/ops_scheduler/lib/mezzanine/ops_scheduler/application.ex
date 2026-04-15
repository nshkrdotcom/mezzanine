defmodule Mezzanine.OpsScheduler.Application do
  @moduledoc false
  use Application

  @impl true
  def start(_type, _args) do
    children = Mezzanine.Scheduler.children_from_env()

    Supervisor.start_link(children,
      strategy: :one_for_one,
      name: Mezzanine.OpsScheduler.Supervisor
    )
  end
end
