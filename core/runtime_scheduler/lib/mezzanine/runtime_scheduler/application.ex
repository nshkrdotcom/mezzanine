defmodule Mezzanine.RuntimeScheduler.Application do
  @moduledoc false

  use Application

  @impl true
  def start(_type, _args) do
    children = [
      Mezzanine.RuntimeScheduler.Repo
    ]

    Supervisor.start_link(
      children,
      strategy: :one_for_one,
      name: Mezzanine.RuntimeScheduler.Supervisor
    )
  end
end
