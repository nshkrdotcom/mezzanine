defmodule Mezzanine.Projections.Application do
  @moduledoc false

  use Application

  @impl true
  def start(_type, _args) do
    children = [
      Mezzanine.Projections.Repo
    ]

    Supervisor.start_link(children,
      strategy: :one_for_one,
      name: Mezzanine.Projections.Supervisor
    )
  end
end
