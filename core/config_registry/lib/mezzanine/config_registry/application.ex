defmodule Mezzanine.ConfigRegistry.Application do
  @moduledoc false
  use Application

  @impl true
  def start(_type, _args) do
    children = [
      Mezzanine.ConfigRegistry.Repo,
      Mezzanine.Pack.Registry
    ]

    Supervisor.start_link(children,
      strategy: :one_for_one,
      name: Mezzanine.ConfigRegistry.Supervisor
    )
  end
end
