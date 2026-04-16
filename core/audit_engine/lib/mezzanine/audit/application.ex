defmodule Mezzanine.Audit.Application do
  @moduledoc false

  use Application

  @impl true
  def start(_type, _args) do
    children = [
      Mezzanine.Audit.Repo
    ]

    Supervisor.start_link(children,
      strategy: :one_for_one,
      name: Mezzanine.Audit.Supervisor
    )
  end
end
