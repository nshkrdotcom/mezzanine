defmodule Mezzanine.Archival.Application do
  @moduledoc false

  use Application

  @impl true
  def start(_type, _args) do
    children = [
      Mezzanine.Archival.Repo
    ]

    Supervisor.start_link(children, strategy: :one_for_one, name: Mezzanine.Archival.Supervisor)
  end
end
