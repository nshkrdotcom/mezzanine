defmodule Mezzanine.OpsDomain.Application do
  @moduledoc false
  use Application

  @impl true
  def start(_type, _args) do
    children = [
      Mezzanine.OpsDomain.Repo
    ]

    Supervisor.start_link(children, strategy: :one_for_one, name: Mezzanine.OpsDomain.Supervisor)
  end
end
