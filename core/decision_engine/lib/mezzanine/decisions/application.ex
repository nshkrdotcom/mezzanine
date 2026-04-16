defmodule Mezzanine.Decisions.Application do
  @moduledoc false

  use Application

  @impl true
  def start(_type, _args) do
    children = [
      Mezzanine.Decisions.Repo
    ]

    Supervisor.start_link(children, strategy: :one_for_one, name: Mezzanine.Decisions.Supervisor)
  end
end
