defmodule Mezzanine.Execution.Application do
  @moduledoc false

  use Application

  @impl true
  def start(_type, _args) do
    children = [
      Mezzanine.Execution.Repo
    ]

    Supervisor.start_link(children, strategy: :one_for_one, name: Mezzanine.Execution.Supervisor)
  end
end
