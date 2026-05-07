defmodule Mezzanine.Projections.Application do
  @moduledoc false

  use Application

  @impl true
  def start(_type, _args) do
    children =
      if Application.get_env(:mezzanine_projection_engine, :start_runtime_children?, false) do
        [Mezzanine.Projections.Repo]
      else
        []
      end

    Supervisor.start_link(children,
      strategy: :one_for_one,
      name: Mezzanine.Projections.Supervisor
    )
  end
end
