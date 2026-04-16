defmodule Mezzanine.ConfigRegistry.Application do
  @moduledoc false
  use Application

  @impl true
  def start(_type, _args) do
    Supervisor.start_link(children(),
      strategy: :one_for_one,
      name: Mezzanine.ConfigRegistry.Supervisor
    )
  end

  defp children do
    if Application.get_env(:mezzanine_config_registry, :start_runtime_children?, true) do
      [
        Mezzanine.ConfigRegistry.Repo,
        Mezzanine.Pack.Registry
      ]
    else
      []
    end
  end
end
