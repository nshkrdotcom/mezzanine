defmodule Mezzanine.OpsDomain.Application do
  @moduledoc false
  use Application

  @impl true
  def start(_type, _args) do
    Supervisor.start_link(children(),
      strategy: :one_for_one,
      name: Mezzanine.OpsDomain.Supervisor
    )
  end

  defp children do
    if Application.get_env(:mezzanine_ops_domain, :start_runtime_children?, true) do
      [Mezzanine.OpsDomain.Repo]
    else
      []
    end
  end
end
