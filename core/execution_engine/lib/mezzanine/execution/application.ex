defmodule Mezzanine.Execution.Application do
  @moduledoc false

  use Application

  @impl true
  def start(_type, _args) do
    Supervisor.start_link(children(),
      strategy: :one_for_one,
      name: Mezzanine.Execution.Supervisor
    )
  end

  defp children do
    if Application.get_env(:mezzanine_execution_engine, :start_runtime_children?, true) do
      [Mezzanine.Execution.Repo]
    else
      []
    end
  end
end
