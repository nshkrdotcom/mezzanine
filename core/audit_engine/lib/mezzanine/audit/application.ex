defmodule Mezzanine.Audit.Application do
  @moduledoc false

  use Application

  @impl true
  def start(_type, _args) do
    Supervisor.start_link(children(),
      strategy: :one_for_one,
      name: Mezzanine.Audit.Supervisor
    )
  end

  defp children do
    if Application.get_env(:mezzanine_audit_engine, :start_runtime_children?, true) do
      [Mezzanine.Audit.Repo]
    else
      []
    end
  end
end
