defmodule Mezzanine.Archival.Application do
  @moduledoc false

  use Application

  @impl true
  def start(_type, _args) do
    Supervisor.start_link(children(),
      strategy: :one_for_one,
      name: Mezzanine.Archival.Supervisor
    )
  end

  defp children do
    if Application.get_env(:mezzanine_archival_engine, :start_runtime_children?, true) do
      [
        Mezzanine.Archival.Repo,
        {Mezzanine.RepoTelemetryBridge,
         repo: Mezzanine.Archival.Repo,
         repo_name: "archival",
         query_event: [:mezzanine_archival_engine, :repo, :query]},
        Mezzanine.Archival.Scheduler
      ]
    else
      []
    end
  end
end
