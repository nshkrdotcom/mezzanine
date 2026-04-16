defmodule Mezzanine.EvidenceLedger.Application do
  @moduledoc false

  use Application

  @impl true
  def start(_type, _args) do
    Supervisor.start_link(children(),
      strategy: :one_for_one,
      name: Mezzanine.EvidenceLedger.Supervisor
    )
  end

  defp children do
    if Application.get_env(:mezzanine_evidence_engine, :start_runtime_children?, true) do
      [Mezzanine.EvidenceLedger.Repo]
    else
      []
    end
  end
end
