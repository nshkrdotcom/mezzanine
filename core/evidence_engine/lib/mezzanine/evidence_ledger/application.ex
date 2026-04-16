defmodule Mezzanine.EvidenceLedger.Application do
  @moduledoc false

  use Application

  @impl true
  def start(_type, _args) do
    children = [
      Mezzanine.EvidenceLedger.Repo
    ]

    Supervisor.start_link(children,
      strategy: :one_for_one,
      name: Mezzanine.EvidenceLedger.Supervisor
    )
  end
end
