defmodule Mezzanine.RuntimeProfile.Application do
  @moduledoc false

  use Application

  @impl true
  def start(_type, _args) do
    Supervisor.start_link(
      [Mezzanine.RuntimeProfileStore],
      strategy: :one_for_one,
      name: Mezzanine.RuntimeProfile.Supervisor
    )
  end
end
