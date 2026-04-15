defmodule Mezzanine.OpsDomain.DataCase do
  @moduledoc false
  use ExUnit.CaseTemplate

  alias Ecto.Adapters.SQL.Sandbox
  alias Mezzanine.OpsDomain.Repo

  using do
    quote do
      alias Mezzanine.OpsDomain.Repo

      import Ecto
      import Ecto.Changeset
      import Ecto.Query
      import Mezzanine.OpsDomain.DataCase
    end
  end

  setup tags do
    __MODULE__.setup_sandbox(tags)
    :ok
  end

  def setup_sandbox(tags) do
    pid = Sandbox.start_owner!(Repo, shared: not tags[:async])

    on_exit(fn -> Sandbox.stop_owner(pid) end)
  end
end
