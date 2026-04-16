defmodule Mezzanine.Audit.DataCase do
  @moduledoc false
  use ExUnit.CaseTemplate

  alias Ecto.Adapters.SQL.Sandbox
  alias Mezzanine.Audit.Repo

  using do
    quote do
      alias Mezzanine.Audit.Repo

      import Ecto
      import Ecto.Changeset
      import Ecto.Query
      import Mezzanine.Audit.DataCase
    end
  end

  setup tags do
    pid = Sandbox.start_owner!(Repo, shared: not tags[:async])
    on_exit(fn -> Sandbox.stop_owner(pid) end)
    :ok
  end
end
