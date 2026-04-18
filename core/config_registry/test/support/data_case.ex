defmodule Mezzanine.ConfigRegistry.DataCase do
  @moduledoc false
  use ExUnit.CaseTemplate

  alias Ecto.Adapters.SQL.Sandbox
  alias Mezzanine.ConfigRegistry.Repo
  alias Mezzanine.Execution.Repo, as: ExecutionRepo

  using do
    quote do
      alias Mezzanine.ConfigRegistry.Repo

      import Ecto
      import Ecto.Changeset
      import Ecto.Query
      import Mezzanine.ConfigRegistry.DataCase
    end
  end

  setup tags do
    shared? = not tags[:async]
    execution_owner = Sandbox.start_owner!(ExecutionRepo, shared: shared?)
    pid = Sandbox.start_owner!(Repo, shared: shared?)

    on_exit(fn ->
      Sandbox.stop_owner(pid)
      Sandbox.stop_owner(execution_owner)
    end)

    :ok
  end
end
