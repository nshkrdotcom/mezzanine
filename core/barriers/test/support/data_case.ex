defmodule Mezzanine.Barriers.DataCase do
  @moduledoc false

  use ExUnit.CaseTemplate

  alias Ecto.Adapters.SQL.Sandbox
  alias Mezzanine.Execution.Repo

  using do
    quote do
      alias Mezzanine.Execution.Repo

      import Ecto
      import Ecto.Changeset
      import Ecto.Query
      import Mezzanine.Barriers.DataCase
    end
  end

  setup tags do
    shared? = not tags[:async]
    owner = Sandbox.start_owner!(Repo, shared: shared?)

    on_exit(fn -> Sandbox.stop_owner(owner) end)
    :ok
  end
end
