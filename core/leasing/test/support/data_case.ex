defmodule Mezzanine.Leasing.DataCase do
  @moduledoc false

  use ExUnit.CaseTemplate

  alias Ecto.Adapters.SQL.Sandbox
  alias Mezzanine.Leasing.Repo

  using do
    quote do
      alias Mezzanine.Leasing.Repo

      import Ecto
      import Ecto.Changeset
      import Ecto.Query
      import Mezzanine.Leasing.DataCase
    end
  end

  setup tags do
    shared? = not tags[:async]
    owner = Sandbox.start_owner!(Repo, shared: shared?)

    on_exit(fn ->
      Sandbox.stop_owner(owner)
    end)

    :ok
  end
end
