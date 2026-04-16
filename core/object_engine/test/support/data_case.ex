defmodule Mezzanine.Objects.DataCase do
  @moduledoc false

  use ExUnit.CaseTemplate

  alias Ecto.Adapters.SQL.Sandbox
  alias Mezzanine.Audit.Repo, as: AuditRepo
  alias Mezzanine.Objects.Repo, as: ObjectsRepo

  using do
    quote do
      alias Mezzanine.Objects.Repo

      import Ecto
      import Ecto.Changeset
      import Ecto.Query
      import Mezzanine.Objects.DataCase
    end
  end

  setup tags do
    setup_sandboxes(tags)
    :ok
  end

  def setup_sandboxes(tags) do
    object_owner = Sandbox.start_owner!(ObjectsRepo, shared: not tags[:async])

    audit_owner = Sandbox.start_owner!(AuditRepo, shared: not tags[:async])

    on_exit(fn ->
      Sandbox.stop_owner(audit_owner)
      Sandbox.stop_owner(object_owner)
    end)
  end
end
