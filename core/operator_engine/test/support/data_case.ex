defmodule Mezzanine.Operator.DataCase do
  @moduledoc false

  use ExUnit.CaseTemplate

  alias Ecto.Adapters.SQL.Sandbox
  alias Mezzanine.Audit.Repo, as: AuditRepo
  alias Mezzanine.Execution.Repo, as: ExecutionRepo
  alias Mezzanine.Objects.Repo, as: ObjectsRepo

  using do
    quote do
      alias Mezzanine.Execution.Repo

      import Ecto
      import Ecto.Changeset
      import Ecto.Query
      import Mezzanine.Operator.DataCase
    end
  end

  setup tags do
    shared? = not tags[:async]

    execution_owner = Sandbox.start_owner!(ExecutionRepo, shared: shared?)
    objects_owner = Sandbox.start_owner!(ObjectsRepo, shared: shared?)
    audit_owner = Sandbox.start_owner!(AuditRepo, shared: shared?)

    on_exit(fn ->
      Sandbox.stop_owner(audit_owner)
      Sandbox.stop_owner(objects_owner)
      Sandbox.stop_owner(execution_owner)
    end)

    :ok
  end
end
