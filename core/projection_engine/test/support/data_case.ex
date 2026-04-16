defmodule Mezzanine.Projections.DataCase do
  @moduledoc false

  use ExUnit.CaseTemplate

  alias Ecto.Adapters.SQL.Sandbox
  alias Mezzanine.Audit.Repo, as: AuditRepo
  alias Mezzanine.Decisions.Repo, as: DecisionsRepo
  alias Mezzanine.EvidenceLedger.Repo, as: EvidenceRepo
  alias Mezzanine.Execution.Repo, as: ExecutionRepo
  alias Mezzanine.Objects.Repo, as: ObjectsRepo
  alias Mezzanine.Projections.Repo, as: ProjectionsRepo

  using do
    quote do
      alias Mezzanine.Projections.Repo

      import Ecto
      import Ecto.Changeset
      import Ecto.Query
      import Mezzanine.Projections.DataCase
    end
  end

  setup tags do
    setup_sandboxes(tags)
    :ok
  end

  def setup_sandboxes(tags) do
    shared? = not tags[:async]

    projection_owner = Sandbox.start_owner!(ProjectionsRepo, shared: shared?)
    evidence_owner = Sandbox.start_owner!(EvidenceRepo, shared: shared?)
    decision_owner = Sandbox.start_owner!(DecisionsRepo, shared: shared?)
    execution_owner = Sandbox.start_owner!(ExecutionRepo, shared: shared?)
    object_owner = Sandbox.start_owner!(ObjectsRepo, shared: shared?)
    audit_owner = Sandbox.start_owner!(AuditRepo, shared: shared?)

    on_exit(fn ->
      Sandbox.stop_owner(audit_owner)
      Sandbox.stop_owner(object_owner)
      Sandbox.stop_owner(execution_owner)
      Sandbox.stop_owner(decision_owner)
      Sandbox.stop_owner(evidence_owner)
      Sandbox.stop_owner(projection_owner)
    end)
  end
end
