defmodule Mezzanine.Audit do
  @moduledoc """
  Durable audit-ledger domain for the neutral Mezzanine substrate.
  """

  use Ash.Domain

  resources do
    resource(Mezzanine.Audit.AuditFact)
    resource(Mezzanine.Audit.ExecutionLineageRecord)
  end
end
