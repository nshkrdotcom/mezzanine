defmodule MezzanineAuditEngine do
  @moduledoc """
  Neutral audit-ledger contract entrypoint for the Mezzanine rebuild.
  """

  @spec contract_modules() :: [module()]
  def contract_modules do
    [
      Mezzanine.Audit.TraceContract,
      Mezzanine.Audit.ExecutionLineage,
      Mezzanine.Audit.Freshness,
      Mezzanine.Audit.UnifiedTrace
    ]
  end
end
