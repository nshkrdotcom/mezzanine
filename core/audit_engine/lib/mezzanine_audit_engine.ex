defmodule MezzanineAuditEngine do
  @moduledoc """
  Neutral audit-ledger contract entrypoint for the Mezzanine rebuild.
  """

  @spec contract_modules() :: [module()]
  def contract_modules do
    [
      Mezzanine.Audit.TraceContract,
      Mezzanine.Audit.ExecutionLineage,
      Mezzanine.Audit.Staleness,
      Mezzanine.Audit.TenantScopedTraceJoin,
      Mezzanine.Audit.UnifiedTrace
    ]
  end

  @spec durable_modules() :: [module()]
  def durable_modules do
    [
      Mezzanine.Audit,
      Mezzanine.Audit.AuditAppend,
      Mezzanine.Audit.AuditFact,
      Mezzanine.Audit.AuditInclusionProof,
      Mezzanine.Audit.AuditQuery,
      Mezzanine.Audit.ExecutionLineageRecord,
      Mezzanine.Audit.ExecutionLineageStore,
      Mezzanine.Audit.WorkAudit
    ]
  end

  @spec components() :: [module()]
  def components do
    contract_modules() ++ durable_modules()
  end
end
