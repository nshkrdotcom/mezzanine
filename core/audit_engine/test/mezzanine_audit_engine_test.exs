defmodule MezzanineAuditEngineTest do
  use ExUnit.Case

  test "lists the frozen operational contract modules" do
    assert [
             Mezzanine.Audit.TraceContract,
             Mezzanine.Audit.ExecutionLineage,
             Mezzanine.Audit.Staleness,
             Mezzanine.Audit.UnifiedTrace
           ] == MezzanineAuditEngine.contract_modules()
  end

  test "exposes the neutral work-audit service from the audit engine" do
    assert Mezzanine.Audit.WorkAudit in MezzanineAuditEngine.durable_modules()
  end
end
