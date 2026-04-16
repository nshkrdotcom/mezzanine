defmodule MezzanineAuditEngineTest do
  use ExUnit.Case

  test "lists the frozen operational contract modules" do
    assert [
             Mezzanine.Audit.TraceContract,
             Mezzanine.Audit.ExecutionLineage,
             Mezzanine.Audit.Freshness,
             Mezzanine.Audit.UnifiedTrace
           ] == MezzanineAuditEngine.contract_modules()
  end
end
