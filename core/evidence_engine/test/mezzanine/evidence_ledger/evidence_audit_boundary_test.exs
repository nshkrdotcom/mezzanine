defmodule Mezzanine.EvidenceLedger.EvidenceAuditBoundaryTest do
  use ExUnit.Case, async: true

  @source_path Path.expand("../../../lib/mezzanine/evidence_ledger/evidence_record.ex", __DIR__)

  test "evidence aggregate delegates audit fact writes to the audit append owner" do
    source = File.read!(@source_path)

    refute source =~ "alias Mezzanine.Audit.AuditFact"
    refute source =~ "AuditFact.record("
    refute source =~ "record_audit_fact("
    assert source =~ "AuditAppend.append_fact"
  end
end
