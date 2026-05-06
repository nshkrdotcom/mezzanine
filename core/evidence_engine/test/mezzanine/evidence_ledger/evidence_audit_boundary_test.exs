defmodule Mezzanine.EvidenceLedger.EvidenceAuditBoundaryTest do
  use ExUnit.Case, async: true

  @source_path Path.expand("../../../lib/mezzanine/evidence_ledger/evidence_record.ex", __DIR__)

  test "evidence aggregate delegates audit fact writes to the audit append owner" do
    source = File.read!(@source_path)

    refute String.contains?(source, "alias Mezzanine.Audit.AuditFact")
    refute String.contains?(source, "AuditFact.record(")
    refute String.contains?(source, "record_audit_fact(")
    assert String.contains?(source, "AuditAppend.append_fact")
  end
end
