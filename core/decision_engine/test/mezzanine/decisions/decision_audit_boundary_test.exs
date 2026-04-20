defmodule Mezzanine.Decisions.DecisionAuditBoundaryTest do
  use ExUnit.Case, async: true

  @source_path Path.expand("../../../lib/mezzanine/decisions/decision_record.ex", __DIR__)

  test "decision aggregate delegates audit fact writes to the audit append owner" do
    source = File.read!(@source_path)

    refute source =~ "alias Mezzanine.Audit.AuditFact"
    refute source =~ "record_audit_fact("
    assert source =~ "AuditAppend.append_fact"
  end
end
