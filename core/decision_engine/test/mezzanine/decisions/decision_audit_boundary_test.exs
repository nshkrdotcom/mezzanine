defmodule Mezzanine.Decisions.DecisionAuditBoundaryTest do
  use ExUnit.Case, async: true

  @source_path Path.expand("../../../lib/mezzanine/decisions/decision_record.ex", __DIR__)

  test "decision aggregate delegates audit fact writes to the audit append owner" do
    source = File.read!(@source_path)

    refute String.contains?(source, "alias Mezzanine.Audit.AuditFact")
    refute String.contains?(source, "record_audit_fact(")
    assert String.contains?(source, "AuditAppend.append_fact")
  end
end
