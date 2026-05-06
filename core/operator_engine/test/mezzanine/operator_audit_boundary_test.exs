defmodule Mezzanine.OperatorAuditBoundaryTest do
  use ExUnit.Case, async: true

  @source_path Path.expand("../../lib/mezzanine/operator_commands.ex", __DIR__)

  test "operator commands delegate audit writes through bounded-context owner actions" do
    source = File.read!(@source_path)

    refute String.contains?(source, "@insert_audit_fact_sql")
    refute String.contains?(source, "insert_audit_fact(")
    refute String.contains?(source, "AuditAppend.append_fact")
    assert String.contains?(source, "SubjectRecord.pause")
    assert String.contains?(source, "ExecutionRecord.record_operator_cancelled")
    assert String.contains?(source, "OperatorActionClassification")
  end
end
