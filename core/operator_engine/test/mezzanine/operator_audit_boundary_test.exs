defmodule Mezzanine.OperatorAuditBoundaryTest do
  use ExUnit.Case, async: true

  @source_path Path.expand("../../lib/mezzanine/operator_commands.ex", __DIR__)

  test "operator commands delegate audit writes through bounded-context owner actions" do
    source = File.read!(@source_path)

    refute source =~ "@insert_audit_fact_sql"
    refute source =~ "insert_audit_fact("
    refute source =~ "AuditAppend.append_fact"
    assert source =~ "SubjectRecord.pause"
    assert source =~ "ExecutionRecord.record_operator_cancelled"
  end
end
