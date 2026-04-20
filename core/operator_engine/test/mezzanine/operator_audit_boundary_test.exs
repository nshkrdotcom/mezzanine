defmodule Mezzanine.OperatorAuditBoundaryTest do
  use ExUnit.Case, async: true

  @source_path Path.expand("../../lib/mezzanine/operator_commands.ex", __DIR__)

  test "operator commands delegate audit fact writes to the audit append owner" do
    source = File.read!(@source_path)

    refute source =~ "@insert_audit_fact_sql"
    refute source =~ "insert_audit_fact("
    assert source =~ "AuditAppend.append_fact"
  end
end
