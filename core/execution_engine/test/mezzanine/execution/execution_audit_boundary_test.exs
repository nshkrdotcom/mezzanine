defmodule Mezzanine.Execution.ExecutionAuditBoundaryTest do
  use ExUnit.Case, async: true

  @source_path Path.expand("../../../lib/mezzanine/execution/execution_record.ex", __DIR__)

  test "execution aggregate delegates audit fact writes to the audit append owner" do
    source = File.read!(@source_path)

    refute source =~ "alias Mezzanine.Audit.{AuditFact"
    refute source =~ "record_audit_fact("
    refute source =~ "record_dispatch_audit_fact("
    assert source =~ "AuditAppend.append_fact"
  end
end
