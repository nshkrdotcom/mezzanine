defmodule Mezzanine.Execution.ExecutionAuditBoundaryTest do
  use ExUnit.Case, async: true

  @source_path Path.expand("../../../lib/mezzanine/execution/execution_record.ex", __DIR__)

  test "execution aggregate delegates audit fact writes to the audit append owner" do
    source = File.read!(@source_path)

    refute String.contains?(source, "alias Mezzanine.Audit.{AuditFact")
    refute String.contains?(source, "record_audit_fact(")
    refute String.contains?(source, "record_dispatch_audit_fact(")
    assert String.contains?(source, "AuditAppend.append_fact")
  end
end
