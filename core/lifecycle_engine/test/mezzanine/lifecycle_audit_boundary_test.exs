defmodule Mezzanine.LifecycleAuditBoundaryTest do
  use ExUnit.Case, async: true

  @source_path Path.expand("../../lib/mezzanine/lifecycle_evaluator.ex", __DIR__)

  test "lifecycle evaluator delegates audit fact writes to the audit append owner" do
    source = File.read!(@source_path)

    refute String.contains?(source, "@insert_audit_fact_sql")
    refute String.contains?(source, "insert_audit_fact(")
    assert String.contains?(source, "AuditAppend.append_fact")
  end
end
