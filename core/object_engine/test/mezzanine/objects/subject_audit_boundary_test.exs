defmodule Mezzanine.Objects.SubjectAuditBoundaryTest do
  use ExUnit.Case, async: true

  @source_path Path.expand("../../../lib/mezzanine/objects/subject_record.ex", __DIR__)

  test "subject aggregate delegates audit fact writes to the audit append owner" do
    source = File.read!(@source_path)

    refute source =~ "alias Mezzanine.Audit.AuditFact"
    refute source =~ "AuditFact.record("
    assert source =~ "AuditAppend.append_fact"
  end
end
