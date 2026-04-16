defmodule Mezzanine.Objects.PersistenceTest do
  use Mezzanine.Objects.DataCase, async: false

  alias Mezzanine.Audit.AuditFact
  alias Mezzanine.Objects.SubjectRecord

  test "ingest persists subject records with installation scoping and source refs" do
    assert {:ok, subject} =
             SubjectRecord.ingest(%{
               installation_id: "inst-1",
               source_ref: "linear:ticket:123",
               subject_kind: "linear_coding_ticket",
               lifecycle_state: "queued",
               title: "Triage user report",
               payload: %{priority: "high"},
               trace_id: "trace-ingest",
               causation_id: "cause-ingest",
               actor_ref: %{kind: :intake}
             })

    assert subject.installation_id == "inst-1"
    assert subject.source_ref == "linear:ticket:123"
    assert subject.row_version == 1

    assert {:ok, reloaded} =
             SubjectRecord.by_installation_source_ref("inst-1", "linear:ticket:123")

    assert reloaded.id == subject.id

    assert has_index?("subject_records", ["installation_id", "source_ref"])
    assert has_index?("subject_records", ["installation_id", "lifecycle_state"])
    assert has_index?("subject_records", ["installation_id", "subject_kind"])

    assert {:ok, [audit_fact]} = AuditFact.list_trace("inst-1", "trace-ingest")
    assert audit_fact.fact_kind == :subject_ingested
    assert audit_fact.subject_id == subject.id
  end

  test "advance_lifecycle owns canonical business progression and rejects stale writes" do
    assert {:ok, subject} =
             SubjectRecord.ingest(%{
               installation_id: "inst-1",
               source_ref: "linear:ticket:456",
               subject_kind: "linear_coding_ticket",
               lifecycle_state: "queued",
               payload: %{},
               trace_id: "trace-bootstrap",
               causation_id: "cause-bootstrap",
               actor_ref: %{kind: :intake}
             })

    assert {:ok, advanced_subject} =
             SubjectRecord.advance_lifecycle(subject, %{
               lifecycle_state: "executing",
               trace_id: "trace-lifecycle",
               causation_id: "cause-lifecycle",
               actor_ref: %{kind: :system}
             })

    assert advanced_subject.lifecycle_state == "executing"
    assert advanced_subject.row_version == 2

    assert {:error, error} =
             SubjectRecord.advance_lifecycle(subject, %{
               lifecycle_state: "failed",
               trace_id: "trace-stale",
               causation_id: "cause-stale",
               actor_ref: %{kind: :system}
             })

    assert Exception.message(error) =~ "stale"

    assert {:ok, [audit_fact]} = AuditFact.list_trace("inst-1", "trace-lifecycle")
    assert audit_fact.fact_kind == :lifecycle_advanced
    assert audit_fact.subject_id == subject.id
  end

  test "block and unblock preserve lifecycle state while toggling the rescue overlay" do
    assert {:ok, subject} =
             SubjectRecord.ingest(%{
               installation_id: "inst-1",
               source_ref: "linear:ticket:789",
               subject_kind: "linear_coding_ticket",
               lifecycle_state: "awaiting_decision",
               payload: %{},
               trace_id: "trace-block-bootstrap",
               causation_id: "cause-block-bootstrap",
               actor_ref: %{kind: :intake}
             })

    assert {:ok, blocked_subject} =
             SubjectRecord.block(subject, %{
               block_reason: "waiting_for_human_override",
               trace_id: "trace-block",
               causation_id: "cause-block",
               actor_ref: %{kind: :operator}
             })

    assert blocked_subject.lifecycle_state == "awaiting_decision"
    assert blocked_subject.block_reason == "waiting_for_human_override"
    assert %DateTime{} = blocked_subject.blocked_at

    assert {:ok, unblocked_subject} =
             SubjectRecord.unblock(blocked_subject, %{
               trace_id: "trace-unblock",
               causation_id: "cause-unblock",
               actor_ref: %{kind: :operator}
             })

    assert unblocked_subject.lifecycle_state == "awaiting_decision"
    assert is_nil(unblocked_subject.block_reason)
    assert is_nil(unblocked_subject.blocked_at)

    assert {:ok, [block_fact]} = AuditFact.list_trace("inst-1", "trace-block")
    assert block_fact.fact_kind == :subject_blocked

    assert {:ok, [unblock_fact]} = AuditFact.list_trace("inst-1", "trace-unblock")
    assert unblock_fact.fact_kind == :subject_unblocked
  end

  defp has_index?(table_name, columns) when is_binary(table_name) and is_list(columns) do
    columns_sql = Enum.join(columns, ", ")

    Repo.query!(
      """
      SELECT indexdef
      FROM pg_indexes
      WHERE schemaname = current_schema()
        AND tablename = $1
      """,
      [table_name]
    ).rows
    |> Enum.any?(fn [indexdef] ->
      String.contains?(indexdef, "(#{columns_sql})")
    end)
  end
end
