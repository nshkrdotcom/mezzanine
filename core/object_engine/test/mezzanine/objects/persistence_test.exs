defmodule Mezzanine.Objects.PersistenceTest do
  use Mezzanine.Objects.DataCase, async: false

  alias Mezzanine.Audit.AuditFact
  alias Mezzanine.Objects.{SubjectPayloadSchema, SubjectRecord}

  test "ingest persists subject records with installation scoping and source refs" do
    assert {:ok, subject} =
             SubjectRecord.ingest(%{
               installation_id: "inst-1",
               source_ref: "linear:ticket:123",
               subject_kind: "linear_coding_ticket",
               lifecycle_state: "queued",
               title: "Triage user report",
               schema_ref: SubjectPayloadSchema.default_schema_ref!("linear_coding_ticket"),
               schema_version:
                 SubjectPayloadSchema.default_schema_version!("linear_coding_ticket"),
               payload: %{
                 identifier: "linear:ticket:123",
                 source_kind: "linear",
                 title: "Triage user report"
               },
               trace_id: "trace-ingest",
               causation_id: "cause-ingest",
               actor_ref: %{kind: :intake}
             })

    assert subject.installation_id == "inst-1"
    assert subject.source_ref == "linear:ticket:123"
    assert subject.schema_ref == "mezzanine.subject.linear_coding_ticket.payload.v1"
    assert subject.schema_version == 1

    assert subject.payload == %{
             "identifier" => "linear:ticket:123",
             "source_kind" => "linear",
             "title" => "Triage user report"
           }

    assert subject.status == "active"
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
    assert audit_fact.payload["schema_ref"] == subject.schema_ref
    assert audit_fact.payload["schema_version"] == subject.schema_version
    assert audit_fact.payload["schema_hash"] =~ "sha256:"
  end

  test "ingest persists provider source metadata and progress refs" do
    assert {:ok, subject} =
             SubjectRecord.ingest(%{
               installation_id: "inst-source-metadata",
               source_ref: "linear:issue:LIN-101",
               source_event_id: "src_linear_101_rev_2",
               source_binding_id: "linear-primary",
               provider: "linear",
               provider_external_ref: "LIN-101",
               provider_revision: "rev-2",
               source_state: "Todo",
               state_mapping: %{
                 provider_state: "Todo",
                 lifecycle_state: "candidate",
                 terminal?: false
               },
               blocker_refs: [
                 %{
                   provider: "linear",
                   external_ref: "LIN-100",
                   terminal?: false,
                   relation: "blocked_by"
                 }
               ],
               labels: ["backend", "needs-triage"],
               priority: 2,
               branch_ref: "feature/lin-101",
               source_url: "https://linear.app/example/issue/LIN-101",
               workpad_ref: "linear-comment:workpad-1",
               progress_ref: "linear-comment:progress-1",
               source_routing: %{team_id: "team-1", assignee_id: "user-1"},
               lifecycle_version: 3,
               payload_schema_revision: "linear.issue.v1@2",
               subject_kind: "linear_coding_ticket",
               lifecycle_state: "candidate",
               schema_ref: SubjectPayloadSchema.default_schema_ref!("linear_coding_ticket"),
               schema_version:
                 SubjectPayloadSchema.default_schema_version!("linear_coding_ticket"),
               payload: %{
                 identifier: "linear:issue:LIN-101",
                 source_kind: "linear",
                 title: "Persist source metadata"
               },
               trace_id: "trace-source-metadata",
               causation_id: "cause-source-metadata",
               actor_ref: %{kind: :source_reducer}
             })

    assert subject.source_event_id == "src_linear_101_rev_2"
    assert subject.source_binding_id == "linear-primary"
    assert subject.provider == "linear"
    assert subject.provider_external_ref == "LIN-101"
    assert subject.provider_revision == "rev-2"
    assert subject.source_state == "Todo"
    assert subject.state_mapping["lifecycle_state"] == "candidate"
    assert [%{"external_ref" => "LIN-100"}] = subject.blocker_refs
    assert subject.labels == ["backend", "needs-triage"]
    assert subject.priority == 2
    assert subject.branch_ref == "feature/lin-101"
    assert subject.source_url =~ "LIN-101"
    assert subject.workpad_ref == "linear-comment:workpad-1"
    assert subject.progress_ref == "linear-comment:progress-1"
    assert subject.source_routing["team_id"] == "team-1"
    assert subject.lifecycle_version == 3
    assert subject.payload_schema_revision == "linear.issue.v1@2"

    assert has_index?("subject_records", [
             "installation_id",
             "source_binding_id",
             "provider_external_ref"
           ])

    assert {:ok, [audit_fact]} =
             AuditFact.list_trace("inst-source-metadata", "trace-source-metadata")

    assert audit_fact.payload["source_event_id"] == subject.source_event_id
    assert audit_fact.payload["source_binding_id"] == subject.source_binding_id
    assert audit_fact.payload["provider_external_ref"] == subject.provider_external_ref
    assert audit_fact.payload["source_state"] == subject.source_state
  end

  test "source-owned pack subject schemas are accepted for governed proofs" do
    assert {:ok, expense_subject} =
             SubjectRecord.ingest(%{
               installation_id: "inst-governed",
               source_ref: "expense:request:123",
               subject_kind: "expense_request",
               lifecycle_state: "submitted",
               schema_ref: SubjectPayloadSchema.default_schema_ref!("expense_request"),
               schema_version: SubjectPayloadSchema.default_schema_version!("expense_request"),
               payload: %{amount_cents: 12_500, merchant: "Atlas Travel"},
               trace_id: "trace-expense-schema",
               causation_id: "cause-expense-schema",
               actor_ref: %{kind: :intake}
             })

    assert expense_subject.schema_ref == "mezzanine.subject.expense_request.payload.v1"
    assert expense_subject.payload == %{"amount_cents" => 12_500, "merchant" => "Atlas Travel"}

    assert {:ok, invoice_subject} =
             SubjectRecord.ingest(%{
               installation_id: "inst-governed",
               source_ref: "invoice:request:123",
               subject_kind: "invoice_request",
               lifecycle_state: "submitted",
               schema_ref: SubjectPayloadSchema.default_schema_ref!("invoice_request"),
               schema_version: SubjectPayloadSchema.default_schema_version!("invoice_request"),
               payload: %{invoice_number: "INV-42", amount_cents: 42_000},
               trace_id: "trace-invoice-schema",
               causation_id: "cause-invoice-schema",
               actor_ref: %{kind: :intake}
             })

    assert invoice_subject.schema_ref == "mezzanine.subject.invoice_request.payload.v1"
    assert invoice_subject.payload == %{"amount_cents" => 42_000, "invoice_number" => "INV-42"}
  end

  test "ingest rejects oversized inline subject payloads before persistence" do
    assert {:error, oversized_error} =
             SubjectRecord.ingest(%{
               installation_id: "inst-1",
               source_ref: "linear:ticket:oversized-payload",
               subject_kind: "linear_coding_ticket",
               lifecycle_state: "queued",
               schema_ref: SubjectPayloadSchema.default_schema_ref!("linear_coding_ticket"),
               schema_version:
                 SubjectPayloadSchema.default_schema_version!("linear_coding_ticket"),
               payload: %{
                 identifier: "linear:ticket:oversized-payload",
                 source_kind: "linear",
                 title: String.duplicate("x", 70_000)
               },
               trace_id: "trace-oversized-subject-payload",
               causation_id: "cause-oversized-subject-payload",
               actor_ref: %{kind: :intake}
             })

    assert Exception.message(oversized_error) =~ "subject_payload_ref_required"
  end

  test "ingest rejects missing, unknown, stale, or incompatible subject payload schemas" do
    assert {:error, missing_schema_error} =
             SubjectRecord.ingest(%{
               installation_id: "inst-1",
               source_ref: "linear:ticket:missing-schema",
               subject_kind: "linear_coding_ticket",
               lifecycle_state: "queued",
               payload: %{identifier: "linear:ticket:missing-schema"},
               trace_id: "trace-missing-schema",
               causation_id: "cause-missing-schema",
               actor_ref: %{kind: :intake}
             })

    assert Exception.message(missing_schema_error) =~
             "subject payload must match a source-owned schema"

    assert {:error, missing_version_error} =
             SubjectRecord.ingest(%{
               installation_id: "inst-1",
               source_ref: "linear:ticket:missing-schema-version",
               subject_kind: "linear_coding_ticket",
               lifecycle_state: "queued",
               schema_ref: SubjectPayloadSchema.default_schema_ref!("linear_coding_ticket"),
               payload: %{identifier: "linear:ticket:missing-schema-version"},
               trace_id: "trace-missing-schema-version",
               causation_id: "cause-missing-schema-version",
               actor_ref: %{kind: :intake}
             })

    assert Exception.message(missing_version_error) =~
             "{:missing_subject_payload_schema_field, :schema_version}"

    assert {:error, unknown_ref_error} =
             SubjectRecord.ingest(%{
               installation_id: "inst-1",
               source_ref: "linear:ticket:unknown-schema-ref",
               subject_kind: "linear_coding_ticket",
               lifecycle_state: "queued",
               schema_ref: "mezzanine.subject.linear_coding_ticket.payload.unknown",
               schema_version: 1,
               payload: %{identifier: "linear:ticket:unknown-schema-ref"},
               trace_id: "trace-unknown-schema-ref",
               causation_id: "cause-unknown-schema-ref",
               actor_ref: %{kind: :intake}
             })

    assert Exception.message(unknown_ref_error) =~
             "unknown_subject_payload_schema_ref"

    assert Exception.message(unknown_ref_error) =~
             "subject-payload-schema-quarantine:"

    assert {:error, stale_version_error} =
             SubjectRecord.ingest(%{
               installation_id: "inst-1",
               source_ref: "linear:ticket:stale-schema-version",
               subject_kind: "linear_coding_ticket",
               lifecycle_state: "queued",
               schema_ref: SubjectPayloadSchema.default_schema_ref!("linear_coding_ticket"),
               schema_version: 0,
               payload: %{identifier: "linear:ticket:stale-schema-version"},
               trace_id: "trace-stale-schema-version",
               causation_id: "cause-stale-schema-version",
               actor_ref: %{kind: :intake}
             })

    assert Exception.message(stale_version_error) =~
             "stale_subject_payload_schema_version"

    assert Exception.message(stale_version_error) =~
             "subject-payload-schema-quarantine:"

    assert {:error, unknown_version_error} =
             SubjectRecord.ingest(%{
               installation_id: "inst-1",
               source_ref: "linear:ticket:future-schema-version",
               subject_kind: "linear_coding_ticket",
               lifecycle_state: "queued",
               schema_ref: SubjectPayloadSchema.default_schema_ref!("linear_coding_ticket"),
               schema_version: 2,
               payload: %{identifier: "linear:ticket:future-schema-version"},
               trace_id: "trace-future-schema-version",
               causation_id: "cause-future-schema-version",
               actor_ref: %{kind: :intake}
             })

    assert Exception.message(unknown_version_error) =~
             "unknown_subject_payload_schema_version"

    assert Exception.message(unknown_version_error) =~
             "subject-payload-schema-quarantine:"

    assert {:error, invalid_payload_error} =
             SubjectRecord.ingest(%{
               installation_id: "inst-1",
               source_ref: "linear:ticket:bad-payload",
               subject_kind: "linear_coding_ticket",
               lifecycle_state: "queued",
               schema_ref: SubjectPayloadSchema.default_schema_ref!("linear_coding_ticket"),
               schema_version:
                 SubjectPayloadSchema.default_schema_version!("linear_coding_ticket"),
               payload: %{identifier: 123},
               trace_id: "trace-bad-payload",
               causation_id: "cause-bad-payload",
               actor_ref: %{kind: :intake}
             })

    assert Exception.message(invalid_payload_error) =~
             "subject payload must match a source-owned schema"
  end

  test "pause, resume, and cancel mutate durable operator status without rewriting lifecycle truth" do
    assert {:ok, subject} =
             SubjectRecord.ingest(%{
               installation_id: "inst-1",
               source_ref: "linear:ticket:operator-status",
               subject_kind: "linear_coding_ticket",
               lifecycle_state: "awaiting_execution",
               schema_ref: SubjectPayloadSchema.default_schema_ref!("linear_coding_ticket"),
               schema_version:
                 SubjectPayloadSchema.default_schema_version!("linear_coding_ticket"),
               payload: %{},
               trace_id: "trace-status-bootstrap",
               causation_id: "cause-status-bootstrap",
               actor_ref: %{kind: :intake}
             })

    assert {:ok, paused_subject} =
             SubjectRecord.pause(subject, %{
               reason: "operator hold",
               trace_id: "trace-status-pause",
               causation_id: "cause-status-pause",
               actor_ref: %{kind: :operator}
             })

    assert paused_subject.lifecycle_state == "awaiting_execution"
    assert paused_subject.status == "paused"
    assert paused_subject.status_reason == "operator hold"
    assert %DateTime{} = paused_subject.status_updated_at

    assert {:ok, resumed_subject} =
             SubjectRecord.resume(paused_subject, %{
               trace_id: "trace-status-resume",
               causation_id: "cause-status-resume",
               actor_ref: %{kind: :operator}
             })

    assert resumed_subject.lifecycle_state == "awaiting_execution"
    assert resumed_subject.status == "active"
    assert is_nil(resumed_subject.status_reason)

    assert {:ok, cancelled_subject} =
             SubjectRecord.cancel(resumed_subject, %{
               reason: "operator cancelled",
               trace_id: "trace-status-cancel",
               causation_id: "cause-status-cancel",
               actor_ref: %{kind: :operator}
             })

    assert cancelled_subject.lifecycle_state == "awaiting_execution"
    assert cancelled_subject.status == "cancelled"
    assert cancelled_subject.status_reason == "operator cancelled"
    assert %DateTime{} = cancelled_subject.terminal_at

    assert {:ok, [pause_fact]} = AuditFact.list_trace("inst-1", "trace-status-pause")
    assert pause_fact.fact_kind == :subject_paused

    assert {:ok, [resume_fact]} = AuditFact.list_trace("inst-1", "trace-status-resume")
    assert resume_fact.fact_kind == :subject_resumed

    assert {:ok, [cancel_fact]} = AuditFact.list_trace("inst-1", "trace-status-cancel")
    assert cancel_fact.fact_kind == :subject_cancelled
  end

  test "advance_lifecycle owns canonical business progression and rejects stale writes" do
    assert {:ok, subject} =
             SubjectRecord.ingest(%{
               installation_id: "inst-1",
               source_ref: "linear:ticket:456",
               subject_kind: "linear_coding_ticket",
               lifecycle_state: "queued",
               schema_ref: SubjectPayloadSchema.default_schema_ref!("linear_coding_ticket"),
               schema_version:
                 SubjectPayloadSchema.default_schema_version!("linear_coding_ticket"),
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
               schema_ref: SubjectPayloadSchema.default_schema_ref!("linear_coding_ticket"),
               schema_version:
                 SubjectPayloadSchema.default_schema_version!("linear_coding_ticket"),
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
