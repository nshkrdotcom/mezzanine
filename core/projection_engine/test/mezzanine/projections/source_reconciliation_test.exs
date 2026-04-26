defmodule Mezzanine.Projections.SourceReconciliationTest do
  use Mezzanine.Projections.DataCase, async: false

  alias Mezzanine.Execution.ExecutionRecord
  alias Mezzanine.Objects.SubjectRecord
  alias Mezzanine.Projections.{ProjectionRow, SourceReconciliation}

  test "terminal source state stops active work and records completed totals" do
    %{subject: subject, execution: execution} = reconciliation_fixture("terminal")

    assert {:ok, result} =
             SourceReconciliation.reconcile(%{
               installation_id: "inst-1",
               subject_id: subject.id,
               execution_id: execution.id,
               source_state: "Done",
               canonical_state: "completed",
               source_revision: "rev-terminal",
               trace_id: "trace-source-terminal",
               causation_id: "cause-source-terminal",
               actor_ref: %{kind: :source_reconciler}
             })

    assert result.subject.lifecycle_state == "completed"
    assert result.action.safe_action == "stop_lower_run"

    assert {:ok, completed_totals} =
             ProjectionRow.row_by_key("inst-1", "source_reconciliation_totals", "completed")

    assert completed_totals.payload["completed_count"] == 1
  end

  test "missing source stops work and quarantines the subject with projection evidence" do
    %{subject: subject, execution: execution} = reconciliation_fixture("missing")

    assert {:ok, result} =
             SourceReconciliation.reconcile(%{
               installation_id: "inst-1",
               subject_id: subject.id,
               execution_id: execution.id,
               source_visible?: false,
               source_revision: "rev-missing",
               trace_id: "trace-source-missing",
               causation_id: "cause-source-missing",
               actor_ref: %{kind: :source_reconciler}
             })

    assert result.subject.lifecycle_state == "quarantined"
    assert result.action.reason == "source_missing"

    assert {:ok, row} =
             ProjectionRow.row_by_key("inst-1", "source_reconciliation_queue", subject.id)

    assert row.payload["safe_action"] == "stop_lower_run"
    assert row.payload["reason"] == "source_missing"
  end

  test "reassignment, blockers, stale source, and out-of-band updates produce stable actions" do
    reassigned = reconciliation_fixture("reassigned")

    assert {:ok, reassigned_result} =
             SourceReconciliation.reconcile(%{
               installation_id: "inst-1",
               subject_id: reassigned.subject.id,
               execution_id: reassigned.execution.id,
               assigned_to_current_worker?: false,
               source_revision: "rev-reassigned",
               trace_id: "trace-source-reassigned",
               causation_id: "cause-source-reassigned",
               actor_ref: %{kind: :source_reconciler}
             })

    assert reassigned_result.subject.block_reason == "source_reassigned"
    assert reassigned_result.action.safe_action == "stop_lower_run"

    blocked = reconciliation_fixture("blocked")

    assert {:ok, blocked_result} =
             SourceReconciliation.reconcile(%{
               installation_id: "inst-1",
               subject_id: blocked.subject.id,
               execution_id: blocked.execution.id,
               blocker_refs: [%{"external_ref" => "LIN-100", "terminal?" => false}],
               source_revision: "rev-blocked",
               trace_id: "trace-source-blocked",
               causation_id: "cause-source-blocked",
               actor_ref: %{kind: :source_reconciler}
             })

    assert blocked_result.subject.block_reason == "blocked_by_source"
    assert blocked_result.action.safe_action == "skip_dispatch"

    stale = reconciliation_fixture("stale")

    assert {:ok, stale_result} =
             SourceReconciliation.reconcile(%{
               installation_id: "inst-1",
               subject_id: stale.subject.id,
               execution_id: stale.execution.id,
               stale?: true,
               retry_at: ~U[2026-04-25 13:00:00Z],
               source_revision: "rev-stale",
               trace_id: "trace-source-stale",
               causation_id: "cause-source-stale",
               actor_ref: %{kind: :source_reconciler}
             })

    assert stale_result.action.safe_action == "retry_source_refresh"

    assert {:ok, retry_row} =
             ProjectionRow.row_by_key("inst-1", "source_revalidation_queue", stale.subject.id)

    assert retry_row.payload["retry_at"] == "2026-04-25T13:00:00Z"

    changed = reconciliation_fixture("changed")

    assert {:ok, changed_result} =
             SourceReconciliation.reconcile(%{
               installation_id: "inst-1",
               subject_id: changed.subject.id,
               execution_id: changed.execution.id,
               source_revision: "rev-new",
               payload_changed?: true,
               trace_id: "trace-source-changed",
               causation_id: "cause-source-changed",
               actor_ref: %{kind: :source_reconciler}
             })

    assert changed_result.action.reason == "source_updated"

    assert {:ok, row} =
             ProjectionRow.row_by_key("inst-1", "source_reconciliation_queue", changed.subject.id)

    assert row.payload["source_revision"] == "rev-new"
    assert row.payload["safe_action"] == "refresh_subject_projection"
  end

  defp reconciliation_fixture(suffix) do
    source_ref =
      "linear:ticket:source-reconciliation-#{suffix}-#{System.unique_integer([:positive])}"

    {:ok, subject} =
      SubjectRecord.ingest(%{
        installation_id: "inst-1",
        source_ref: source_ref,
        source_event_id: "source-event-#{suffix}",
        source_binding_id: "source-binding-1",
        provider: "linear",
        provider_external_ref: "LIN-#{System.unique_integer([:positive])}",
        provider_revision: "1",
        source_state: "In Progress",
        subject_kind: "linear_coding_ticket",
        lifecycle_state: "running",
        status: "active",
        title: "Source reconciliation #{suffix}",
        schema_ref: "mezzanine.subject.linear_coding_ticket.payload.v1",
        schema_version: 1,
        payload: %{},
        trace_id: "trace-subject-reconcile-#{suffix}",
        causation_id: "cause-subject-reconcile-#{suffix}",
        actor_ref: %{kind: :source}
      })

    {:ok, execution} =
      ExecutionRecord.dispatch(%{
        tenant_id: "tenant-1",
        installation_id: "inst-1",
        subject_id: subject.id,
        recipe_ref: "coding_ops",
        dispatch_envelope: %{"capability" => "codex.session.turn"},
        submission_dedupe_key: "inst-1:source-reconcile:#{suffix}",
        trace_id: "trace-execution-reconcile-#{suffix}",
        causation_id: "cause-execution-reconcile-#{suffix}",
        actor_ref: %{kind: :scheduler}
      })

    %{subject: subject, execution: execution}
  end
end
