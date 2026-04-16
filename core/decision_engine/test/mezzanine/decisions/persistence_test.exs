defmodule Mezzanine.Decisions.PersistenceTest do
  use Mezzanine.Decisions.DataCase, async: false

  alias Mezzanine.Audit.AuditFact
  alias Mezzanine.Decisions.DecisionRecord
  alias Mezzanine.Execution.ExecutionRecord
  alias Mezzanine.Objects.SubjectRecord

  test "create_pending persists the decision ledger and emits audit" do
    assert {:ok, subject} = ingest_subject("linear:ticket:decision-pending")
    assert {:ok, execution} = dispatch_execution(subject, "decision-pending")

    assert {:ok, decision} =
             DecisionRecord.create_pending(%{
               installation_id: "inst-1",
               subject_id: subject.id,
               execution_id: execution.id,
               decision_kind: "human_review_required",
               required_by: ~U[2026-04-20 00:00:00.000000Z],
               trace_id: "trace-decision-pending",
               causation_id: "cause-decision-pending",
               actor_ref: %{kind: :scheduler}
             })

    assert decision.lifecycle_state == "pending"
    assert decision.execution_id == execution.id
    assert DecisionRecord.exists?(subject.id, "human_review_required")

    assert {:ok, [audit_fact]} = AuditFact.list_trace("inst-1", "trace-decision-pending")
    assert audit_fact.fact_kind == :decision_created
    assert audit_fact.decision_id == decision.id
  end

  test "decide resolves the row and exposes resolved-for-subject reads" do
    assert {:ok, subject} = ingest_subject("linear:ticket:decision-resolve")
    assert {:ok, execution} = dispatch_execution(subject, "decision-resolve")
    assert {:ok, decision} = create_pending_decision(subject, execution, "resolve")

    assert {:ok, resolved_decision} =
             DecisionRecord.decide(decision, %{
               decision_value: "accept",
               reason: "approved by reviewer",
               trace_id: "trace-decision-resolve",
               causation_id: "cause-decision-resolve",
               actor_ref: %{kind: :reviewer, id: "alice"}
             })

    assert resolved_decision.lifecycle_state == "resolved"
    assert resolved_decision.decision_value == "accept"
    assert resolved_decision.reason == "approved by reviewer"

    assert {:ok, [resolved_row]} = DecisionRecord.resolved_for_subject(subject.id)
    assert resolved_row.id == resolved_decision.id
  end

  test "read_overdue and expire move pending decisions into explicit expiry state" do
    assert {:ok, subject} = ingest_subject("linear:ticket:decision-expire")
    assert {:ok, execution} = dispatch_execution(subject, "decision-expire")

    assert {:ok, decision} =
             DecisionRecord.create_pending(%{
               installation_id: "inst-1",
               subject_id: subject.id,
               execution_id: execution.id,
               decision_kind: "security_review_required",
               required_by: ~U[2026-04-14 00:00:00.000000Z],
               trace_id: "trace-decision-bootstrap",
               causation_id: "cause-decision-bootstrap",
               actor_ref: %{kind: :scheduler}
             })

    assert {:ok, overdue_rows} =
             DecisionRecord.read_overdue("inst-1", ~U[2026-04-15 00:00:00.000000Z])

    assert Enum.any?(overdue_rows, &(&1.id == decision.id))

    assert {:ok, expired_decision} =
             DecisionRecord.expire(decision, %{
               trace_id: "trace-decision-expire",
               causation_id: "cause-decision-expire",
               actor_ref: %{kind: :sla_monitor}
             })

    assert expired_decision.lifecycle_state == "expired"
    assert expired_decision.decision_value == "expired"
  end

  defp ingest_subject(source_ref) do
    SubjectRecord.ingest(%{
      installation_id: "inst-1",
      source_ref: source_ref,
      subject_kind: "linear_coding_ticket",
      lifecycle_state: "queued",
      payload: %{},
      trace_id: "trace-subject-#{source_ref}",
      causation_id: "cause-subject-#{source_ref}",
      actor_ref: %{kind: :intake}
    })
  end

  defp dispatch_execution(subject, suffix) do
    ExecutionRecord.dispatch(%{
      installation_id: "inst-1",
      subject_id: subject.id,
      recipe_ref: "triage_ticket",
      dispatch_envelope: %{"capability" => "sandbox.exec"},
      submission_dedupe_key: "inst-1:decision:#{suffix}",
      trace_id: "trace-execution-#{suffix}",
      causation_id: "cause-execution-#{suffix}",
      actor_ref: %{kind: :scheduler}
    })
  end

  defp create_pending_decision(subject, execution, suffix) do
    DecisionRecord.create_pending(%{
      installation_id: "inst-1",
      subject_id: subject.id,
      execution_id: execution.id,
      decision_kind: "human_review_required",
      required_by: ~U[2026-04-20 00:00:00.000000Z],
      trace_id: "trace-decision-create-#{suffix}",
      causation_id: "cause-decision-create-#{suffix}",
      actor_ref: %{kind: :scheduler}
    })
  end
end
