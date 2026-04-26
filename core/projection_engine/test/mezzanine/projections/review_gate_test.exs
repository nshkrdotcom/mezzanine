defmodule Mezzanine.Projections.ReviewGateTest do
  use Mezzanine.Projections.DataCase, async: false

  alias Mezzanine.Audit.AuditFact
  alias Mezzanine.DecisionCommands
  alias Mezzanine.Execution.ExecutionRecord
  alias Mezzanine.Objects.SubjectRecord
  alias Mezzanine.Projections.{ProjectionRow, ReviewGate}

  test "accept completes the review-gated subject and records operator projection state" do
    %{decision: decision, subject: subject} = review_fixture("accept")

    assert {:ok, result} =
             ReviewGate.resolve(%{
               decision_id: decision.id,
               decision_action: :accept,
               reason: "ready to release",
               trace_id: "trace-review-accept",
               causation_id: "cause-review-accept",
               actor_ref: %{kind: :operator, id: "ops-1"}
             })

    assert result.decision.lifecycle_state == "resolved"
    assert result.decision.decision_value == "accept"
    assert result.subject.lifecycle_state == "completed"

    assert {:ok, projection} =
             ProjectionRow.row_by_key("inst-1", "review_gate_runtime", decision.id)

    assert projection.subject_id == subject.id
    assert projection.payload["decision"]["decision_value"] == "accept"
    assert projection.payload["subject"]["lifecycle_state"] == "completed"

    assert {:ok, audit_facts} = AuditFact.list_trace("inst-1", "trace-review-accept")
    audit_fact = Enum.find(audit_facts, &(&1.fact_kind == :review_gate_resolved))
    assert audit_fact.payload["decision_action"] == "accept"
  end

  test "reject creates a durable rework path without static provider selectors" do
    %{decision: decision, subject: subject, execution: execution} = review_fixture("reject")

    assert {:ok, result} =
             ReviewGate.resolve(%{
               decision_id: decision.id,
               decision_action: "reject",
               reason: "tests missing",
               review_policy: %{
                 rework_lifecycle_state: "needs_rework",
                 rework_recipe_ref: "coding_ops.rework",
                 rework_reason: "review_rejected"
               },
               trace_id: "trace-review-reject",
               causation_id: "cause-review-reject",
               actor_ref: %{kind: :operator, id: "ops-2"}
             })

    assert result.decision.decision_value == "reject"
    assert result.subject.lifecycle_state == "needs_rework"
    assert result.projection.projection_name == "review_rework_queue"

    assert {:ok, projection} =
             ProjectionRow.row_by_key("inst-1", "review_rework_queue", subject.id)

    assert projection.execution_id == execution.id
    assert projection.payload["safe_action"] == "operator.rework"
    assert projection.payload["reason"] == "review_rejected"
    assert projection.payload["rework_recipe_ref"] == "coding_ops.rework"
    refute Map.has_key?(projection.payload, "github_issue_number")
    refute Map.has_key?(projection.payload, "linear_issue_id")
  end

  test "waive expire and escalate follow configured lifecycle and projection policy" do
    waive = review_fixture("waive")

    assert {:ok, waive_result} =
             ReviewGate.resolve(%{
               decision_id: waive.decision.id,
               decision_action: :waive,
               reason: "operator accepted existing evidence",
               review_policy: %{waive_lifecycle_state: "completed_by_waiver"},
               trace_id: "trace-review-waive",
               causation_id: "cause-review-waive",
               actor_ref: %{kind: :operator, id: "ops-waive"}
             })

    assert waive_result.decision.lifecycle_state == "waived"
    assert waive_result.subject.lifecycle_state == "completed_by_waiver"

    expired = review_fixture("expire")

    assert {:ok, expired_result} =
             ReviewGate.resolve(%{
               decision_id: expired.decision.id,
               decision_action: :expire,
               reason: "review SLA expired",
               review_policy: %{expire_lifecycle_state: "review_expired"},
               trace_id: "trace-review-expire",
               causation_id: "cause-review-expire",
               actor_ref: %{kind: :workflow_timer}
             })

    assert expired_result.decision.lifecycle_state == "expired"
    assert expired_result.subject.lifecycle_state == "review_expired"
    assert expired_result.projection.projection_name == "review_expiry_queue"

    escalated = review_fixture("escalate")

    assert {:ok, escalated_result} =
             ReviewGate.resolve(%{
               decision_id: escalated.decision.id,
               decision_action: :escalate,
               reason: "security owner required",
               review_policy: %{
                 escalate_lifecycle_state: "awaiting_security_review",
                 escalation_owner_ref: "team:security"
               },
               trace_id: "trace-review-escalate",
               causation_id: "cause-review-escalate",
               actor_ref: %{kind: :operator, id: "ops-escalate"}
             })

    assert escalated_result.decision.lifecycle_state == "escalated"
    assert escalated_result.subject.lifecycle_state == "awaiting_security_review"
    assert escalated_result.projection.projection_name == "review_escalation_queue"
    assert escalated_result.projection.payload["escalation_owner_ref"] == "team:security"
  end

  defp review_fixture(suffix) do
    source_ref = "linear:ticket:review-gate-#{suffix}-#{System.unique_integer([:positive])}"

    {:ok, subject} =
      SubjectRecord.ingest(%{
        installation_id: "inst-1",
        source_ref: source_ref,
        source_event_id: "source-review-#{suffix}",
        source_binding_id: "source-binding-1",
        provider: "linear",
        provider_external_ref: "LIN-#{System.unique_integer([:positive])}",
        provider_revision: "1",
        source_state: "Review",
        state_mapping: %{"Review" => "awaiting_review"},
        subject_kind: "linear_coding_ticket",
        lifecycle_state: "awaiting_review",
        status: "active",
        title: "Review gate #{suffix}",
        schema_ref: "mezzanine.subject.linear_coding_ticket.payload.v1",
        schema_version: 1,
        payload: %{},
        trace_id: "trace-review-subject-#{suffix}",
        causation_id: "cause-review-subject-#{suffix}",
        actor_ref: %{kind: :source}
      })

    {:ok, execution} =
      ExecutionRecord.dispatch(%{
        tenant_id: "tenant-1",
        installation_id: "inst-1",
        subject_id: subject.id,
        recipe_ref: "coding_ops",
        dispatch_envelope: %{"capability" => "codex.session.turn"},
        submission_dedupe_key: "inst-1:review-gate:#{suffix}",
        trace_id: "trace-review-execution-#{suffix}",
        causation_id: "cause-review-execution-#{suffix}",
        actor_ref: %{kind: :scheduler}
      })

    {:ok, decision} =
      DecisionCommands.create_pending(%{
        installation_id: "inst-1",
        subject_id: subject.id,
        execution_id: execution.id,
        decision_kind: "operator_review_required",
        required_by: ~U[2026-04-25 12:00:00Z],
        trace_id: "trace-review-decision-#{suffix}",
        causation_id: "cause-review-decision-#{suffix}",
        actor_ref: %{kind: :workflow}
      })

    %{subject: subject, execution: execution, decision: decision}
  end
end
