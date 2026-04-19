defmodule Mezzanine.Audit.OperationalContractTest do
  use ExUnit.Case, async: true

  alias Mezzanine.Audit.{
    ExecutionLineage,
    Staleness,
    TraceContract,
    UnifiedTrace
  }

  alias Mezzanine.Audit.UnifiedTrace.{Query, Timeline}

  test "trace contract freezes indexed join keys and public lookup posture" do
    assert [:trace_id, :causation_id] == TraceContract.indexed_join_keys()

    assert [:audit_fact, :execution_record, :decision_record, :evidence_record] ==
             TraceContract.indexed_ledger_families()

    assert [:installation_id, :subject_id, :execution_id, :trace_id] ==
             TraceContract.public_lookup_keys()

    refute :lower_run_id in TraceContract.public_lookup_keys()
    refute :lower_attempt_id in TraceContract.public_lookup_keys()
  end

  test "execution lineage keeps lower identifiers internal to the bridge contract" do
    lineage =
      ExecutionLineage.new!(%{
        trace_id: "trace-1",
        causation_id: "cause-1",
        installation_id: "inst-1",
        subject_id: "subject-1",
        execution_id: "exec-1",
        citadel_request_id: "citadel-req-1",
        citadel_submission_id: "citadel-sub-1",
        ji_submission_key: "ji-sub-1",
        lower_run_id: "run-1",
        lower_attempt_id: "attempt-1",
        artifact_refs: ["artifact-1"]
      })

    assert %{
             installation_id: "inst-1",
             subject_id: "subject-1",
             execution_id: "exec-1",
             trace_id: "trace-1"
           } == ExecutionLineage.public_lookup(lineage)

    assert %{
             citadel_submission_id: "citadel-sub-1",
             ji_submission_key: "ji-sub-1",
             lower_run_id: "run-1",
             lower_attempt_id: "attempt-1",
             artifact_refs: ["artifact-1"]
           } == ExecutionLineage.lower_identifiers(lineage)
  end

  test "unified trace assembles the operator-facing 3 AM query timeline" do
    query = Query.new!(%{trace_id: "trace-1", installation_id: "inst-1"})

    assert {:ok, %Timeline{} = timeline} =
             UnifiedTrace.assemble(query, %{
               audit_facts: [
                 %{
                   id: "audit-1",
                   trace_id: "trace-1",
                   causation_id: nil,
                   occurred_at: ~U[2026-04-15 10:00:00Z],
                   event: :dispatch_requested,
                   payload: %{dispatch_state: :pending_dispatch}
                 }
               ],
               executions: [
                 %{
                   id: "exec-1",
                   trace_id: "trace-1",
                   causation_id: "audit-1",
                   occurred_at: ~U[2026-04-15 10:01:00Z],
                   dispatch_state: :accepted
                 }
               ],
               decisions: [
                 %{
                   id: "decision-1",
                   trace_id: "trace-1",
                   causation_id: "exec-1",
                   occurred_at: ~U[2026-04-15 10:03:00Z],
                   decision_state: :pending
                 }
               ],
               evidence: [],
               lower_facts: [
                 %{
                   id: "run-1",
                   trace_id: "trace-1",
                   causation_id: "exec-1",
                   occurred_at: ~U[2026-04-15 10:02:00Z],
                   source: :lower_run_status,
                   run_state: :running
                 }
               ]
             })

    assert ["audit-1", "exec-1", "run-1", "decision-1"] ==
             Enum.map(timeline.steps, & &1.ref)

    assert Enum.all?(timeline.steps, &(&1.trace_id == "trace-1"))

    lower_step = Enum.find(timeline.steps, &(&1.ref == "run-1"))
    assert lower_step.staleness_class == :lower_fresh
    refute lower_step.operator_actionable?

    authoritative_steps =
      Enum.filter(
        timeline.steps,
        &(Staleness.operator_actionable?(&1.staleness_class) and &1.ref != "run-1")
      )

    assert ["audit-1", "exec-1", "decision-1"] == Enum.map(authoritative_steps, & &1.ref)
  end

  test "diagnostic lower facts stay out of the default unified trace response" do
    query = Query.new!(%{trace_id: "trace-1", installation_id: "inst-1"})

    assert {:ok, %Timeline{} = timeline} =
             UnifiedTrace.assemble(query, %{
               audit_facts: [],
               executions: [],
               decisions: [],
               evidence: [],
               lower_facts: [
                 %{
                   id: "diag-1",
                   trace_id: "trace-1",
                   causation_id: nil,
                   occurred_at: ~U[2026-04-15 10:04:00Z],
                   source: :bridge_diagnostic,
                   payload: %{latency_ms: 12}
                 }
               ]
             })

    assert [] == timeline.steps
  end
end
