defmodule Mezzanine.EvidenceLedger.PersistenceTest do
  use Mezzanine.EvidenceLedger.DataCase, async: false

  alias Mezzanine.Audit.AuditFact
  alias Mezzanine.EvidenceLedger.{EvidenceRecord, Summary}
  alias Mezzanine.Execution.ExecutionRecord
  alias Mezzanine.Objects.SubjectRecord

  test "collect persists evidence linked back to subject and execution truth" do
    assert {:ok, subject} = ingest_subject("linear:ticket:evidence-collect")
    assert {:ok, execution} = dispatch_execution(subject, "collect")

    assert {:ok, evidence} =
             EvidenceRecord.collect(%{
               installation_id: "inst-1",
               subject_id: subject.id,
               execution_id: execution.id,
               evidence_kind: "run_log",
               collector_ref: "jido_run_output",
               content_ref: "artifact://run-log-1",
               status: "collected",
               metadata: %{"size" => 512},
               trace_id: "trace-evidence-collect",
               causation_id: "cause-evidence-collect",
               actor_ref: %{kind: :collector}
             })

    assert evidence.status == "collected"
    assert evidence.execution_id == execution.id
    assert evidence.content_ref == "artifact://run-log-1"

    assert {:ok, [audit_fact]} = AuditFact.list_trace("inst-1", "trace-evidence-collect")
    assert audit_fact.fact_kind == :evidence_collected
    assert audit_fact.evidence_id == evidence.id
  end

  test "verify promotes pending evidence into verified state and emits audit" do
    assert {:ok, subject} = ingest_subject("linear:ticket:evidence-verify")
    assert {:ok, execution} = dispatch_execution(subject, "verify")

    assert {:ok, evidence} =
             EvidenceRecord.collect(%{
               installation_id: "inst-1",
               subject_id: subject.id,
               execution_id: execution.id,
               evidence_kind: "pull_request",
               collector_ref: "github_pr_finder",
               content_ref: nil,
               status: "pending",
               metadata: %{},
               trace_id: "trace-evidence-bootstrap",
               causation_id: "cause-evidence-bootstrap",
               actor_ref: %{kind: :collector}
             })

    assert {:ok, verified_evidence} =
             EvidenceRecord.verify(evidence, %{
               content_ref: "artifact://pull-request-1",
               metadata: %{"state" => "verified"},
               trace_id: "trace-evidence-verify",
               causation_id: "cause-evidence-verify",
               actor_ref: %{kind: :reviewer, id: "alice"}
             })

    assert verified_evidence.status == "verified"
    assert verified_evidence.content_ref == "artifact://pull-request-1"

    assert {:ok, [audit_fact]} = AuditFact.list_trace("inst-1", "trace-evidence-verify")
    assert audit_fact.fact_kind == :evidence_verified
    assert audit_fact.evidence_id == evidence.id
  end

  test "completeness bookkeeping stays incomplete until all required evidence is present" do
    assert {:ok, subject} = ingest_subject("linear:ticket:evidence-summary")
    assert {:ok, execution} = dispatch_execution(subject, "summary")

    assert :incomplete ==
             Summary.completeness(subject.id, execution.id, ["run_log", "pull_request"])

    assert {:ok, _run_log} =
             EvidenceRecord.collect(%{
               installation_id: "inst-1",
               subject_id: subject.id,
               execution_id: execution.id,
               evidence_kind: "run_log",
               collector_ref: "jido_run_output",
               content_ref: "artifact://run-log-2",
               status: "collected",
               metadata: %{},
               trace_id: "trace-evidence-run-log",
               causation_id: "cause-evidence-run-log",
               actor_ref: %{kind: :collector}
             })

    assert :incomplete ==
             Summary.completeness(subject.id, execution.id, ["run_log", "pull_request"])

    assert {:ok, _pull_request} =
             EvidenceRecord.collect(%{
               installation_id: "inst-1",
               subject_id: subject.id,
               execution_id: execution.id,
               evidence_kind: "pull_request",
               collector_ref: "github_pr_finder",
               content_ref: "artifact://pull-request-2",
               status: "verified",
               metadata: %{},
               trace_id: "trace-evidence-pull-request",
               causation_id: "cause-evidence-pull-request",
               actor_ref: %{kind: :collector}
             })

    assert :complete ==
             Summary.completeness(subject.id, execution.id, ["run_log", "pull_request"])
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
      submission_dedupe_key: "inst-1:evidence:#{suffix}",
      trace_id: "trace-execution-#{suffix}",
      causation_id: "cause-execution-#{suffix}",
      actor_ref: %{kind: :scheduler}
    })
  end
end
