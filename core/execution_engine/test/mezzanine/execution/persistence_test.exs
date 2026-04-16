defmodule Mezzanine.Execution.PersistenceTest do
  use Mezzanine.Execution.DataCase, async: false

  alias Mezzanine.Audit.{AuditFact, ExecutionLineageStore}
  alias Mezzanine.Execution.{DispatchOutboxEntry, ExecutionRecord}
  alias Mezzanine.Objects.SubjectRecord

  test "dispatch persists execution records, outbox truth, and lineage joins" do
    assert {:ok, subject} = ingest_subject("linear:ticket:dispatch")

    assert {:ok, execution} =
             ExecutionRecord.dispatch(%{
               installation_id: "inst-1",
               subject_id: subject.id,
               recipe_ref: "triage_ticket",
               compiled_pack_revision: 7,
               binding_snapshot: %{
                 "placement_ref" => "local_docker",
                 "connector_bindings" => %{"github_write" => %{"connector_key" => "github_app"}}
               },
               dispatch_envelope: %{"capability" => "sandbox.exec"},
               submission_dedupe_key: "inst-1:exec:dispatch",
               trace_id: "trace-dispatch",
               causation_id: "cause-dispatch",
               actor_ref: %{kind: :scheduler}
             })

    assert execution.dispatch_state == :pending_dispatch
    assert execution.dispatch_attempt_count == 0
    assert execution.trace_id == "trace-dispatch"
    assert execution.compiled_pack_revision == 7

    assert execution.binding_snapshot == %{
             "placement_ref" => "local_docker",
             "connector_bindings" => %{"github_write" => %{"connector_key" => "github_app"}}
           }

    assert {:ok, outbox} = DispatchOutboxEntry.by_execution_id(execution.id)
    assert outbox.status == :pending
    assert outbox.submission_dedupe_key == "inst-1:exec:dispatch"
    assert outbox.execution_id == execution.id
    assert outbox.compiled_pack_revision == 7

    assert outbox.binding_snapshot == %{
             "placement_ref" => "local_docker",
             "connector_bindings" => %{"github_write" => %{"connector_key" => "github_app"}}
           }

    assert {:ok, lineage} = ExecutionLineageStore.fetch(execution.id)
    assert lineage.dispatch_outbox_entry_id == outbox.id

    assert {:ok, [audit_fact]} = AuditFact.list_trace("inst-1", "trace-dispatch")
    assert audit_fact.fact_kind == :execution_dispatched
    assert audit_fact.execution_id == execution.id
  end

  test "accepted dispatch stores receipt and completes the outbox row" do
    assert {:ok, subject} = ingest_subject("linear:ticket:accepted")
    assert {:ok, execution} = dispatch_execution(subject, "trace-accepted-bootstrap", "accepted")

    assert {:ok, accepted_execution} =
             ExecutionRecord.record_accepted(execution, %{
               submission_ref: %{"id" => "sub-1"},
               lower_receipt: %{"state" => "accepted"},
               trace_id: "trace-accepted",
               causation_id: "cause-accepted",
               actor_ref: %{kind: :dispatcher}
             })

    assert accepted_execution.dispatch_state == :accepted
    assert accepted_execution.submission_ref == %{"id" => "sub-1"}
    assert accepted_execution.lower_receipt == %{"state" => "accepted"}

    assert {:ok, outbox} = DispatchOutboxEntry.by_execution_id(execution.id)
    assert outbox.status == :completed
  end

  test "retryable dispatch failure reschedules without duplicating rows" do
    retry_at = ~U[2026-04-16 03:00:00.000000Z]

    assert {:ok, subject} = ingest_subject("linear:ticket:retry")
    assert {:ok, execution} = dispatch_execution(subject, "trace-retry-bootstrap", "retry")
    assert {:ok, original_outbox} = DispatchOutboxEntry.by_execution_id(execution.id)

    assert {:ok, retryable_execution} =
             ExecutionRecord.record_retryable_failure(execution, %{
               last_dispatch_error_kind: "bridge_unavailable",
               last_dispatch_error_payload: %{"reason" => "timeout"},
               next_dispatch_at: retry_at,
               trace_id: "trace-retry",
               causation_id: "cause-retry",
               actor_ref: %{kind: :dispatcher}
             })

    assert retryable_execution.dispatch_state == :dispatching_retry
    assert retryable_execution.dispatch_attempt_count == 1
    assert retryable_execution.next_dispatch_at == retry_at

    assert {:ok, outbox} = DispatchOutboxEntry.by_execution_id(execution.id)
    assert outbox.id == original_outbox.id
    assert outbox.status == :pending_retry
    assert outbox.available_at == retry_at
  end

  test "terminal rejection marks the execution rejected and stops retries" do
    assert {:ok, subject} = ingest_subject("linear:ticket:reject")
    assert {:ok, execution} = dispatch_execution(subject, "trace-reject-bootstrap", "reject")

    assert {:ok, rejected_execution} =
             ExecutionRecord.record_terminal_rejection(execution, %{
               terminal_rejection_reason: "unsupported_capability",
               last_dispatch_error_payload: %{"capability" => "prod_ssh_write"},
               trace_id: "trace-reject",
               causation_id: "cause-reject",
               actor_ref: %{kind: :dispatcher}
             })

    assert rejected_execution.dispatch_state == :rejected
    assert rejected_execution.terminal_rejection_reason == "unsupported_capability"
    refute ExecutionRecord.has_active_execution?(subject.id)

    assert {:ok, outbox} = DispatchOutboxEntry.by_execution_id(execution.id)
    assert outbox.status == :terminal
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

  defp dispatch_execution(subject, trace_id, suffix) do
    ExecutionRecord.dispatch(%{
      installation_id: "inst-1",
      subject_id: subject.id,
      recipe_ref: "triage_ticket",
      compiled_pack_revision: 1,
      binding_snapshot: %{},
      dispatch_envelope: %{"capability" => "sandbox.exec"},
      submission_dedupe_key: "inst-1:exec:#{suffix}",
      trace_id: trace_id,
      causation_id: "cause-#{suffix}",
      actor_ref: %{kind: :scheduler}
    })
  end
end
