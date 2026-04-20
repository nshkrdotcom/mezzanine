defmodule Mezzanine.Execution.PersistenceTest do
  use Mezzanine.Execution.DataCase, async: false

  alias Mezzanine.Audit.{AuditFact, ExecutionLineageStore}
  alias Mezzanine.Execution.{ExecutionRecord, Repo}

  test "dispatch persists execution records, records a Temporal workflow handoff, and stores lineage joins" do
    assert {:ok, subject} = ingest_subject("linear:ticket:dispatch")

    assert {:ok, execution} =
             ExecutionRecord.dispatch(%{
               tenant_id: "tenant-1",
               installation_id: "inst-1",
               subject_id: subject.id,
               recipe_ref: "triage_ticket",
               compiled_pack_revision: 7,
               binding_snapshot: %{
                 "placement_ref" => "local_docker",
                 "connector_bindings" => %{"github_write" => %{"connector_key" => "github_app"}}
               },
               dispatch_envelope: %{"capability" => "sandbox.exec"},
               intent_snapshot: %{
                 "recipe_ref" => "triage_ticket",
                 "required_lifecycle_hints" => ["ticket_status"]
               },
               submission_dedupe_key: "inst-1:exec:dispatch",
               trace_id: "trace-dispatch",
               causation_id: "cause-dispatch",
               actor_ref: %{kind: :scheduler}
             })

    assert execution.dispatch_state == :queued
    assert execution.dispatch_attempt_count == 0
    assert execution.trace_id == "trace-dispatch"
    assert execution.compiled_pack_revision == 7
    assert execution.dispatch_envelope == %{"capability" => "sandbox.exec"}

    assert execution.intent_snapshot == %{
             "recipe_ref" => "triage_ticket",
             "required_lifecycle_hints" => ["ticket_status"]
           }

    assert execution.submission_dedupe_key == "inst-1:exec:dispatch"

    assert execution.binding_snapshot == %{
             "placement_ref" => "local_docker",
             "connector_bindings" => %{"github_write" => %{"connector_key" => "github_app"}}
           }

    assert {:ok, handoff} = ExecutionRecord.enqueue_dispatch(execution)
    assert handoff.provider == :temporal_workflow
    assert handoff.workflow_type == :execution_attempt
    assert handoff.workflow_module == "Mezzanine.Workflows.ExecutionAttempt"
    assert handoff.workflow_runtime_boundary == "Mezzanine.WorkflowRuntime"
    assert handoff.task_queue == "mezzanine.hazmat"
    assert handoff.execution_id == execution.id
    assert handoff.tenant_id == "tenant-1"
    assert handoff.release_manifest_ref == "phase4-v6-milestone31-temporal-cutover"
    assert retired_dispatch_jobs(execution.id) == []

    assert {:ok, lineage} = ExecutionLineageStore.fetch(execution.id)
    assert lineage.execution_id == execution.id
    assert lineage.trace_id == "trace-dispatch"
    assert is_nil(lineage.ji_submission_key)
    assert is_nil(lineage.lower_run_id)

    assert {:ok, [audit_fact]} = AuditFact.list_trace("inst-1", "trace-dispatch")
    assert audit_fact.fact_kind == :execution_dispatched
    assert audit_fact.execution_id == execution.id
  end

  test "accepted dispatch stores lower receipt and enriches execution lineage" do
    assert {:ok, subject} = ingest_subject("linear:ticket:accepted")
    assert {:ok, execution} = dispatch_execution(subject, "trace-accepted-bootstrap", "accepted")

    assert {:ok, accepted_execution} =
             ExecutionRecord.record_accepted(execution, %{
               submission_ref: %{"id" => "sub-1"},
               lower_receipt: %{
                 "state" => "accepted",
                 "ji_submission_key" => "ji-sub-1",
                 "run_id" => "run-1"
               },
               trace_id: "trace-accepted",
               causation_id: "cause-accepted",
               actor_ref: %{kind: :dispatcher}
             })

    assert accepted_execution.dispatch_state == :accepted_active
    assert accepted_execution.submission_ref == %{"id" => "sub-1"}

    assert accepted_execution.lower_receipt == %{
             "state" => "accepted",
             "ji_submission_key" => "ji-sub-1",
             "run_id" => "run-1"
           }

    assert {:ok, lineage} = ExecutionLineageStore.fetch(execution.id)
    assert lineage.ji_submission_key == "ji-sub-1"
    assert lineage.lower_run_id == "run-1"
  end

  test "retryable dispatch failure updates retry state without duplicating execution rows" do
    retry_at = ~U[2026-04-16 03:00:00.000000Z]

    assert {:ok, subject} = ingest_subject("linear:ticket:retry")
    assert {:ok, execution} = dispatch_execution(subject, "trace-retry-bootstrap", "retry")

    assert {:ok, retryable_execution} =
             ExecutionRecord.record_retryable_failure(execution, %{
               last_dispatch_error_kind: "bridge_unavailable",
               last_dispatch_error_payload: %{"reason" => "timeout"},
               next_dispatch_at: retry_at,
               trace_id: "trace-retry",
               causation_id: "cause-retry",
               actor_ref: %{kind: :dispatcher}
             })

    assert retryable_execution.dispatch_state == :in_flight
    assert retryable_execution.dispatch_attempt_count == 1
    assert retryable_execution.next_dispatch_at == retry_at
    assert retryable_execution.last_dispatch_error_kind == "bridge_unavailable"
  end

  test "terminal rejection marks the execution rejected and stops active-work detection" do
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
  end

  test "semantic failure preserves execution trace identity and lower lineage joins" do
    assert {:ok, subject} = ingest_subject("linear:ticket:semantic-failure")
    assert {:ok, execution} = dispatch_execution(subject, "trace-semantic-bootstrap", "semantic")

    assert {:ok, accepted_execution} =
             ExecutionRecord.record_accepted(execution, %{
               submission_ref: %{"id" => "sub-semantic"},
               lower_receipt: %{
                 "state" => "accepted",
                 "ji_submission_key" => "ji-sub-semantic",
                 "run_id" => "run-semantic"
               },
               trace_id: "trace-semantic-bootstrap",
               causation_id: "cause-semantic-accepted",
               actor_ref: %{kind: :dispatcher}
             })

    assert {:ok, failed_execution} =
             ExecutionRecord.record_semantic_failure(accepted_execution, %{
               lower_receipt: accepted_execution.lower_receipt,
               last_dispatch_error_payload: %{
                 "error" => %{"kind" => "semantic_failure", "reason" => "model_confused"}
               },
               trace_id: "trace-semantic-mutation",
               causation_id: "cause-semantic-failed",
               actor_ref: %{kind: :reconciler}
             })

    assert failed_execution.dispatch_state == :failed
    assert failed_execution.trace_id == "trace-semantic-bootstrap"

    assert {:ok, lineage} = ExecutionLineageStore.fetch(execution.id)
    assert lineage.trace_id == "trace-semantic-bootstrap"
    assert lineage.lower_run_id == "run-semantic"

    assert {:ok, audit_facts} = AuditFact.list_trace("inst-1", "trace-semantic-bootstrap")

    assert Enum.any?(audit_facts, &(&1.fact_kind == :execution_dispatched))
    assert Enum.any?(audit_facts, &(&1.fact_kind == :execution_failed))
  end

  defp ingest_subject(source_ref) do
    now = DateTime.utc_now() |> DateTime.truncate(:microsecond)
    subject_id = Ecto.UUID.generate()

    Repo.query!(
      """
      INSERT INTO subject_records (
        id,
        installation_id,
        source_ref,
        subject_kind,
        lifecycle_state,
        status,
        payload,
        schema_ref,
        schema_version,
        opened_at,
        status_updated_at,
        row_version,
        inserted_at,
        updated_at
      )
      VALUES ($1::uuid, $2, $3, $4, $5, 'active', $6, $7, 1, $8, $8, 1, $8, $8)
      """,
      [
        Ecto.UUID.dump!(subject_id),
        "inst-1",
        source_ref,
        "linear_coding_ticket",
        "queued",
        %{},
        "mezzanine.subject.linear_coding_ticket.payload.v1",
        now
      ]
    )

    {:ok,
     %{
       id: subject_id,
       installation_id: "inst-1",
       source_ref: source_ref,
       subject_kind: "linear_coding_ticket",
       lifecycle_state: "queued",
       status: "active"
     }}
  end

  defp retired_dispatch_jobs(execution_id) do
    Repo.all(Oban.Job)
    |> Enum.filter(fn job ->
      job.worker == "Mezzanine.ExecutionDispatchWorker" and
        job.args["execution_id"] == execution_id
    end)
  end

  defp dispatch_execution(subject, trace_id, suffix) do
    ExecutionRecord.dispatch(%{
      tenant_id: "tenant-1",
      installation_id: "inst-1",
      subject_id: subject.id,
      recipe_ref: "triage_ticket",
      compiled_pack_revision: 1,
      binding_snapshot: %{},
      dispatch_envelope: %{"capability" => "sandbox.exec"},
      intent_snapshot: %{},
      submission_dedupe_key: "inst-1:exec:#{suffix}",
      trace_id: trace_id,
      causation_id: "cause-#{suffix}",
      actor_ref: %{kind: :scheduler}
    })
  end
end
