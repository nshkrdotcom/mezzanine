defmodule Mezzanine.Decisions.PersistenceTest do
  use Mezzanine.Decisions.DataCase, async: false

  alias Mezzanine.Audit.Repo, as: AuditRepo
  alias Mezzanine.DecisionCommands
  alias Mezzanine.Decisions.Repo, as: DecisionsRepo
  alias Mezzanine.Execution.ExecutionRecord
  alias Mezzanine.Execution.Repo, as: ExecutionRepo

  test "create_pending persists the decision ledger and emits workflow timer evidence" do
    assert {:ok, subject} = ingest_subject("linear:ticket:decision-pending")
    assert {:ok, execution} = dispatch_execution(subject, "decision-pending")

    assert {:ok, decision} =
             DecisionCommands.create_pending(%{
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
    assert decision_exists?(subject.id, "human_review_required")
    assert decision_expiry_jobs() == []

    assert [audit_fact] = audit_facts_for_trace("inst-1", "trace-decision-pending")
    assert audit_fact.fact_kind == :decision_created
    assert audit_fact.decision_id == decision.id
    assert audit_fact.payload["workflow_timer_ref"] =~ "workflow-timer://decision/"
    refute Map.has_key?(audit_fact.payload, "expiry_job_id")
  end

  test "decide resolves the row and exposes resolved-for-subject reads" do
    assert {:ok, subject} = ingest_subject("linear:ticket:decision-resolve")
    assert {:ok, execution} = dispatch_execution(subject, "decision-resolve")
    assert {:ok, decision} = create_pending_decision(subject, execution, "resolve")
    assert decision_expiry_jobs() == []

    assert {:ok, resolved_decision} =
             DecisionCommands.decide(decision, %{
               decision_value: "accept",
               reason: "approved by reviewer",
               trace_id: "trace-decision-resolve",
               causation_id: "cause-decision-resolve",
               actor_ref: %{kind: :reviewer, id: "alice"}
             })

    assert resolved_decision.lifecycle_state == "resolved"
    assert resolved_decision.decision_value == "accept"
    assert resolved_decision.reason == "approved by reviewer"
    assert decision_expiry_jobs() == []

    assert [resolved_row] = resolved_decisions_for_subject(subject.id)
    assert resolved_row.id == resolved_decision.id

    assert_terminal_attempt("trace-decision-resolve", resolved_decision, %{
      "requested_decision" => "accept",
      "outcome" => "accepted",
      "observed_lifecycle_state" => "resolved"
    })
  end

  test "terminal resolution command routes accept reject and escalate attempts" do
    assert {:ok, accept_subject} = ingest_subject("linear:ticket:decision-terminal-accept")
    assert {:ok, reject_subject} = ingest_subject("linear:ticket:decision-terminal-reject")
    assert {:ok, escalate_subject} = ingest_subject("linear:ticket:decision-terminal-escalate")

    assert {:ok, accept_execution} =
             dispatch_execution(accept_subject, "decision-terminal-accept")

    assert {:ok, reject_execution} =
             dispatch_execution(reject_subject, "decision-terminal-reject")

    assert {:ok, escalate_execution} =
             dispatch_execution(escalate_subject, "decision-terminal-escalate")

    assert {:ok, accept_decision} =
             create_pending_decision(accept_subject, accept_execution, "accept")

    assert {:ok, reject_decision} =
             create_pending_decision(reject_subject, reject_execution, "reject")

    assert {:ok, escalate_decision} =
             create_pending_decision(escalate_subject, escalate_execution, "escalate")

    assert {:ok, accepted_decision} =
             DecisionCommands.resolve_terminal(accept_decision, :accept, %{
               reason: "approved by reviewer",
               trace_id: "trace-decision-terminal-accept",
               causation_id: "cause-decision-terminal-accept",
               actor_ref: %{kind: :reviewer, id: "alice", tenant_id: "tenant-1"},
               attempt_id: "attempt-terminal-accept",
               idempotency_key: "idem-terminal-accept"
             })

    assert {:ok, rejected_decision} =
             DecisionCommands.reject(reject_decision, %{
               reason: "rejected by reviewer",
               trace_id: "trace-decision-terminal-reject",
               causation_id: "cause-decision-terminal-reject",
               actor_ref: %{kind: :reviewer, id: "bob", tenant_id: "tenant-1"},
               attempt_id: "attempt-terminal-reject",
               idempotency_key: "idem-terminal-reject"
             })

    assert {:ok, escalated_decision} =
             DecisionCommands.escalate(escalate_decision, %{
               reason: "requires escalation owner",
               trace_id: "trace-decision-terminal-escalate",
               causation_id: "cause-decision-terminal-escalate",
               actor_ref: %{kind: :reviewer, id: "carol", tenant_id: "tenant-1"},
               attempt_id: "attempt-terminal-escalate",
               idempotency_key: "idem-terminal-escalate"
             })

    assert accepted_decision.lifecycle_state == "resolved"
    assert accepted_decision.decision_value == "accept"
    assert rejected_decision.lifecycle_state == "resolved"
    assert rejected_decision.decision_value == "reject"
    assert escalated_decision.lifecycle_state == "escalated"

    assert_terminal_attempt("trace-decision-terminal-accept", accepted_decision, %{
      "attempt_id" => "attempt-terminal-accept",
      "idempotency_key" => "idem-terminal-accept",
      "requested_decision" => "accept",
      "outcome" => "accepted",
      "tenant_id" => "tenant-1"
    })

    assert_terminal_attempt("trace-decision-terminal-reject", rejected_decision, %{
      "attempt_id" => "attempt-terminal-reject",
      "idempotency_key" => "idem-terminal-reject",
      "requested_decision" => "reject",
      "outcome" => "accepted",
      "tenant_id" => "tenant-1"
    })

    assert_terminal_attempt("trace-decision-terminal-escalate", escalated_decision, %{
      "attempt_id" => "attempt-terminal-escalate",
      "idempotency_key" => "idem-terminal-escalate",
      "requested_decision" => "escalate",
      "outcome" => "accepted",
      "tenant_id" => "tenant-1"
    })
  end

  test "duplicate terminal retry requires same idempotency and same decision" do
    assert {:ok, subject} = ingest_subject("linear:ticket:decision-duplicate-retry")
    assert {:ok, execution} = dispatch_execution(subject, "decision-duplicate-retry")
    assert {:ok, decision} = create_pending_decision(subject, execution, "duplicate-retry")

    assert {:ok, resolved_decision} =
             DecisionCommands.accept(decision, %{
               reason: "approved by reviewer",
               trace_id: "trace-decision-duplicate-accepted",
               causation_id: "cause-decision-duplicate-accepted",
               actor_ref: %{kind: :reviewer, id: "alice", tenant_id: "tenant-1"},
               attempt_id: "attempt-duplicate-accepted",
               idempotency_key: "idem-duplicate-terminal"
             })

    assert {:error,
            {:decision_terminal_resolution_failed, {:decision_not_pending, "resolved"},
             :duplicate_same_decision}} =
             DecisionCommands.accept(decision, %{
               reason: "same retry envelope",
               trace_id: "trace-decision-duplicate-same",
               causation_id: "cause-decision-duplicate-same",
               actor_ref: %{kind: :reviewer, id: "alice", tenant_id: "tenant-1"},
               attempt_id: "attempt-duplicate-same",
               idempotency_key: "idem-duplicate-terminal"
             })

    assert {:error,
            {:decision_terminal_resolution_failed, {:decision_not_pending, "resolved"},
             :conflict_rejected}} =
             DecisionCommands.accept(decision, %{
               reason: "same decision but different idempotency",
               trace_id: "trace-decision-duplicate-different-idempotency",
               causation_id: "cause-decision-duplicate-different-idempotency",
               actor_ref: %{kind: :reviewer, id: "bob", tenant_id: "tenant-1"},
               attempt_id: "attempt-duplicate-different-idempotency",
               idempotency_key: "idem-different-terminal"
             })

    assert {:error,
            {:decision_terminal_resolution_failed, {:decision_not_pending, "resolved"},
             :conflict_rejected}} =
             DecisionCommands.reject(decision, %{
               reason: "opposite reviewer decision",
               trace_id: "trace-decision-duplicate-opposite",
               causation_id: "cause-decision-duplicate-opposite",
               actor_ref: %{kind: :reviewer, id: "carol", tenant_id: "tenant-1"},
               attempt_id: "attempt-duplicate-opposite",
               idempotency_key: "idem-opposite-terminal"
             })

    assert_terminal_attempt("trace-decision-duplicate-accepted", resolved_decision, %{
      "idempotency_key" => "idem-duplicate-terminal",
      "requested_decision" => "accept",
      "outcome" => "accepted"
    })

    assert_conflict_attempt("trace-decision-duplicate-same", resolved_decision, %{
      "idempotency_key" => "idem-duplicate-terminal",
      "requested_decision" => "accept",
      "outcome" => "duplicate_same_decision",
      "observed_lifecycle_state" => "resolved",
      "observed_decision_value" => "accept"
    })

    assert_conflict_attempt(
      "trace-decision-duplicate-different-idempotency",
      resolved_decision,
      %{
        "idempotency_key" => "idem-different-terminal",
        "requested_decision" => "accept",
        "outcome" => "conflict_rejected",
        "observed_lifecycle_state" => "resolved",
        "observed_decision_value" => "accept"
      }
    )

    assert_conflict_attempt("trace-decision-duplicate-opposite", resolved_decision, %{
      "idempotency_key" => "idem-opposite-terminal",
      "requested_decision" => "reject",
      "outcome" => "conflict_rejected",
      "observed_lifecycle_state" => "resolved",
      "observed_decision_value" => "accept"
    })
  end

  test "read_overdue and expire move pending decisions into explicit expiry state" do
    assert {:ok, subject} = ingest_subject("linear:ticket:decision-expire")
    assert {:ok, execution} = dispatch_execution(subject, "decision-expire")

    assert {:ok, decision} =
             DecisionCommands.create_pending(%{
               installation_id: "inst-1",
               subject_id: subject.id,
               execution_id: execution.id,
               decision_kind: "security_review_required",
               required_by: ~U[2026-04-14 00:00:00.000000Z],
               trace_id: "trace-decision-bootstrap",
               causation_id: "cause-decision-bootstrap",
               actor_ref: %{kind: :scheduler}
             })

    overdue_rows = overdue_decisions("inst-1", ~U[2026-04-15 00:00:00.000000Z])

    assert Enum.any?(overdue_rows, &(&1.id == decision.id))

    assert {:ok, expired_decision} =
             DecisionCommands.expire(decision, %{
               trace_id: "trace-decision-expire",
               causation_id: "cause-decision-expire",
               actor_ref: %{kind: :sla_monitor}
             })

    assert expired_decision.lifecycle_state == "expired"
    assert expired_decision.decision_value == "expired"
    assert decision_expiry_jobs() == []
  end

  test "workflow timer expiry is idempotent when a decision was resolved before expiry" do
    assert {:ok, subject} = ingest_subject("linear:ticket:decision-expire-race")
    assert {:ok, execution} = dispatch_execution(subject, "decision-expire-race")
    assert {:ok, decision} = create_pending_decision(subject, execution, "expire-race")

    assert {:ok, _resolved_decision} =
             DecisionCommands.decide(decision, %{
               decision_value: "accept",
               reason: "approved before expiry fired",
               trace_id: "trace-decision-expire-race-resolve",
               causation_id: "cause-decision-expire-race-resolve",
               actor_ref: %{kind: :reviewer}
             })

    assert {:error,
            {:decision_terminal_resolution_failed, {:decision_not_pending, "resolved"},
             :stale_expiry}} =
             DecisionCommands.expire(decision, %{
               trace_id: "trace-decision-expire-race-expire",
               causation_id: "cause-decision-expire-race-expire",
               actor_ref: %{kind: :workflow_timer}
             })

    assert_terminal_attempt("trace-decision-expire-race-expire", decision, %{
      "requested_decision" => "expired",
      "outcome" => "stale_expiry",
      "observed_lifecycle_state" => "resolved",
      "observed_decision_value" => "accept"
    })

    assert_conflict_attempt("trace-decision-expire-race-expire", decision, %{
      "requested_decision" => "expired",
      "outcome" => "stale_expiry",
      "observed_lifecycle_state" => "resolved",
      "observed_decision_value" => "accept",
      "conflict_attempt?" => true,
      "conflict_error_class" => "stale_expiry"
    })
  end

  test "stale expected row version writes conflict attempt evidence before returning" do
    assert {:ok, subject} = ingest_subject("linear:ticket:decision-stale-row-version")
    assert {:ok, execution} = dispatch_execution(subject, "decision-stale-row-version")
    assert {:ok, decision} = create_pending_decision(subject, execution, "stale-row-version")

    expected_row_version = decision.row_version + 1
    attempted_at = ~U[2026-04-20 01:02:03.000000Z]

    assert {:error,
            {:decision_terminal_resolution_failed,
             {:stale_row_version, ^expected_row_version, observed_row_version},
             :stale_row_version}} =
             DecisionCommands.accept(decision, %{
               reason: "losing concurrent reviewer",
               trace_id: "trace-decision-stale-row-version",
               causation_id: "cause-decision-stale-row-version",
               actor_ref: %{kind: :reviewer, id: "stale", tenant_id: "tenant-1"},
               expected_row_version: expected_row_version,
               attempt_id: "attempt-stale-row-version",
               idempotency_key: "idem-stale-row-version",
               attempted_at: attempted_at
             })

    assert observed_row_version == decision.row_version

    assert_terminal_attempt("trace-decision-stale-row-version", decision, %{
      "attempt_id" => "attempt-stale-row-version",
      "idempotency_key" => "idem-stale-row-version",
      "requested_decision" => "accept",
      "outcome" => "stale_row_version",
      "expected_row_version" => expected_row_version,
      "observed_row_version" => decision.row_version
    })

    assert_conflict_attempt("trace-decision-stale-row-version", decision, %{
      "attempt_id" => "attempt-stale-row-version",
      "idempotency_key" => "idem-stale-row-version",
      "requested_decision" => "accept",
      "outcome" => "stale_row_version",
      "expected_row_version" => expected_row_version,
      "observed_row_version" => decision.row_version,
      "conflict_attempt?" => true,
      "terminal_attempt_fact_kind" => "decision_terminal_resolution_attempt",
      "conflict_error_class" => "stale_row_version"
    })

    assert_conflict_attempt_fields("trace-decision-stale-row-version", decision, %{
      "attempt_id" => "attempt-stale-row-version",
      "decision_id" => decision.id,
      "tenant_id" => "tenant-1",
      "installation_id" => "inst-1",
      "subject_id" => subject.id,
      "execution_id" => execution.id,
      "decision_kind" => "human_review_required",
      "actor_ref" => %{"kind" => "reviewer", "id" => "stale", "tenant_id" => "tenant-1"},
      "requested_decision" => "accept",
      "reason" => "losing concurrent reviewer",
      "trace_id" => "trace-decision-stale-row-version",
      "causation_id" => "cause-decision-stale-row-version",
      "idempotency_key" => "idem-stale-row-version",
      "observed_lifecycle_state" => "pending",
      "observed_decision_value" => nil,
      "expected_row_version" => expected_row_version,
      "observed_row_version" => decision.row_version,
      "winner_decision_id" => decision.id,
      "outcome" => "stale_row_version",
      "attempted_at" => DateTime.to_iso8601(attempted_at)
    })
  end

  defp ingest_subject(source_ref) do
    now = DateTime.utc_now() |> DateTime.truncate(:microsecond)
    subject_id = Ecto.UUID.generate()

    ExecutionRepo.query!(
      """
      INSERT INTO subject_records (
        id,
        installation_id,
        source_ref,
        subject_kind,
        lifecycle_state,
        status,
        payload,
        schema_version,
        opened_at,
        status_updated_at,
        row_version,
        inserted_at,
        updated_at
      )
      VALUES ($1::uuid, $2, $3, $4, $5, 'active', $6, 1, $7, $7, 1, $7, $7)
      """,
      [
        Ecto.UUID.dump!(subject_id),
        "inst-1",
        source_ref,
        "linear_coding_ticket",
        "queued",
        %{},
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

  defp dispatch_execution(subject, suffix) do
    ExecutionRecord.dispatch(%{
      tenant_id: "tenant-1",
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
    DecisionCommands.create_pending(%{
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

  defp decision_expiry_jobs do
    ExecutionRepo.all(Oban.Job)
    |> Enum.filter(&(&1.worker == "Elixir.Mezzanine.DecisionExpiryWorker"))
  end

  defp decision_exists?(subject_id, decision_kind) do
    DecisionsRepo.query!(
      """
      SELECT 1
      FROM decision_records
      WHERE subject_id = $1::uuid
        AND decision_kind = $2
      LIMIT 1
      """,
      [Ecto.UUID.dump!(subject_id), decision_kind]
    ).num_rows == 1
  end

  defp resolved_decisions_for_subject(subject_id) do
    DecisionsRepo.query!(
      """
      SELECT id, lifecycle_state
      FROM decision_records
      WHERE subject_id = $1::uuid
        AND lifecycle_state IN ('resolved', 'waived', 'expired')
      ORDER BY inserted_at ASC
      """,
      [Ecto.UUID.dump!(subject_id)]
    ).rows
    |> Enum.map(fn [id, lifecycle_state] ->
      %{id: normalize_uuid(id), lifecycle_state: lifecycle_state}
    end)
  end

  defp overdue_decisions(installation_id, now) do
    DecisionsRepo.query!(
      """
      SELECT id
      FROM decision_records
      WHERE installation_id = $1
        AND lifecycle_state = 'pending'
        AND required_by IS NOT NULL
        AND required_by <= $2
      ORDER BY required_by ASC, inserted_at ASC
      """,
      [installation_id, now]
    ).rows
    |> Enum.map(fn [id] -> %{id: normalize_uuid(id)} end)
  end

  defp audit_facts_for_trace(installation_id, trace_id) do
    AuditRepo.query!(
      """
      SELECT fact_kind, decision_id, payload
      FROM audit_facts
      WHERE installation_id = $1
        AND trace_id = $2
      ORDER BY occurred_at ASC, inserted_at ASC
      """,
      [installation_id, trace_id]
    ).rows
    |> Enum.map(fn [fact_kind, decision_id, payload] ->
      %{
        fact_kind: String.to_atom(fact_kind),
        decision_id: normalize_uuid(decision_id),
        payload: payload
      }
    end)
  end

  defp assert_terminal_attempt(trace_id, decision, expected_payload) do
    attempt_fact =
      "inst-1"
      |> audit_facts_for_trace(trace_id)
      |> Enum.find(&(&1.fact_kind == :decision_terminal_resolution_attempt))

    assert attempt_fact
    assert attempt_fact.decision_id == decision.id

    for {key, value} <- expected_payload do
      assert attempt_fact.payload[key] == value
    end
  end

  defp assert_conflict_attempt(trace_id, decision, expected_payload) do
    attempt_fact =
      "inst-1"
      |> audit_facts_for_trace(trace_id)
      |> Enum.find(&(&1.fact_kind == :decision_conflict_attempt))

    assert attempt_fact
    assert attempt_fact.decision_id == decision.id

    for {key, value} <- expected_payload do
      assert attempt_fact.payload[key] == value
    end
  end

  defp assert_conflict_attempt_fields(trace_id, decision, expected_payload) do
    attempt_fact =
      "inst-1"
      |> audit_facts_for_trace(trace_id)
      |> Enum.find(&(&1.fact_kind == :decision_conflict_attempt))

    assert attempt_fact
    assert attempt_fact.decision_id == decision.id

    for {key, value} <- expected_payload do
      assert Map.has_key?(attempt_fact.payload, key)
      assert attempt_fact.payload[key] == value
    end
  end

  defp normalize_uuid(<<_::128>> = uuid), do: Ecto.UUID.load!(uuid)
  defp normalize_uuid(uuid), do: uuid
end
