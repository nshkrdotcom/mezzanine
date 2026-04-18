defmodule Mezzanine.Decisions.PersistenceTest do
  use Mezzanine.Decisions.DataCase, async: false

  alias Mezzanine.DecisionCommands
  alias Mezzanine.DecisionExpiryWorker
  alias Mezzanine.Execution.ExecutionRecord
  alias Mezzanine.Execution.Repo

  test "create_pending persists the decision ledger and emits audit" do
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
    assert is_integer(decision.expiry_job_id)
    assert decision_exists?(subject.id, "human_review_required")

    expiry_job = expiry_job_for!(decision.id)
    assert expiry_job.worker == Oban.Worker.to_string(DecisionExpiryWorker)
    assert expiry_job.scheduled_at == decision.required_by

    assert [audit_fact] = audit_facts_for_trace("inst-1", "trace-decision-pending")
    assert audit_fact.fact_kind == :decision_created
    assert audit_fact.decision_id == decision.id
  end

  test "decide resolves the row and exposes resolved-for-subject reads" do
    assert {:ok, subject} = ingest_subject("linear:ticket:decision-resolve")
    assert {:ok, execution} = dispatch_execution(subject, "decision-resolve")
    assert {:ok, decision} = create_pending_decision(subject, execution, "resolve")
    assert expiry_job_for!(decision.id).id == decision.expiry_job_id

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
    assert is_nil(resolved_decision.expiry_job_id)
    refute expiry_job_exists?(decision.expiry_job_id)

    assert [resolved_row] = resolved_decisions_for_subject(subject.id)
    assert resolved_row.id == resolved_decision.id
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
    assert is_nil(expired_decision.expiry_job_id)
  end

  test "expiry worker discards cleanly when a decision was resolved before the worker ran" do
    assert {:ok, subject} = ingest_subject("linear:ticket:decision-expire-race")
    assert {:ok, execution} = dispatch_execution(subject, "decision-expire-race")
    assert {:ok, decision} = create_pending_decision(subject, execution, "expire-race")

    expiry_job = expiry_job_for!(decision.id)

    assert {:ok, _resolved_decision} =
             DecisionCommands.decide(decision, %{
               decision_value: "accept",
               reason: "approved before expiry fired",
               trace_id: "trace-decision-expire-race-resolve",
               causation_id: "cause-decision-expire-race-resolve",
               actor_ref: %{kind: :reviewer}
             })

    assert :discard =
             DecisionExpiryWorker.perform(%Oban.Job{
               id: expiry_job.id,
               attempt: 1,
               queue: expiry_job.queue,
               args: expiry_job.args
             })
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

  defp expiry_job_for!(decision_id) do
    Repo.all(Oban.Job)
    |> Enum.find(fn job ->
      job.worker == Oban.Worker.to_string(DecisionExpiryWorker) and
        job.args["decision_id"] == decision_id
    end)
    |> case do
      nil -> flunk("expected a decision expiry job for decision #{decision_id}")
      job -> job
    end
  end

  defp expiry_job_exists?(job_id) do
    Repo.all(Oban.Job)
    |> Enum.any?(&(&1.id == job_id))
  end

  defp decision_exists?(subject_id, decision_kind) do
    Repo.query!(
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
    Repo.query!(
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
    Repo.query!(
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
    Repo.query!(
      """
      SELECT fact_kind, decision_id
      FROM audit_facts
      WHERE installation_id = $1
        AND trace_id = $2
      ORDER BY occurred_at ASC, inserted_at ASC
      """,
      [installation_id, trace_id]
    ).rows
    |> Enum.map(fn [fact_kind, decision_id] ->
      %{
        fact_kind: String.to_atom(fact_kind),
        decision_id: normalize_uuid(decision_id)
      }
    end)
  end

  defp normalize_uuid(<<_::128>> = uuid), do: Ecto.UUID.load!(uuid)
  defp normalize_uuid(uuid), do: uuid
end
