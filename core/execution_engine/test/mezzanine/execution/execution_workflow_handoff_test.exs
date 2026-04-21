defmodule Mezzanine.ExecutionWorkflowHandoffTest do
  use Mezzanine.Execution.DataCase, async: false

  alias Mezzanine.Execution.{ExecutionRecord, Repo}

  test "execution dispatch records a Temporal workflow handoff without enqueueing saga jobs" do
    assert {:ok, subject} = ingest_subject("linear:ticket:temporal-cutover-dispatch")
    assert {:ok, execution} = dispatch_execution(subject, "temporal-cutover")

    assert {:ok, handoff} = ExecutionRecord.enqueue_dispatch(execution)

    assert handoff.provider == :temporal_workflow
    assert handoff.workflow_module == "Mezzanine.Workflows.ExecutionAttempt"
    assert handoff.workflow_runtime_boundary == "Mezzanine.WorkflowRuntime"
    assert handoff.execution_id == execution.id
    assert handoff.tenant_id == "tenant-1"
    assert handoff.installation_id == "inst-1"
    assert handoff.trace_id == "trace-temporal-cutover"
    assert handoff.release_manifest_ref == "phase4-v6-milestone31-temporal-cutover"

    assert retired_dispatch_jobs(execution.id) == []
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

  defp dispatch_execution(subject, suffix) do
    ExecutionRecord.dispatch(%{
      tenant_id: "tenant-1",
      installation_id: "inst-1",
      subject_id: subject.id,
      recipe_ref: "triage_ticket",
      compiled_pack_revision: 7,
      binding_snapshot: %{"placement_ref" => "local_docker"},
      dispatch_envelope: %{"capability" => "sandbox.exec"},
      intent_snapshot: %{"recipe_ref" => "triage_ticket"},
      submission_dedupe_key: "inst-1:exec:#{suffix}",
      trace_id: "trace-temporal-cutover",
      causation_id: "cause-#{suffix}",
      actor_ref: %{kind: :scheduler}
    })
  end
end
