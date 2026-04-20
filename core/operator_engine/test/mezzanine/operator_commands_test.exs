defmodule Mezzanine.OperatorCommandsTest do
  use Mezzanine.Operator.DataCase, async: false

  alias Mezzanine.Execution.ExecutionRecord
  alias Mezzanine.Execution.Repo
  alias Mezzanine.ExecutionCancelWorker
  alias Mezzanine.LeaseInvalidation
  alias Mezzanine.Leasing
  alias Mezzanine.OperatorCommands

  test "pause and resume record workflow signal refs without mutating Oban saga jobs" do
    assert {:ok, subject} = ingest_subject("linear:ticket:pause")
    assert {:ok, pending_execution} = dispatch_execution(subject, "pause-pending")
    assert {:ok, accepted_execution} = accepted_execution(subject, "pause-accepted")

    %{read_lease: read_lease, stream_lease: stream_lease} =
      issue_leases!(subject, accepted_execution, "pause")

    assert {:ok, pause_result} =
             OperatorCommands.pause(subject.id,
               reason: "operator hold",
               trace_id: "trace-operator-pause",
               causation_id: "cause-operator-pause",
               actor_ref: %{kind: :operator}
             )

    assert pause_result.status == "paused"

    assert pause_result.details.workflow_signal_refs == [
             "workflow-signal://operator.pause/#{accepted_execution.id}"
           ]

    assert Enum.sort(pause_result.details.invalidated_lease_ids) ==
             Enum.sort([read_lease.lease_id, stream_lease.lease_id])

    assert {:ok, pending_reloaded} = Ash.get(ExecutionRecord, pending_execution.id)
    assert pending_reloaded.dispatch_state == :queued

    assert Repo.aggregate(Oban.Job, :count, :id) == 0

    assert {:ok, resume_result} =
             OperatorCommands.resume(subject.id,
               trace_id: "trace-operator-resume",
               causation_id: "cause-operator-resume",
               actor_ref: %{kind: :operator}
             )

    assert resume_result.status == "active"

    assert resume_result.details.workflow_signal_refs == [
             "workflow-signal://operator.resume/#{accepted_execution.id}"
           ]

    assert Repo.aggregate(Oban.Job, :count, :id) == 0

    assert Enum.map(subject_invalidations("subject_paused"), & &1.lease_id) |> Enum.sort() ==
             Enum.sort([read_lease.lease_id, stream_lease.lease_id])
  end

  test "cancel marks active executions cancelled and records workflow signal refs only" do
    assert {:ok, subject} = ingest_subject("linear:ticket:cancel")
    assert {:ok, pending_execution} = dispatch_execution(subject, "cancel-pending")
    assert {:ok, accepted_execution} = accepted_execution(subject, "cancel-accepted")

    %{read_lease: read_lease, stream_lease: stream_lease} =
      issue_leases!(subject, accepted_execution, "cancel")

    assert {:ok, cancel_result} =
             OperatorCommands.cancel(subject.id,
               reason: "operator cancel",
               trace_id: "trace-operator-cancel",
               causation_id: "cause-operator-cancel",
               actor_ref: %{kind: :operator}
             )

    assert cancel_result.status == "cancelled"

    assert cancel_result.details.workflow_signal_refs == [
             "workflow-signal://operator.cancel/#{accepted_execution.id}"
           ]

    assert Enum.sort(cancel_result.details.cancelled_execution_ids) ==
             Enum.sort([pending_execution.id, accepted_execution.id])

    assert Enum.sort(cancel_result.details.invalidated_lease_ids) ==
             Enum.sort([read_lease.lease_id, stream_lease.lease_id])

    assert {:ok, pending_reloaded} = Ash.get(ExecutionRecord, pending_execution.id)
    assert pending_reloaded.dispatch_state == :cancelled

    assert {:ok, accepted_reloaded} = Ash.get(ExecutionRecord, accepted_execution.id)
    assert accepted_reloaded.dispatch_state == :cancelled

    assert :execution_cancelled in audit_kinds_for_trace("inst-1", "trace-operator-cancel")
    assert :subject_cancelled in audit_kinds_for_trace("inst-1", "trace-operator-cancel")
    assert Repo.aggregate(Oban.Job, :count, :id) == 0
  end

  test "cancel worker is a retired M31 tombstone, not an Oban worker" do
    assert ExecutionCancelWorker.retired?()
    refute function_exported?(ExecutionCancelWorker, :perform, 1)

    assert %{
             signal: "operator.cancel@operator-cancel.v1",
             contract: "Mezzanine.OperatorWorkflowSignal.v1",
             boundary: "Mezzanine.WorkflowRuntime.signal_workflow/1"
           } = ExecutionCancelWorker.replacement()
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
      compiled_pack_revision: 1,
      binding_snapshot: %{},
      dispatch_envelope: %{"capability" => "sandbox.exec"},
      intent_snapshot: %{},
      submission_dedupe_key: "inst-1:operator:#{suffix}",
      trace_id: "trace-execution-#{suffix}",
      causation_id: "cause-execution-#{suffix}",
      actor_ref: %{kind: :scheduler}
    })
  end

  defp accepted_execution(subject, suffix) do
    with {:ok, execution} <- dispatch_execution(subject, suffix) do
      ExecutionRecord.record_accepted(execution, %{
        submission_ref: %{"id" => "sub-#{suffix}"},
        lower_receipt: %{"state" => "accepted", "run_id" => "run-#{suffix}"},
        trace_id: "trace-accepted-#{suffix}",
        causation_id: "cause-accepted-#{suffix}",
        actor_ref: %{kind: :dispatcher}
      })
    end
  end

  defp audit_kinds_for_trace(installation_id, trace_id) do
    Repo.query!(
      """
      SELECT fact_kind
      FROM audit_facts
      WHERE installation_id = $1
        AND trace_id = $2
      ORDER BY occurred_at ASC, inserted_at ASC
      """,
      [installation_id, trace_id]
    ).rows
    |> Enum.map(fn [fact_kind] -> String.to_atom(fact_kind) end)
  end

  defp issue_leases!(subject, execution, suffix) do
    {:ok, read_lease} =
      Leasing.issue_read_lease(
        %{
          trace_id: "trace-lease-#{suffix}",
          tenant_id: "tenant-1",
          installation_id: "inst-1",
          installation_revision: 1,
          activation_epoch: 1,
          lease_epoch: 1,
          subject_id: subject.id,
          execution_id: execution.id,
          lineage_anchor: %{"submission_ref" => "sub-#{suffix}"},
          allowed_family: "unified_trace",
          allowed_operations: [:fetch_run],
          scope: %{}
        },
        repo: Repo
      )

    {:ok, stream_lease} =
      Leasing.issue_stream_attach_lease(
        %{
          trace_id: "trace-lease-#{suffix}",
          tenant_id: "tenant-1",
          installation_id: "inst-1",
          installation_revision: 1,
          activation_epoch: 1,
          lease_epoch: 1,
          subject_id: subject.id,
          execution_id: execution.id,
          lineage_anchor: %{"submission_ref" => "sub-#{suffix}"},
          allowed_family: "runtime_stream",
          scope: %{}
        },
        repo: Repo
      )

    %{read_lease: read_lease, stream_lease: stream_lease}
  end

  defp subject_invalidations(reason) do
    Repo.all(LeaseInvalidation)
    |> Enum.filter(&(&1.reason == reason))
    |> Enum.sort_by(& &1.sequence_number)
  end
end
