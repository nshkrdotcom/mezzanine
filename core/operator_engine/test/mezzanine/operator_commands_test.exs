defmodule Mezzanine.OperatorCommandsTest do
  use Mezzanine.Operator.DataCase, async: false

  alias Mezzanine.Execution.ExecutionRecord
  alias Mezzanine.Execution.Repo
  alias Mezzanine.ExecutionCancelWorker
  alias Mezzanine.ExecutionDispatchWorker
  alias Mezzanine.LeaseInvalidation
  alias Mezzanine.Leasing
  alias Mezzanine.OperatorCommands

  @reconcile_worker "Mezzanine.ExecutionReconcileWorker"

  defmodule LowerGatewayStub do
    @behaviour Mezzanine.LowerGateway

    @impl true
    def dispatch(_claim), do: {:error, {:unexpected_lower_gateway_call, :dispatch}}

    @impl true
    def lookup_submission(_submission_dedupe_key, _tenant_id), do: :never_seen

    @impl true
    def fetch_execution_outcome(_execution_lookup, _tenant_id),
      do: {:error, {:unexpected_lower_gateway_call, :fetch_execution_outcome}}

    @impl true
    def request_cancel(submission_ref, tenant_id, reason) do
      send(
        Process.get(:operator_commands_test_pid),
        {:request_cancel, [submission_ref, tenant_id, reason]}
      )

      case Process.get(:operator_commands_test_responses, %{}) do
        %{request_cancel: handler} when is_function(handler, 1) ->
          handler.([submission_ref, tenant_id, reason])

        _other ->
          {:error, {:unexpected_lower_gateway_call, :request_cancel}}
      end
    end
  end

  setup do
    original_impl = Application.get_env(:mezzanine_execution_engine, :lower_gateway_impl)

    Application.put_env(:mezzanine_execution_engine, :lower_gateway_impl, LowerGatewayStub)
    Process.put(:operator_commands_test_pid, self())
    Process.put(:operator_commands_test_responses, %{})

    on_exit(fn ->
      restore_lower_gateway_impl(original_impl)
      Process.delete(:operator_commands_test_pid)
      Process.delete(:operator_commands_test_responses)
    end)

    :ok
  end

  test "pause and resume retarget only dispatch jobs and preserve delayed reconcile work" do
    assert {:ok, subject} = ingest_subject("linear:ticket:pause")
    assert {:ok, execution} = dispatch_execution(subject, "pause")

    %{read_lease: read_lease, stream_lease: stream_lease} =
      issue_leases!(subject, execution, "pause")

    dispatch_job = dispatch_job_for!(execution.id)
    original_dispatch_schedule = dispatch_job.scheduled_at

    scheduled_at = ~U[2026-04-18 12:00:00.000000Z]

    Repo.insert!(
      Oban.Job.new(
        %{"execution_id" => execution.id},
        worker: @reconcile_worker,
        queue: "reconcile",
        scheduled_at: scheduled_at
      )
    )

    reconcile_job = reconcile_job_for!(execution.id)

    assert {:ok, pause_result} =
             OperatorCommands.pause(subject.id,
               reason: "operator hold",
               trace_id: "trace-operator-pause",
               causation_id: "cause-operator-pause",
               actor_ref: %{kind: :operator}
             )

    assert pause_result.status == "paused"

    assert Enum.sort(pause_result.details.invalidated_lease_ids) ==
             Enum.sort([read_lease.lease_id, stream_lease.lease_id])

    paused_dispatch_job = dispatch_job_for!(execution.id)
    refute paused_dispatch_job.scheduled_at == original_dispatch_schedule

    assert paused_dispatch_job.meta["pause_scheduled_at"] ==
             DateTime.to_iso8601(original_dispatch_schedule)

    untouched_reconcile_job = reconcile_job_for!(execution.id)
    assert untouched_reconcile_job.id == reconcile_job.id
    assert untouched_reconcile_job.scheduled_at == reconcile_job.scheduled_at

    assert {:ok, resume_result} =
             OperatorCommands.resume(subject.id,
               trace_id: "trace-operator-resume",
               causation_id: "cause-operator-resume",
               actor_ref: %{kind: :operator}
             )

    assert resume_result.status == "active"

    resumed_dispatch_job = dispatch_job_for!(execution.id)
    assert resumed_dispatch_job.scheduled_at == original_dispatch_schedule
    refute Map.has_key?(resumed_dispatch_job.meta, "pause_scheduled_at")

    resumed_reconcile_job = reconcile_job_for!(execution.id)
    assert resumed_reconcile_job.scheduled_at == reconcile_job.scheduled_at

    assert Enum.map(subject_invalidations("subject_paused"), & &1.lease_id) |> Enum.sort() ==
             Enum.sort([read_lease.lease_id, stream_lease.lease_id])
  end

  test "cancel marks the subject cancelled, cancels queued dispatch, and enqueues lower cancel work" do
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

    assert Enum.sort(cancel_result.details.invalidated_lease_ids) ==
             Enum.sort([read_lease.lease_id, stream_lease.lease_id])

    cancelled_dispatch_job = dispatch_job_for!(pending_execution.id)
    assert cancelled_dispatch_job.state == "cancelled"

    assert {:ok, pending_reloaded} = Ash.get(ExecutionRecord, pending_execution.id)
    assert pending_reloaded.dispatch_state == :cancelled

    assert {:ok, accepted_reloaded} = Ash.get(ExecutionRecord, accepted_execution.id)
    assert accepted_reloaded.dispatch_state == :cancelled

    cancel_job = cancel_job_for!(accepted_execution.id)
    assert cancel_job.worker == Oban.Worker.to_string(ExecutionCancelWorker)

    assert Enum.map(subject_invalidations("subject_cancelled"), & &1.lease_id) |> Enum.sort() ==
             Enum.sort([read_lease.lease_id, stream_lease.lease_id])
  end

  test "execution cancel worker emits cancel requested telemetry before lower propagation" do
    telemetry_ids = attach_telemetry([[:mezzanine, :cancel, :requested]])

    try do
      assert {:ok, subject} = ingest_subject("linear:ticket:cancel-requested")
      assert {:ok, execution} = accepted_execution(subject, "cancel-requested")

      assert {:ok, _cancel_result} =
               OperatorCommands.cancel(subject.id,
                 reason: "operator cancel",
                 trace_id: "trace-operator-cancel-requested",
                 causation_id: "cause-operator-cancel-requested",
                 actor_ref: %{kind: :operator}
               )

      Process.put(:operator_commands_test_responses, %{
        request_cancel: fn [_submission_ref, _tenant_id, _reason] ->
          {:cancelled, ~U[2026-04-18 02:00:00Z]}
        end
      })

      cancel_job = cancel_job_for!(execution.id)

      assert :ok =
               ExecutionCancelWorker.perform(%Oban.Job{
                 id: cancel_job.id,
                 attempt: 1,
                 queue: cancel_job.queue,
                 args: cancel_job.args
               })

      assert_received {:request_cancel, [%{"id" => "sub-cancel-requested"}, "tenant-1", reason]}
      assert reason["reason"] == "operator cancel"
      assert reason["execution_id"] == execution.id

      assert_receive {:telemetry_event, [:mezzanine, :cancel, :requested], %{count: 1}, metadata}

      assert metadata.event_name == "cancel.requested"
      assert metadata.trace_id == "trace-operator-cancel-requested"
      assert metadata.subject_id == subject.id
      assert metadata.execution_id == execution.id
      assert metadata.submission_dedupe_key == "inst-1:operator:cancel-requested"
      assert metadata.tenant_id == "tenant-1"
      assert metadata.installation_id == "inst-1"
      assert metadata.cancel_reason == "operator cancel"
      assert metadata.job_id == cancel_job.id
      assert metadata.job_attempt == 1
    after
      detach_telemetry(telemetry_ids)
    end
  end

  test "execution cancel worker records audit-only late receipts when lower cancel is too late" do
    telemetry_ids =
      attach_telemetry([
        [:mezzanine, :cancel, :requested],
        [:mezzanine, :cancel, :too_late]
      ])

    try do
      assert {:ok, subject} = ingest_subject("linear:ticket:cancel-too-late")
      assert {:ok, execution} = accepted_execution(subject, "cancel-too-late")

      assert {:ok, _cancel_result} =
               OperatorCommands.cancel(subject.id,
                 reason: "operator cancel",
                 trace_id: "trace-operator-cancel-too-late",
                 causation_id: "cause-operator-cancel-too-late",
                 actor_ref: %{kind: :operator}
               )

      Process.put(:operator_commands_test_responses, %{
        request_cancel: fn [_submission_ref, _tenant_id, _reason] ->
          {:too_late,
           %{
             "receipt_id" => "late-receipt-1",
             "status" => "ok",
             "normalized_outcome" => %{"result" => "already completed"}
           }}
        end
      })

      cancel_job = cancel_job_for!(execution.id)

      assert :ok =
               ExecutionCancelWorker.perform(%Oban.Job{
                 id: cancel_job.id,
                 attempt: 1,
                 queue: cancel_job.queue,
                 args: cancel_job.args
               })

      assert_received {:request_cancel, [%{"id" => "sub-cancel-too-late"}, "tenant-1", _reason]}

      assert_receive {:telemetry_event, [:mezzanine, :cancel, :requested], %{count: 1},
                      requested_metadata}

      assert requested_metadata.event_name == "cancel.requested"
      assert requested_metadata.execution_id == execution.id
      assert requested_metadata.trace_id == "trace-operator-cancel-too-late"

      assert_receive {:telemetry_event, [:mezzanine, :cancel, :too_late], %{count: 1},
                      too_late_metadata}

      assert too_late_metadata.event_name == "cancel.too_late"
      assert too_late_metadata.execution_id == execution.id
      assert too_late_metadata.subject_id == subject.id
      assert too_late_metadata.trace_id == "trace-operator-cancel-too-late"
      assert too_late_metadata.terminal_outcome_status == "ok"
      assert too_late_metadata.receipt_id == "late-receipt-1"

      audit_kinds =
        audit_kinds_for_trace("inst-1", "trace-operator-cancel-too-late")

      assert :post_cancel_receipt in audit_kinds
      assert :reconciliation_warning in audit_kinds
    after
      detach_telemetry(telemetry_ids)
    end
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

  defp dispatch_job_for!(execution_id) do
    Repo.all(Oban.Job)
    |> Enum.find(fn job ->
      job.worker == Oban.Worker.to_string(ExecutionDispatchWorker) and
        job.args["execution_id"] == execution_id
    end)
    |> case do
      nil -> flunk("expected a dispatch job for execution #{execution_id}")
      job -> job
    end
  end

  defp reconcile_job_for!(execution_id) do
    Repo.all(Oban.Job)
    |> Enum.find(fn job ->
      job.worker == @reconcile_worker and
        job.args["execution_id"] == execution_id
    end)
    |> case do
      nil -> flunk("expected a reconcile job for execution #{execution_id}")
      job -> job
    end
  end

  defp cancel_job_for!(execution_id) do
    Repo.all(Oban.Job)
    |> Enum.find(fn job ->
      job.worker == Oban.Worker.to_string(ExecutionCancelWorker) and
        job.args["execution_id"] == execution_id
    end)
    |> case do
      nil -> flunk("expected a cancel job for execution #{execution_id}")
      job -> job
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

  defp restore_lower_gateway_impl(nil),
    do: Application.delete_env(:mezzanine_execution_engine, :lower_gateway_impl)

  defp restore_lower_gateway_impl(original_impl),
    do: Application.put_env(:mezzanine_execution_engine, :lower_gateway_impl, original_impl)

  defp attach_telemetry(events) do
    Enum.map(events, fn event ->
      handler_id = {__MODULE__, make_ref(), event}
      :ok = :telemetry.attach(handler_id, event, &__MODULE__.handle_telemetry_event/4, self())
      handler_id
    end)
  end

  defp detach_telemetry(handler_ids) do
    Enum.each(handler_ids, &:telemetry.detach/1)
  end

  def handle_telemetry_event(event, measurements, metadata, pid) do
    send(pid, {:telemetry_event, event, measurements, metadata})
  end
end
