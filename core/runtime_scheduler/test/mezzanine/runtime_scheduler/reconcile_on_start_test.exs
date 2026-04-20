defmodule Mezzanine.RuntimeScheduler.ReconcileOnStartTest do
  use Mezzanine.RuntimeScheduler.DataCase, async: false

  alias Mezzanine.Execution.{ExecutionRecord, Repo}
  alias Mezzanine.RuntimeScheduler.ReconcileOnStart

  test "records Temporal handoff recovery for stranded dispatch rows without Oban dispatch jobs" do
    telemetry_ids = attach_telemetry([[:mezzanine, :dispatch, :ambiguous]])
    source_ref = unique_name("linear:ticket:restart-recovery")
    suffix = unique_name("restart")

    assert {:ok, subject} = ingest_subject(source_ref)
    assert {:ok, execution} = dispatch_execution(subject, "trace-runtime-recovery", suffix)

    assert {:ok, dispatching_execution} =
             ExecutionRecord.mark_dispatching(execution, %{
               trace_id: execution.trace_id,
               causation_id: "dispatch-workflow-claimed"
             })

    recovery_now = DateTime.add(DateTime.utc_now(), 5, :second)
    assert dispatching_execution.dispatch_state == :in_flight

    summary =
      try do
        assert {:ok, summary} = ReconcileOnStart.reconcile("inst-1", recovery_now)

        assert_receive {:telemetry_event, [:mezzanine, :dispatch, :ambiguous], %{count: 1},
                        metadata}

        assert metadata.event_name == "dispatch.ambiguous"
        assert metadata.execution_id == execution.id
        assert metadata.installation_id == "inst-1"
        summary
      after
        detach_telemetry(telemetry_ids)
      end

    assert summary.dispatch_recovered_count == 1
    assert summary.dispatch_recovered_execution_ids == [execution.id]
    assert summary.reconcile_handoff_count == 0
    assert summary.reconcile_handoff_execution_ids == []

    assert {:ok, recovered_execution} = Ash.get(ExecutionRecord, execution.id)
    assert recovered_execution.dispatch_state == :in_flight
    assert recovered_execution.dispatch_attempt_count == 1
    assert recovered_execution.next_dispatch_at == recovery_now
    assert recovered_execution.last_dispatch_error_kind == "restart_recovery"
    assert Repo.aggregate(Oban.Job, :count, :id) == 0
  end

  test "claims receipt reconciliation waves as Temporal workflow handoffs" do
    telemetry_ids = attach_telemetry([[:mezzanine, :startup, :reconcile, :handoff_recorded]])
    source_ref = unique_name("linear:ticket:startup-reconcile")
    suffix = unique_name("startup-reconcile")

    assert {:ok, subject} = ingest_subject(source_ref)
    assert {:ok, awaiting_receipt_execution} = awaiting_receipt_execution(subject, suffix)

    now = DateTime.add(DateTime.utc_now(), 10, :second)

    summary =
      try do
        assert {:ok, summary} = ReconcileOnStart.reconcile("inst-1", now)

        assert_receive {:telemetry_event, [:mezzanine, :startup, :reconcile, :handoff_recorded],
                        %{count: 1}, metadata}

        assert metadata.event_name == "startup.reconcile.handoff_recorded"
        assert metadata.execution_id == awaiting_receipt_execution.id
        assert metadata.installation_id == "inst-1"
        summary
      after
        detach_telemetry(telemetry_ids)
      end

    assert summary.dispatch_recovered_count == 0
    assert summary.dispatch_recovered_execution_ids == []
    assert summary.reconcile_handoff_count == 1
    assert summary.reconcile_handoff_execution_ids == [awaiting_receipt_execution.id]
    assert Repo.aggregate(Oban.Job, :count, :id) == 0
  end

  test "concurrent startup reconciliation records one handoff claim per execution" do
    telemetry_ids = attach_telemetry([[:mezzanine, :startup, :reconcile, :unique_drop]])
    source_ref = unique_name("linear:ticket:startup-reconcile-concurrent")
    suffix = unique_name("startup-race")

    assert {:ok, subject} = ingest_subject(source_ref)
    assert {:ok, awaiting_receipt_execution} = awaiting_receipt_execution(subject, suffix)

    now = DateTime.add(DateTime.utc_now(), 10, :second)

    results =
      1..3
      |> Task.async_stream(
        fn launcher ->
          ReconcileOnStart.reconcile("inst-1", now,
            actor_ref: %{kind: :runtime_scheduler, launcher: launcher}
          )
        end,
        ordered: false,
        timeout: 5_000,
        max_concurrency: 3
      )
      |> Enum.map(fn {:ok, {:ok, summary}} -> summary end)

    assert length(results) == 3
    assert Enum.map(results, & &1.reconcile_handoff_count) |> Enum.sum() == 1

    assert Enum.all?(results, fn summary ->
             summary.reconcile_handoff_execution_ids == [awaiting_receipt_execution.id]
           end)

    try do
      assert_receive {:telemetry_event, [:mezzanine, :startup, :reconcile, :unique_drop],
                      %{count: 1}, metadata}

      assert metadata.event_name == "startup.reconcile.unique_drop"
      assert metadata.execution_id == awaiting_receipt_execution.id
    after
      detach_telemetry(telemetry_ids)
    end

    assert Repo.aggregate(Oban.Job, :count, :id) == 0
  end

  defp awaiting_receipt_execution(subject, suffix) do
    with {:ok, execution} <- dispatch_execution(subject, "trace-startup-reconcile", suffix) do
      ExecutionRecord.record_accepted(execution, %{
        submission_ref: %{"id" => "sub-#{suffix}"},
        lower_receipt: %{"state" => "accepted", "run_id" => "run-#{suffix}"},
        trace_id: execution.trace_id,
        causation_id: "cause-accepted-#{suffix}",
        actor_ref: %{kind: :dispatcher}
      })
    end
  end

  defp dispatch_execution(subject, trace_id, suffix) do
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
      trace_id: trace_id,
      causation_id: "cause-#{suffix}",
      actor_ref: %{kind: :scheduler}
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

  defp unique_name(prefix), do: "#{prefix}:#{System.unique_integer([:positive])}"

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
