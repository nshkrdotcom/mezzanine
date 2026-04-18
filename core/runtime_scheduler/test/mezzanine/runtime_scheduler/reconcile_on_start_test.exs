defmodule Mezzanine.RuntimeScheduler.ReconcileOnStartTest do
  use Mezzanine.RuntimeScheduler.DataCase, async: false

  alias Mezzanine.Execution.{ExecutionRecord, Repo}
  alias Mezzanine.ExecutionDispatchWorker
  alias Mezzanine.ExecutionReconcileWorker
  alias Mezzanine.RuntimeScheduler.ReconcileOnStart

  @dispatch_snapshot %{
    "placement_ref" => "local_docker",
    "execution_params" => %{"timeout_ms" => 600_000},
    "connector_bindings" => %{"github_write" => %{"connector_key" => "github_app"}}
  }

  defmodule LowerGatewayStub do
    @behaviour Mezzanine.LowerGateway

    @impl true
    def dispatch(claim) do
      reply(:dispatch, [claim], {:error, {:unexpected_lower_gateway_call, :dispatch}})
    end

    @impl true
    def lookup_submission(submission_dedupe_key, tenant_id) do
      reply(:lookup_submission, [submission_dedupe_key, tenant_id], :never_seen)
    end

    @impl true
    def fetch_execution_outcome(execution_lookup, tenant_id) do
      reply(
        :fetch_execution_outcome,
        [execution_lookup, tenant_id],
        {:error, {:unexpected_lower_gateway_call, :fetch_execution_outcome}}
      )
    end

    @impl true
    def request_cancel(submission_ref, tenant_id, reason) do
      reply(
        :request_cancel,
        [submission_ref, tenant_id, reason],
        {:error, {:unexpected_lower_gateway_call, :request_cancel}}
      )
    end

    defp reply(operation, args, fallback) do
      send(Process.get(:runtime_scheduler_test_pid), {operation, args})

      case Process.get(:runtime_scheduler_test_responses, %{}) do
        %{^operation => handler} when is_function(handler, 1) -> handler.(args)
        _other -> fallback
      end
    end
  end

  setup do
    original_impl = Application.get_env(:mezzanine_execution_engine, :lower_gateway_impl)

    Application.put_env(:mezzanine_execution_engine, :lower_gateway_impl, LowerGatewayStub)
    Process.put(:runtime_scheduler_test_pid, self())
    Process.put(:runtime_scheduler_test_responses, %{})

    on_exit(fn ->
      restore_lower_gateway_impl(original_impl)
      Process.delete(:runtime_scheduler_test_pid)
      Process.delete(:runtime_scheduler_test_responses)
    end)

    :ok
  end

  test "requeues stranded dispatch rows and resumes by lower lookup without blind redispatch" do
    telemetry_ids = attach_telemetry([[:mezzanine, :dispatch, :ambiguous]])
    source_ref = unique_name("linear:ticket:restart-recovery")
    suffix = unique_name("restart")

    assert {:ok, subject} = ingest_subject(source_ref)
    assert {:ok, execution} = dispatch_execution(subject, "trace-runtime-recovery", suffix)

    assert {:ok, dispatching_execution} =
             ExecutionRecord.mark_dispatching(execution, %{
               trace_id: execution.trace_id,
               causation_id: "dispatch-worker-claimed"
             })

    delete_dispatch_jobs!(execution.id)

    recovery_now = DateTime.add(DateTime.utc_now(), 5, :second)

    Process.put(:runtime_scheduler_test_responses, %{
      lookup_submission: fn [_submission_dedupe_key, _tenant_id] ->
        {:accepted,
         %{
           submission_ref: %{"id" => "sub-recovered", "status" => "duplicate"},
           lower_receipt: %{
             "state" => "accepted",
             "ji_submission_key" => "ji-sub-recovered",
             "run_id" => "run-recovered"
           }
         }}
      end
    })

    assert dispatching_execution.dispatch_state == :dispatching

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
    assert summary.reconcile_enqueued_count == 0
    assert summary.reconcile_execution_ids == []

    assert {:ok, recovered_execution} = Ash.get(ExecutionRecord, execution.id)
    assert recovered_execution.dispatch_state == :dispatching_retry
    assert recovered_execution.dispatch_attempt_count == 1
    assert recovered_execution.next_dispatch_at == recovery_now
    assert recovered_execution.last_dispatch_error_kind == "restart_recovery"

    job = dispatch_job_for!(execution.id)

    assert :ok =
             ExecutionDispatchWorker.perform(%Oban.Job{
               id: job.id,
               attempt: 1,
               queue: job.queue,
               args: job.args
             })

    submission_dedupe_key = execution.submission_dedupe_key
    assert_received {:lookup_submission, [^submission_dedupe_key, "tenant-1"]}
    refute_received {:dispatch, _args}

    assert {:ok, awaiting_receipt_execution} = Ash.get(ExecutionRecord, execution.id)
    assert awaiting_receipt_execution.dispatch_state == :awaiting_receipt
    assert awaiting_receipt_execution.dispatch_attempt_count == 1

    assert awaiting_receipt_execution.submission_ref == %{
             "id" => "sub-recovered",
             "status" => "duplicate"
           }
  end

  test "enqueues reconciliation for executions already awaiting receipts at startup" do
    telemetry_ids = attach_telemetry([[:mezzanine, :startup, :reconcile, :enqueued]])
    source_ref = unique_name("linear:ticket:startup-reconcile")
    suffix = unique_name("startup-reconcile")

    assert {:ok, subject} = ingest_subject(source_ref)

    assert {:ok, execution} =
             dispatch_execution(subject, "trace-startup-reconcile", suffix)

    assert {:ok, awaiting_receipt_execution} =
             ExecutionRecord.record_accepted(execution, %{
               submission_ref: %{"id" => "sub-awaiting"},
               lower_receipt: %{"state" => "accepted", "run_id" => "run-awaiting"},
               trace_id: execution.trace_id,
               causation_id: "cause-awaiting",
               actor_ref: %{kind: :dispatcher}
             })

    now = DateTime.add(DateTime.utc_now(), 10, :second)

    summary =
      try do
        assert {:ok, summary} = ReconcileOnStart.reconcile("inst-1", now)

        assert_receive {:telemetry_event, [:mezzanine, :startup, :reconcile, :enqueued],
                        %{count: 1}, metadata}

        assert metadata.event_name == "startup.reconcile.enqueued"
        assert metadata.execution_id == awaiting_receipt_execution.id
        assert metadata.installation_id == "inst-1"
        summary
      after
        detach_telemetry(telemetry_ids)
      end

    assert summary.dispatch_recovered_count == 0
    assert summary.dispatch_recovered_execution_ids == []
    assert summary.reconcile_enqueued_count == 1
    assert summary.reconcile_execution_ids == [awaiting_receipt_execution.id]

    [job] = reconcile_jobs_for(awaiting_receipt_execution.id)
    assert job.worker == Oban.Worker.to_string(ExecutionReconcileWorker)
    assert job.args["execution_id"] == awaiting_receipt_execution.id
  end

  test "concurrent startup reconciliation reuses one durable reconcile job per execution" do
    telemetry_ids = attach_telemetry([[:mezzanine, :startup, :reconcile, :unique_drop]])
    source_ref = unique_name("linear:ticket:startup-reconcile-concurrent")
    suffix = unique_name("startup-race")

    assert {:ok, subject} = ingest_subject(source_ref)

    assert {:ok, awaiting_receipt_execution} =
             dispatch_execution(subject, "trace-startup-reconcile-concurrent", suffix)
             |> then(fn {:ok, execution} ->
               ExecutionRecord.record_accepted(execution, %{
                 submission_ref: %{"id" => "sub-startup-race"},
                 lower_receipt: %{"state" => "accepted", "run_id" => "run-startup-race"},
                 trace_id: execution.trace_id,
                 causation_id: "cause-startup-race",
                 actor_ref: %{kind: :dispatcher}
               })
             end)

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

    assert Enum.all?(results, fn summary ->
             summary.reconcile_execution_ids == [awaiting_receipt_execution.id]
           end)

    try do
      assert [_job] = reconcile_jobs_for(awaiting_receipt_execution.id)

      assert_receive {:telemetry_event, [:mezzanine, :startup, :reconcile, :unique_drop],
                      %{count: 1}, metadata}

      assert metadata.event_name == "startup.reconcile.unique_drop"
    after
      detach_telemetry(telemetry_ids)
    end
  end

  defp dispatch_job_for!(execution_id) do
    Repo.all(Oban.Job)
    |> Enum.find(fn job ->
      job.worker == Oban.Worker.to_string(Mezzanine.ExecutionDispatchWorker) and
        job.args["execution_id"] == execution_id
    end)
    |> case do
      nil -> flunk("expected a dispatch job for execution #{execution_id}")
      job -> job
    end
  end

  defp delete_dispatch_jobs!(execution_id) do
    Repo.all(Oban.Job)
    |> Enum.filter(fn job ->
      job.worker == Oban.Worker.to_string(Mezzanine.ExecutionDispatchWorker) and
        job.args["execution_id"] == execution_id
    end)
    |> Enum.each(&Repo.delete!/1)
  end

  defp reconcile_jobs_for(execution_id) do
    Repo.all(Oban.Job)
    |> Enum.filter(fn job ->
      job.worker == Oban.Worker.to_string(Mezzanine.ExecutionReconcileWorker) and
        job.args["execution_id"] == execution_id
    end)
  end

  defp restore_lower_gateway_impl(nil),
    do: Application.delete_env(:mezzanine_execution_engine, :lower_gateway_impl)

  defp restore_lower_gateway_impl(value),
    do: Application.put_env(:mezzanine_execution_engine, :lower_gateway_impl, value)

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
       status: "active",
       payload: %{}
     }}
  end

  defp dispatch_execution(subject, trace_id, suffix) do
    ExecutionRecord.dispatch(%{
      tenant_id: "tenant-1",
      installation_id: "inst-1",
      subject_id: subject.id,
      recipe_ref: "triage_ticket",
      compiled_pack_revision: 7,
      binding_snapshot: @dispatch_snapshot,
      dispatch_envelope: %{"capability" => "sandbox.exec"},
      submission_dedupe_key: "inst-1:exec:#{suffix}",
      trace_id: trace_id,
      causation_id: "cause-#{suffix}",
      actor_ref: %{kind: :scheduler}
    })
  end

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

  defp unique_name(base) do
    "#{base}-#{System.unique_integer([:positive])}"
  end
end
