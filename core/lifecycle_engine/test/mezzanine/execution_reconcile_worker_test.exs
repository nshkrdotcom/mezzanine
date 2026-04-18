defmodule Mezzanine.ExecutionReconcileWorkerTest do
  use Mezzanine.LifecycleEngine.DataCase, async: false

  alias Mezzanine.Execution.{ExecutionRecord, Repo}
  alias Mezzanine.ExecutionReceiptWorker
  alias Mezzanine.ExecutionReconcileWorker
  alias Mezzanine.Objects.SubjectRecord

  defmodule LowerGatewayStub do
    @behaviour Mezzanine.LowerGateway

    @impl true
    def dispatch(_claim), do: {:error, {:unexpected_lower_gateway_call, :dispatch}}

    @impl true
    def lookup_submission(_submission_dedupe_key, _tenant_id),
      do: {:error, {:unexpected_lower_gateway_call, :lookup_submission}}

    @impl true
    def fetch_execution_outcome(execution_lookup, tenant_id) do
      send(
        Process.get(:execution_reconcile_worker_test_pid),
        {:fetch_execution_outcome, [execution_lookup, tenant_id]}
      )

      case Process.get(:execution_reconcile_worker_test_responses, %{}) do
        %{fetch_execution_outcome: handler} when is_function(handler, 1) ->
          handler.([execution_lookup, tenant_id])

        _other ->
          :pending
      end
    end

    @impl true
    def request_cancel(_submission_ref, _tenant_id, _reason),
      do: {:error, {:unexpected_lower_gateway_call, :request_cancel}}
  end

  setup do
    original_impl = Application.get_env(:mezzanine_execution_engine, :lower_gateway_impl)

    Application.put_env(:mezzanine_execution_engine, :lower_gateway_impl, LowerGatewayStub)
    Process.put(:execution_reconcile_worker_test_pid, self())
    Process.put(:execution_reconcile_worker_test_responses, %{})

    on_exit(fn ->
      restore_lower_gateway_impl(original_impl)
      Process.delete(:execution_reconcile_worker_test_pid)
      Process.delete(:execution_reconcile_worker_test_responses)
    end)

    :ok
  end

  test "perform snoozes while the lower outcome remains pending" do
    telemetry_ids = attach_telemetry([[:mezzanine, :dispatch, :reconcile, :lookup]])
    assert {:ok, subject} = ingest_subject("linear:ticket:reconcile-pending")

    assert {:ok, execution} =
             awaiting_receipt_execution(subject, "trace-reconcile-pending", "reconcile-pending")

    try do
      assert {:snooze, 30} = perform_reconcile(execution.id)

      assert_receive {:telemetry_event, [:mezzanine, :dispatch, :reconcile, :lookup],
                      measurements, metadata}

      assert measurements.count == 1
      assert is_integer(measurements.latency_ms)
      assert metadata.event_name == "dispatch.reconcile.lookup"
      assert metadata.execution_id == execution.id
      assert metadata.outcome_status == "pending"
    after
      detach_telemetry(telemetry_ids)
    end

    assert_received {:fetch_execution_outcome, [_lookup, "tenant-1"]}
    assert [] == receipt_jobs_for(execution.id)
  end

  test "perform enqueues durable receipt work when lower outcome becomes terminal" do
    telemetry_ids = attach_telemetry([[:mezzanine, :dispatch, :reconcile, :lookup]])
    assert {:ok, subject} = ingest_subject("linear:ticket:reconcile-terminal")

    assert {:ok, execution} =
             awaiting_receipt_execution(subject, "trace-reconcile-terminal", "reconcile-terminal")

    Process.put(:execution_reconcile_worker_test_responses, %{
      fetch_execution_outcome: fn [_lookup, _tenant_id] ->
        {:ok,
         %{
           receipt_id: "receipt-terminal",
           status: "ok",
           lower_receipt: %{"state" => "completed", "run_id" => "run-terminal"},
           normalized_outcome: %{"summary" => "done"},
           artifact_refs: [],
           observed_at: "2026-04-16T04:05:00Z"
         }}
      end
    })

    try do
      assert :ok = perform_reconcile(execution.id)

      assert_receive {:telemetry_event, [:mezzanine, :dispatch, :reconcile, :lookup],
                      measurements, metadata}

      assert measurements.count == 1
      assert is_integer(measurements.latency_ms)
      assert metadata.execution_id == execution.id
      assert metadata.outcome_status == "ok"
    after
      detach_telemetry(telemetry_ids)
    end

    [job] = receipt_jobs_for(execution.id)
    assert job.worker == Oban.Worker.to_string(ExecutionReceiptWorker)
    assert job.args["receipt_id"] == "receipt-terminal"
  end

  defp perform_reconcile(execution_id) do
    ExecutionReconcileWorker.perform(%Oban.Job{
      id: 43,
      attempt: 1,
      queue: "reconcile",
      args: %{"execution_id" => execution_id}
    })
  end

  defp receipt_jobs_for(execution_id) do
    Repo.all(Oban.Job)
    |> Enum.filter(fn job ->
      job.worker == Oban.Worker.to_string(Mezzanine.ExecutionReceiptWorker) and
        job.args["execution_id"] == execution_id
    end)
  end

  defp restore_lower_gateway_impl(nil),
    do: Application.delete_env(:mezzanine_execution_engine, :lower_gateway_impl)

  defp restore_lower_gateway_impl(value),
    do: Application.put_env(:mezzanine_execution_engine, :lower_gateway_impl, value)

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

  defp awaiting_receipt_execution(subject, trace_id, suffix) do
    with {:ok, execution} <-
           ExecutionRecord.dispatch(%{
             tenant_id: "tenant-1",
             installation_id: "inst-1",
             subject_id: subject.id,
             recipe_ref: "triage_ticket",
             compiled_pack_revision: 7,
             binding_snapshot: %{},
             dispatch_envelope: %{"capability" => "sandbox.exec"},
             submission_dedupe_key: "inst-1:exec:#{suffix}",
             trace_id: trace_id,
             causation_id: "cause-#{suffix}",
             actor_ref: %{kind: :scheduler}
           }) do
      ExecutionRecord.record_accepted(execution, %{
        submission_ref: %{"id" => "sub-#{suffix}"},
        lower_receipt: %{"state" => "accepted", "run_id" => "run-#{suffix}"},
        trace_id: execution.trace_id,
        causation_id: "cause-accepted-#{suffix}",
        actor_ref: %{kind: :dispatcher}
      })
    end
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
end
