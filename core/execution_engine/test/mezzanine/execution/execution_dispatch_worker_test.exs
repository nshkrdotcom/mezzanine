defmodule Mezzanine.ExecutionDispatchWorkerTest do
  use Mezzanine.Execution.DataCase, async: false

  alias Mezzanine.Audit.ExecutionLineageStore
  alias Mezzanine.Execution.ExecutionRecord
  alias Mezzanine.Execution.Repo
  alias Mezzanine.ExecutionDispatchWorker
  alias Mezzanine.LeaseInvalidation
  alias Mezzanine.Leasing

  @dispatch_snapshot %{
    "placement_ref" => "local_docker",
    "execution_params" => %{"timeout_ms" => 600_000},
    "connector_bindings" => %{"github_write" => %{"connector_key" => "github_app"}},
    "evidence_bindings" => %{"terminal_log" => %{"collector_key" => "artifact_collector"}},
    "actor_bindings" => %{"requester" => %{"resolver_key" => "static_actor"}}
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
      send(Process.get(:execution_dispatch_worker_test_pid), {operation, args})

      case Process.get(:execution_dispatch_worker_test_responses, %{}) do
        %{^operation => handler} when is_function(handler, 1) -> handler.(args)
        _other -> fallback
      end
    end
  end

  setup do
    original_impl = Application.get_env(:mezzanine_execution_engine, :lower_gateway_impl)

    Application.put_env(:mezzanine_execution_engine, :lower_gateway_impl, LowerGatewayStub)
    Process.put(:execution_dispatch_worker_test_pid, self())
    Process.put(:execution_dispatch_worker_test_responses, %{})

    on_exit(fn ->
      restore_lower_gateway_impl(original_impl)
      Process.delete(:execution_dispatch_worker_test_pid)
      Process.delete(:execution_dispatch_worker_test_responses)
    end)

    :ok
  end

  test "perform accepts duplicate-safe submissions and preserves the frozen dispatch snapshot" do
    telemetry_ids =
      attach_telemetry([
        [:mezzanine, :dispatch, :accepted],
        [:mezzanine, :dispatch, :awaiting_receipt]
      ])

    assert {:ok, subject} = ingest_subject("linear:ticket:dispatcher-accepted")
    assert {:ok, execution} = dispatch_execution(subject, "trace-dispatcher-accepted", "accepted")

    Process.put(:execution_dispatch_worker_test_responses, %{
      dispatch: fn [claim] ->
        assert claim.execution_id == execution.id
        assert claim.tenant_id == "tenant-1"
        assert claim.installation_id == "inst-1"
        assert claim.submission_dedupe_key == "inst-1:exec:accepted"
        assert claim.compiled_pack_revision == 7
        assert claim.binding_snapshot == @dispatch_snapshot
        assert claim.dispatch_envelope == %{"capability" => "sandbox.exec"}

        {:accepted,
         %{
           "submission_ref" => %{"id" => "sub-1", "status" => "duplicate"},
           "lower_receipt" => %{
             "state" => "accepted",
             "ji_submission_key" => "ji-sub-1",
             "run_id" => "run-1"
           }
         }}
      end
    })

    try do
      assert :ok = perform_dispatch(execution.id)

      assert_receive {:telemetry_event, [:mezzanine, :dispatch, :accepted], measurements,
                      metadata}

      assert measurements == %{count: 1, latency_ms: measurements.latency_ms}
      assert is_integer(measurements.latency_ms)
      assert metadata.event_name == "dispatch.accepted"
      assert metadata.trace_id == execution.trace_id
      assert metadata.execution_id == execution.id
      assert metadata.subject_id == subject.id
      assert metadata.submission_dedupe_key == execution.submission_dedupe_key
      assert metadata.dispatch_source == :dispatch

      assert_receive {:telemetry_event, [:mezzanine, :dispatch, :awaiting_receipt], %{count: 1},
                      awaiting_receipt_metadata}

      assert awaiting_receipt_metadata.event_name == "dispatch.awaiting_receipt"
      assert awaiting_receipt_metadata.execution_id == execution.id
    after
      detach_telemetry(telemetry_ids)
    end

    assert_received {:lookup_submission, ["inst-1:exec:accepted", "tenant-1"]}
    assert_received {:dispatch, [_claim]}

    assert {:ok, accepted_execution} = Ash.get(ExecutionRecord, execution.id)
    assert accepted_execution.dispatch_state == :awaiting_receipt
    assert accepted_execution.compiled_pack_revision == 7
    assert accepted_execution.binding_snapshot == @dispatch_snapshot
    assert accepted_execution.submission_ref == %{"id" => "sub-1", "status" => "duplicate"}

    assert accepted_execution.lower_receipt == %{
             "state" => "accepted",
             "ji_submission_key" => "ji-sub-1",
             "run_id" => "run-1"
           }

    assert {:ok, lineage} = ExecutionLineageStore.fetch(execution.id)
    assert lineage.ji_submission_key == "ji-sub-1"
    assert lineage.lower_run_id == "run-1"
  end

  test "perform snoozes retryable failures without re-resolving bindings" do
    assert {:ok, subject} = ingest_subject("linear:ticket:dispatcher-retry")
    assert {:ok, execution} = dispatch_execution(subject, "trace-dispatcher-retry", "retry")

    Process.put(:execution_dispatch_worker_test_responses, %{
      dispatch: fn [_claim] ->
        {:error, {:retryable, "bridge_unavailable", %{"reason" => "timeout"}}}
      end
    })

    assert {:snooze, 30} = perform_dispatch(execution.id)

    assert {:ok, retried_execution} = Ash.get(ExecutionRecord, execution.id)
    assert retried_execution.dispatch_state == :dispatching_retry
    assert retried_execution.dispatch_attempt_count == 1
    assert retried_execution.compiled_pack_revision == 7
    assert retried_execution.binding_snapshot == @dispatch_snapshot
    assert retried_execution.last_dispatch_error_kind == "bridge_unavailable"
  end

  test "perform records terminal rejection and stops retries" do
    assert {:ok, subject} = ingest_subject("linear:ticket:dispatcher-reject")
    assert {:ok, execution} = dispatch_execution(subject, "trace-dispatcher-reject", "reject")

    %{read_lease: read_lease, stream_lease: stream_lease} =
      issue_execution_leases!(subject, execution, "reject")

    Process.put(:execution_dispatch_worker_test_responses, %{
      dispatch: fn [_claim] ->
        {:error,
         {:terminal, "unsupported_capability",
          %{"capability" => "prod_ssh_write", "state" => "rejected"}}}
      end
    })

    assert :discard = perform_dispatch(execution.id)

    assert {:ok, rejected_execution} = Ash.get(ExecutionRecord, execution.id)
    assert rejected_execution.dispatch_state == :rejected
    assert rejected_execution.terminal_rejection_reason == "unsupported_capability"

    assert Enum.map(execution_invalidations("execution_rejected"), & &1.lease_id) |> Enum.sort() ==
             Enum.sort([read_lease.lease_id, stream_lease.lease_id])
  end

  test "perform records semantic failure without reopening a bespoke outbox row" do
    assert {:ok, subject} = ingest_subject("linear:ticket:dispatcher-semantic")
    assert {:ok, execution} = dispatch_execution(subject, "trace-dispatcher-semantic", "semantic")

    Process.put(:execution_dispatch_worker_test_responses, %{
      dispatch: fn [_claim] ->
        {:semantic_failure,
         %{
           "lower_receipt" => %{"state" => "accepted", "run_id" => "run-semantic"},
           "error" => %{"kind" => "semantic_failure", "reason" => "model_confused"}
         }}
      end
    })

    assert :discard = perform_dispatch(execution.id)

    assert {:ok, failed_execution} = Ash.get(ExecutionRecord, execution.id)
    assert failed_execution.dispatch_state == :failed
    assert failed_execution.failure_kind == :semantic_failure

    assert failed_execution.last_dispatch_error_payload == %{
             "error" => %{"kind" => "semantic_failure", "reason" => "model_confused"}
           }
  end

  test "perform resumes from lower dedupe lookup before any fresh redispatch" do
    assert {:ok, subject} = ingest_subject("linear:ticket:dispatcher-lookup")
    assert {:ok, execution} = dispatch_execution(subject, "trace-dispatcher-lookup", "lookup")

    Process.put(:execution_dispatch_worker_test_responses, %{
      lookup_submission: fn [_submission_dedupe_key, _tenant_id] ->
        {:accepted,
         %{
           submission_ref: %{"id" => "sub-resumed"},
           lower_receipt: %{"state" => "accepted", "run_id" => "run-resumed"}
         }}
      end
    })

    assert :ok = perform_dispatch(execution.id)

    assert_received {:lookup_submission, ["inst-1:exec:lookup", "tenant-1"]}
    refute_received {:dispatch, _args}

    assert {:ok, accepted_execution} = Ash.get(ExecutionRecord, execution.id)
    assert accepted_execution.dispatch_state == :awaiting_receipt
    assert accepted_execution.submission_ref == %{"id" => "sub-resumed"}
    assert accepted_execution.lower_receipt == %{"state" => "accepted", "run_id" => "run-resumed"}
  end

  test "perform persists prior lower rejection returned by lookup without redispatch" do
    assert {:ok, subject} = ingest_subject("linear:ticket:dispatcher-lookup-rejected")

    assert {:ok, execution} =
             dispatch_execution(subject, "trace-dispatcher-lookup-rejected", "lookup-rejected")

    Process.put(:execution_dispatch_worker_test_responses, %{
      lookup_submission: fn [_submission_dedupe_key, _tenant_id] ->
        {:rejected,
         %{
           reason: "workspace_ref_unresolved",
           retry_class: "after_redecision",
           details: %{"logical_workspace_ref" => "workspace://tenant-1/root"}
         }}
      end
    })

    assert :discard = perform_dispatch(execution.id)

    assert_received {:lookup_submission, ["inst-1:exec:lookup-rejected", "tenant-1"]}
    refute_received {:dispatch, _args}

    assert {:ok, rejected_execution} = Ash.get(ExecutionRecord, execution.id)
    assert rejected_execution.dispatch_state == :rejected
    assert rejected_execution.terminal_rejection_reason == "workspace_ref_unresolved"

    assert rejected_execution.last_dispatch_error_payload == %{
             "retry_class" => "after_redecision",
             "details" => %{"logical_workspace_ref" => "workspace://tenant-1/root"}
           }
  end

  test "perform fails closed when the lower submission identity has expired" do
    expired_at = ~U[2026-04-16 03:00:00Z]

    assert {:ok, subject} = ingest_subject("linear:ticket:dispatcher-expired")
    assert {:ok, execution} = dispatch_execution(subject, "trace-dispatcher-expired", "expired")

    Process.put(:execution_dispatch_worker_test_responses, %{
      lookup_submission: fn [_submission_dedupe_key, _tenant_id] ->
        {:expired, expired_at}
      end
    })

    assert :discard = perform_dispatch(execution.id)
    refute_received {:dispatch, _args}

    assert {:ok, failed_execution} = Ash.get(ExecutionRecord, execution.id)
    assert failed_execution.dispatch_state == :failed
    assert failed_execution.failure_kind == :infrastructure_error
    assert failed_execution.last_dispatch_error_kind == "submission_lookup_expired"

    assert failed_execution.last_dispatch_error_payload == %{
             "reason" => "submission_lookup_expired",
             "last_seen_at" => DateTime.to_iso8601(expired_at)
           }
  end

  test "perform snoozes without lower dispatch when the subject is paused" do
    assert {:ok, subject} = ingest_subject("linear:ticket:dispatcher-paused")
    assert {:ok, execution} = dispatch_execution(subject, "trace-dispatcher-paused", "paused")

    set_subject_status!(subject.id, "paused", "operator hold")

    assert {:snooze, seconds} = perform_dispatch(execution.id)
    assert seconds >= 86_400
    refute_received {:dispatch, _args}

    assert {:ok, paused_execution} = Ash.get(ExecutionRecord, execution.id)
    assert paused_execution.dispatch_state == :pending_dispatch
  end

  test "perform fails closed when the subject was cancelled before lower dispatch" do
    assert {:ok, subject} = ingest_subject("linear:ticket:dispatcher-cancelled")

    assert {:ok, execution} =
             dispatch_execution(subject, "trace-dispatcher-cancelled", "cancelled")

    set_subject_status!(subject.id, "cancelled", "operator cancelled")

    assert :ok = perform_dispatch(execution.id)
    refute_received {:dispatch, _args}

    assert {:ok, cancelled_execution} = Ash.get(ExecutionRecord, execution.id)
    assert cancelled_execution.dispatch_state == :cancelled
    assert cancelled_execution.last_dispatch_error_kind == "operator_cancelled"
  end

  defp perform_dispatch(execution_id) do
    ExecutionDispatchWorker.perform(%Oban.Job{
      id: 41,
      attempt: 1,
      queue: "dispatch",
      args: %{"execution_id" => execution_id}
    })
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
       status: "active"
     }}
  end

  defp set_subject_status!(subject_id, status, reason) do
    now = DateTime.utc_now() |> DateTime.truncate(:microsecond)

    Repo.query!(
      """
      UPDATE subject_records
      SET status = $2,
          status_reason = $3,
          status_updated_at = $4,
          terminal_at = CASE WHEN $2 = 'cancelled' THEN $4 ELSE terminal_at END,
          updated_at = $4
      WHERE id = $1::uuid
      """,
      [Ecto.UUID.dump!(subject_id), status, reason, now]
    )
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

  defp issue_execution_leases!(subject, execution, suffix) do
    {:ok, read_lease} =
      Leasing.issue_read_lease(
        %{
          trace_id: "trace-lease-#{suffix}",
          tenant_id: "tenant-1",
          installation_id: "inst-1",
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

  defp execution_invalidations(reason) do
    Repo.all(LeaseInvalidation)
    |> Enum.filter(&(&1.reason == reason))
    |> Enum.sort_by(& &1.sequence_number)
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
