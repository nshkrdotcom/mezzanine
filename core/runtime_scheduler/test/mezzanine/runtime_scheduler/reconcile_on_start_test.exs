defmodule Mezzanine.RuntimeScheduler.ReconcileOnStartTest do
  use Mezzanine.RuntimeScheduler.DataCase, async: false

  alias Ash
  alias Ecto.Adapters.SQL.Sandbox
  alias Mezzanine.Audit.Repo, as: AuditRepo
  alias Mezzanine.Execution.{Dispatcher, DispatchOutboxEntry, ExecutionRecord}
  alias Mezzanine.Execution.Repo, as: ExecutionRepo
  alias Mezzanine.Objects.Repo, as: ObjectsRepo
  alias Mezzanine.Objects.SubjectRecord
  alias Mezzanine.RuntimeScheduler.ReconcileOnStart
  alias Mezzanine.RuntimeScheduler.Repo, as: RuntimeSchedulerRepo

  @dispatch_snapshot %{
    "placement_ref" => "local_docker",
    "execution_params" => %{"timeout_ms" => 600_000},
    "connector_bindings" => %{"github_write" => %{"connector_key" => "github_app"}}
  }

  test "requeues dispatching rows claimed before restart and replays them without duplicating the outbox row",
       %{sandbox_owners: sandbox_owners} do
    assert {:ok, subject} = ingest_subject("linear:ticket:restart-recovery")
    assert {:ok, execution} = dispatch_execution(subject, "trace-runtime-recovery", "restart")
    assert {:ok, initial_outbox} = DispatchOutboxEntry.by_execution_id(execution.id)
    crash_now = DateTime.add(initial_outbox.available_at, 5, :second)
    recovery_now = DateTime.add(crash_now, 5, :second)

    claimed = crash_after_claim!(crash_now, sandbox_owners)

    assert claimed.execution_id == execution.id
    assert claimed.outbox_id == initial_outbox.id
    assert claimed.submission_dedupe_key == "inst-1:exec:restart"

    assert {:ok, dispatching_execution} = Ash.get(ExecutionRecord, execution.id)
    assert dispatching_execution.dispatch_state == :dispatching

    assert {:ok, dispatching_outbox} = DispatchOutboxEntry.by_execution_id(execution.id)
    assert dispatching_outbox.status == :dispatching

    assert {:ok, summary} = ReconcileOnStart.reconcile("inst-1", recovery_now)
    assert summary.recovered_count == 1
    assert summary.recovered_execution_ids == [execution.id]

    assert {:ok, recovered_execution} = Ash.get(ExecutionRecord, execution.id)
    assert recovered_execution.dispatch_state == :dispatching_retry
    assert recovered_execution.dispatch_attempt_count == 1
    assert recovered_execution.next_dispatch_at == recovery_now
    assert recovered_execution.last_dispatch_error_kind == "restart_recovery"

    assert {:ok, recovered_outbox} = DispatchOutboxEntry.by_execution_id(execution.id)
    assert recovered_outbox.id == initial_outbox.id
    assert recovered_outbox.status == :pending_retry
    assert recovered_outbox.available_at == recovery_now

    assert {:ok, %{classification: :accepted, execution: accepted_execution}} =
             Dispatcher.dispatch_next(
               submit_fun: fn replay_claim ->
                 assert replay_claim.execution_id == execution.id
                 assert replay_claim.outbox_id == initial_outbox.id
                 assert replay_claim.submission_dedupe_key == claimed.submission_dedupe_key

                 {:accepted,
                  %{
                    "submission_ref" => %{"id" => "sub-recovered", "status" => "duplicate"},
                    "lower_receipt" => %{
                      "state" => "accepted",
                      "ji_submission_key" => "ji-sub-recovered",
                      "run_id" => "run-recovered"
                    }
                  }}
               end,
               actor_ref: %{kind: :dispatcher},
               now: recovery_now
             )

    assert accepted_execution.dispatch_state == :accepted
    assert accepted_execution.dispatch_attempt_count == 1

    assert {:ok, completed_outbox} = DispatchOutboxEntry.by_execution_id(execution.id)
    assert completed_outbox.id == initial_outbox.id
    assert completed_outbox.status == :completed
  end

  defp crash_after_claim!(now, sandbox_owners) do
    parent = self()

    {pid, ref} =
      spawn_monitor(fn ->
        receive do
          :proceed ->
            Dispatcher.dispatch_next(
              submit_fun: fn claimed ->
                send(parent, {:claimed_dispatch, claimed})
                exit(:dispatch_process_crashed)
              end,
              actor_ref: %{kind: :dispatcher},
              now: now
            )
        end
      end)

    allow_sandbox_access!(sandbox_owners, pid)
    send(pid, :proceed)

    assert_receive {:claimed_dispatch, claimed}
    assert_receive {:DOWN, ^ref, :process, ^pid, :dispatch_process_crashed}
    claimed
  end

  defp allow_sandbox_access!(sandbox_owners, pid) do
    owner_by_repo = %{
      AuditRepo => sandbox_owners.audit,
      ExecutionRepo => sandbox_owners.execution,
      ObjectsRepo => sandbox_owners.objects,
      RuntimeSchedulerRepo => sandbox_owners.runtime_scheduler
    }

    Enum.each(owner_by_repo, fn {repo, owner} ->
      :ok = Sandbox.allow(repo, owner, pid)
    end)
  end

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

  defp dispatch_execution(subject, trace_id, suffix) do
    ExecutionRecord.dispatch(%{
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
end
