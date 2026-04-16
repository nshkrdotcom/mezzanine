defmodule Mezzanine.Execution.DispatcherTest do
  use Mezzanine.Execution.DataCase, async: false

  alias Mezzanine.Audit.ExecutionLineageStore
  alias Mezzanine.Execution.{Dispatcher, DispatchOutboxEntry, ExecutionRecord}
  alias Mezzanine.Objects.SubjectRecord

  @dispatch_snapshot %{
    "placement_ref" => "local_docker",
    "execution_params" => %{"timeout_ms" => 600_000},
    "connector_bindings" => %{"github_write" => %{"connector_key" => "github_app"}},
    "evidence_bindings" => %{"terminal_log" => %{"collector_key" => "artifact_collector"}},
    "actor_bindings" => %{"requester" => %{"resolver_key" => "static_actor"}}
  }

  test "dispatch_next accepts duplicate-safe submissions and preserves the frozen dispatch snapshot" do
    dispatch_now = due_now()

    assert {:ok, subject} = ingest_subject("linear:ticket:dispatcher-accepted")
    assert {:ok, execution} = dispatch_execution(subject, "trace-dispatcher-accepted", "accepted")

    assert {:ok, %{classification: :accepted, execution: accepted_execution}} =
             Dispatcher.dispatch_next(
               submit_fun: fn claimed ->
                 send(self(), {:claimed_dispatch, claimed})

                 {:accepted,
                  %{
                    "submission_ref" => %{"id" => "sub-1", "status" => "duplicate"},
                    "lower_receipt" => %{
                      "state" => "accepted",
                      "ji_submission_key" => "ji-sub-1",
                      "run_id" => "run-1"
                    }
                  }}
               end,
               actor_ref: %{kind: :dispatcher},
               now: dispatch_now
             )

    assert received_claim(execution.id) == %{
             execution_id: execution.id,
             installation_id: "inst-1",
             submission_dedupe_key: "inst-1:exec:accepted",
             compiled_pack_revision: 7,
             binding_snapshot: @dispatch_snapshot,
             dispatch_envelope: %{"capability" => "sandbox.exec"}
           }

    assert accepted_execution.dispatch_state == :accepted
    assert accepted_execution.compiled_pack_revision == 7
    assert accepted_execution.binding_snapshot == @dispatch_snapshot
    assert accepted_execution.submission_ref == %{"id" => "sub-1", "status" => "duplicate"}

    assert accepted_execution.lower_receipt == %{
             "state" => "accepted",
             "ji_submission_key" => "ji-sub-1",
             "run_id" => "run-1"
           }

    assert {:ok, outbox} = DispatchOutboxEntry.by_execution_id(execution.id)
    assert outbox.status == :completed
    assert outbox.compiled_pack_revision == 7
    assert outbox.binding_snapshot == @dispatch_snapshot

    assert {:ok, lineage} = ExecutionLineageStore.fetch(execution.id)
    assert lineage.ji_submission_key == "ji-sub-1"
    assert lineage.lower_run_id == "run-1"
  end

  test "dispatch_next reschedules retryable failures without re-resolving bindings or reclaiming immediately" do
    assert {:ok, subject} = ingest_subject("linear:ticket:dispatcher-retry")
    assert {:ok, execution} = dispatch_execution(subject, "trace-dispatcher-retry", "retry")

    retry_now =
      DateTime.utc_now()
      |> DateTime.truncate(:second)
      |> DateTime.add(5, :second)

    assert {:ok, %{classification: :retryable_failure, execution: retried_execution}} =
             Dispatcher.dispatch_next(
               submit_fun: fn _claimed ->
                 {:error, {:retryable, "bridge_unavailable", %{"reason" => "timeout"}}}
               end,
               actor_ref: %{kind: :dispatcher},
               now: retry_now,
               retry_delay_ms: 30_000
             )

    assert retried_execution.dispatch_state == :dispatching_retry
    assert retried_execution.dispatch_attempt_count == 1
    assert retried_execution.compiled_pack_revision == 7
    assert retried_execution.binding_snapshot == @dispatch_snapshot

    assert DateTime.compare(
             retried_execution.next_dispatch_at,
             DateTime.add(retry_now, 30, :second)
           ) == :eq

    assert {:ok, outbox} = DispatchOutboxEntry.by_execution_id(execution.id)
    assert outbox.status == :pending_retry
    assert DateTime.compare(outbox.available_at, DateTime.add(retry_now, 30, :second)) == :eq
    assert outbox.compiled_pack_revision == 7
    assert outbox.binding_snapshot == @dispatch_snapshot

    assert {:ok, :empty} =
             Dispatcher.dispatch_next(
               submit_fun: fn _ -> flunk("dispatch row was reclaimed before retry time") end,
               actor_ref: %{kind: :dispatcher},
               now: retry_now
             )
  end

  test "dispatch_next records terminal rejection and stops retries" do
    reject_now = due_now()

    assert {:ok, subject} = ingest_subject("linear:ticket:dispatcher-reject")
    assert {:ok, execution} = dispatch_execution(subject, "trace-dispatcher-reject", "reject")

    assert {:ok, %{classification: :terminal_rejection, execution: rejected_execution}} =
             Dispatcher.dispatch_next(
               submit_fun: fn _claimed ->
                 {:error,
                  {:terminal, "unsupported_capability",
                   %{"capability" => "prod_ssh_write", "state" => "rejected"}}}
               end,
               actor_ref: %{kind: :dispatcher},
               now: reject_now
             )

    assert rejected_execution.dispatch_state == :rejected
    assert rejected_execution.terminal_rejection_reason == "unsupported_capability"

    assert {:ok, outbox} = DispatchOutboxEntry.by_execution_id(execution.id)
    assert outbox.status == :terminal

    assert {:ok, :empty} =
             Dispatcher.dispatch_next(
               submit_fun: fn _ -> flunk("terminal row should not be retried") end,
               actor_ref: %{kind: :dispatcher},
               now: reject_now
             )
  end

  test "reconcile_result records semantic failure without reopening the lowering outbox" do
    accepted_now = due_now()

    assert {:ok, subject} = ingest_subject("linear:ticket:dispatcher-semantic")
    assert {:ok, execution} = dispatch_execution(subject, "trace-dispatcher-semantic", "semantic")

    assert {:ok, %{classification: :accepted, execution: accepted_execution}} =
             Dispatcher.dispatch_next(
               submit_fun: fn _claimed ->
                 {:accepted,
                  %{
                    "submission_ref" => %{"id" => "sub-semantic"},
                    "lower_receipt" => %{"state" => "accepted", "run_id" => "run-semantic"}
                  }}
               end,
               actor_ref: %{kind: :dispatcher},
               now: accepted_now
             )

    assert {:ok, %{classification: :semantic_failure, execution: failed_execution}} =
             Dispatcher.reconcile_result(
               accepted_execution,
               {:semantic_failure,
                %{
                  "lower_receipt" => %{"state" => "accepted", "run_id" => "run-semantic"},
                  "error" => %{"kind" => "semantic_failure", "reason" => "model_confused"}
                }},
               actor_ref: %{kind: :reconciler},
               trace_id: "trace-semantic-reconcile",
               causation_id: "cause-semantic-reconcile"
             )

    assert failed_execution.dispatch_state == :failed
    assert failed_execution.failure_kind == :semantic_failure

    assert failed_execution.last_dispatch_error_payload == %{
             "error" => %{"kind" => "semantic_failure", "reason" => "model_confused"}
           }

    assert {:ok, outbox} = DispatchOutboxEntry.by_execution_id(execution.id)
    assert outbox.status == :completed
  end

  defp received_claim(expected_execution_id) do
    assert_receive {:claimed_dispatch, claimed}
    assert claimed.execution_id == expected_execution_id

    Map.take(claimed, [
      :execution_id,
      :installation_id,
      :submission_dedupe_key,
      :compiled_pack_revision,
      :binding_snapshot,
      :dispatch_envelope
    ])
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

  defp due_now do
    DateTime.utc_now()
    |> DateTime.truncate(:second)
    |> DateTime.add(5, :second)
  end
end
