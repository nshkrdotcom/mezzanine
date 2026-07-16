defmodule Mezzanine.WorkflowRuntime.RunOutboxDispatcherTest do
  use ExUnit.Case, async: false

  alias Mezzanine.Runs.WorkflowHandoff
  alias Mezzanine.WorkflowRuntime.Application, as: RuntimeApplication
  alias Mezzanine.WorkflowRuntime.RunOutboxDispatcher
  alias Mezzanine.WorkflowRuntime.TemporalSupervisor

  defmodule Store do
    def claim_workflow_handoffs("dispatcher-test", 1, opts) do
      send(Keyword.fetch!(opts, :test_pid), :claimed_after_commit)

      {:ok,
       [
         WorkflowHandoff.new!(
           outbox_ref: "outbox://mezzanine/run-dispatch/workflow-start",
           event_ref: "event://mezzanine/run-dispatch/1",
           run_ref: "run://mezzanine/run-dispatch",
           workflow_ref: "workflow://temporal/run-dispatch",
           workflow_type: "mezzanine.agent-run.v1",
           temporal_namespace: "nshkr-production",
           task_queue: "nshkr.mezzanine.agent-run.v1",
           idempotency_key: "run-dispatch:workflow-start",
           state: "dispatched",
           attempt: 1
         )
       ]}
    end

    def fetch_projection("run://mezzanine/run-dispatch", opts) do
      send(Keyword.fetch!(opts, :test_pid), :projection_read)

      {:ok,
       %{
         tenant_ref: "tenant://acme",
         projection: %{"trace_ref" => "trace://synapse/run-dispatch"}
       }}
    end

    def complete_workflow_handoff(outbox_ref, state, error_ref, opts) do
      send(Keyword.fetch!(opts, :test_pid), {:completed, outbox_ref, state, error_ref})

      WorkflowHandoff.new(%{
        outbox_ref: outbox_ref,
        event_ref: "event://mezzanine/run-dispatch/1",
        run_ref: "run://mezzanine/run-dispatch",
        workflow_ref: "workflow://temporal/run-dispatch",
        workflow_type: "mezzanine.agent-run.v1",
        temporal_namespace: "nshkr-production",
        task_queue: "nshkr.mezzanine.agent-run.v1",
        idempotency_key: "run-dispatch:workflow-start",
        state: state,
        attempt: 1,
        last_error_ref: error_ref
      })
    end
  end

  defmodule Runtime do
    def start_workflow(request) do
      send(Application.fetch_env!(:mezzanine_workflow_runtime, :dispatcher_test_pid), {
        :temporal_start,
        request
      })

      {:ok, %{workflow_run_id: "temporal-run-1"}}
    end
  end

  test "dispatches only after claiming the committed outbox and records acknowledgement" do
    Application.put_env(:mezzanine_workflow_runtime, :dispatcher_test_pid, self())
    on_exit(fn -> Application.delete_env(:mezzanine_workflow_runtime, :dispatcher_test_pid) end)

    start_supervised!(
      {RunOutboxDispatcher,
       name: :dispatcher_test,
       schedule?: false,
       store: Store,
       runtime: Runtime,
       lock_owner: "dispatcher-test",
       batch_size: 1,
       store_opts: [test_pid: self()]}
    )

    assert :ok = RunOutboxDispatcher.dispatch_once(:dispatcher_test)
    assert_received :claimed_after_commit
    assert_received :projection_read
    assert_received {:temporal_start, request}
    assert request.task_queue == "nshkr.mezzanine.agent-run.v1"
    assert request.workflow_module == Mezzanine.Workflows.NshkrAgentRun
    assert request.args.run_ref == "run://mezzanine/run-dispatch"

    assert_received {:completed, "outbox://mezzanine/run-dispatch/workflow-start", "acknowledged",
                     nil}
  end

  test "builds the exact production Temporal worker and task queue" do
    [spec] =
      TemporalSupervisor.task_queue_specs(
        enabled?: true,
        task_queues: ["nshkr.mezzanine.agent-run.v1"],
        instance_base: Mezzanine.WorkflowRuntime.TestTemporal,
        address: "127.0.0.1:7233",
        namespace: "nshkr-production"
      )

    assert spec.task_queue == "nshkr.mezzanine.agent-run.v1"
    assert spec.workflows == [Mezzanine.Workflows.NshkrAgentRun]
    assert spec.activities == []
    assert spec.name == Mezzanine.WorkflowRuntime.TestTemporal.NshkrAgentRun
  end

  test "production composition starts the Temporal worker before the outbox dispatcher" do
    [worker_spec, dispatcher_spec] =
      RuntimeApplication.production_child_specs(
        temporal: [
          enabled?: true,
          task_queues: ["nshkr.mezzanine.agent-run.v1"],
          instance_base: Mezzanine.WorkflowRuntime.TestTemporal,
          address: "127.0.0.1:7233",
          namespace: "nshkr-production"
        ],
        outbox_dispatcher: [schedule?: false]
      )

    assert worker_spec.id == Mezzanine.WorkflowRuntime.TestTemporal.NshkrAgentRun

    assert dispatcher_spec ==
             {RunOutboxDispatcher, [schedule?: false]}
  end
end
