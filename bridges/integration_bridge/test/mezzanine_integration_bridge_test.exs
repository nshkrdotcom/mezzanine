defmodule Mezzanine.IntegrationBridgeTest do
  use ExUnit.Case, async: false

  alias Ecto.Adapters.SQL.Sandbox
  alias Jido.Integration.V2.TenantScope
  alias Mezzanine.Audit.{ExecutionLineage, ExecutionLineageStore, Repo}
  alias Mezzanine.IntegrationBridge
  alias Mezzanine.Intent.{EffectIntent, ReadIntent, RunIntent}

  defmodule LowerFactsStub do
    @operations [
      :fetch_submission_receipt,
      :fetch_run,
      :attempts,
      :fetch_attempt,
      :events,
      :fetch_artifact,
      :run_artifacts,
      :resolve_trace
    ]

    def operations, do: @operations
    def operation_supported?(operation), do: operation in @operations

    def fetch_submission_receipt(%TenantScope{} = scope, submission_key) do
      dispatch(:fetch_submission_receipt, [scope, submission_key], %{
        submission_key: submission_key
      })
    end

    def fetch_run(%TenantScope{} = scope, run_id) do
      dispatch(:fetch_run, [scope, run_id], %{run_id: run_id, status: :completed})
    end

    def attempts(%TenantScope{} = scope, run_id) do
      dispatch(:attempts, [scope, run_id], [
        %{attempt_id: "attempt-1", run_id: run_id, status: :completed}
      ])
    end

    def fetch_attempt(%TenantScope{} = scope, attempt_id) do
      dispatch(:fetch_attempt, [scope, attempt_id], %{
        attempt_id: attempt_id,
        run_id: "run-1",
        status: :completed
      })
    end

    def events(%TenantScope{} = scope, run_id) do
      dispatch(:events, [scope, run_id], [%{run_id: run_id, type: "attempt.completed"}])
    end

    def fetch_artifact(%TenantScope{} = scope, artifact_id) do
      dispatch(:fetch_artifact, [scope, artifact_id], %{artifact_id: artifact_id, run_id: "run-1"})
    end

    def run_artifacts(%TenantScope{} = scope, run_id) do
      dispatch(:run_artifacts, [scope, run_id], [%{artifact_id: "artifact-1", run_id: run_id}])
    end

    def resolve_trace(%TenantScope{} = scope, trace_id) do
      dispatch(:resolve_trace, [scope, trace_id], %{
        trace_id: trace_id,
        run: %{run_id: "run-1"}
      })
    end

    defp dispatch(operation, args, fallback) do
      send(Process.get(:integration_bridge_test_pid), {operation, args})

      case Process.get(:integration_bridge_test_responses, %{}) do
        %{^operation => handler} when is_function(handler, 1) -> handler.(args)
        _other -> default_reply(fallback)
      end
    end

    defp default_reply(value) when is_list(value), do: value
    defp default_reply(value), do: {:ok, value}
  end

  setup do
    owner = Sandbox.start_owner!(Repo, shared: true)

    Process.put(:integration_bridge_test_pid, self())
    Process.put(:integration_bridge_test_responses, %{})

    on_exit(fn ->
      Process.delete(:integration_bridge_test_pid)
      Process.delete(:integration_bridge_test_responses)
      Sandbox.stop_owner(owner)
    end)

    :ok
  end

  test "invoke_run_intent delegates to the public integration facade shape" do
    intent =
      RunIntent.new!(%{
        intent_id: "intent-run-1",
        program_id: "program-1",
        work_id: "work-1",
        capability: "linear.issue.execute",
        input: %{"issue_id" => "ENG-42"}
      })

    invoke_fun = fn capability, input, opts ->
      send(self(), {:invoke, capability, input, opts})
      {:ok, %{capability: capability, input: input}}
    end

    assert {:ok, %{capability: "linear.issue.execute"}} =
             IntegrationBridge.invoke_run_intent(
               intent,
               invoke_fun: invoke_fun,
               invoke_opts: [connection_id: "conn-1"]
             )

    assert_received {:invoke, "linear.issue.execute", %{"issue_id" => "ENG-42"},
                     [connection_id: "conn-1"]}
  end

  test "dispatch_effect invokes a capability-backed effect" do
    intent =
      EffectIntent.new!(%{
        intent_id: "effect-1",
        effect_type: :connector_effect,
        subject: "issue",
        payload: %{
          capability_id: "linear.issue.update",
          input: %{"id" => "ENG-42", "state" => "done"}
        }
      })

    invoke_fun = fn capability, input, _opts ->
      {:ok, %{capability: capability, input: input}}
    end

    assert {:ok, %{capability: "linear.issue.update"}} =
             IntegrationBridge.dispatch_effect(intent, invoke_fun: invoke_fun)
  end

  test "dispatch_read routes generic lower reads through lineage-owned lower facts" do
    store_lineage!()

    intent =
      ReadIntent.new!(%{
        intent_id: "read-1",
        read_type: :lower_fact,
        subject: %{
          actor_id: "actor-1",
          tenant_id: "tenant-1",
          installation_id: "inst-1",
          execution_id: "exec-1"
        },
        query: %{
          operation: :fetch_run
        }
      })

    assert {:ok, result} =
             IntegrationBridge.dispatch_read(intent, lower_facts: LowerFactsStub)

    assert result.operation == :fetch_run
    assert result.source == :lower_run_status
    assert result.freshness == :lower_authoritative_unreconciled
    refute result.operator_actionable?

    assert result.lineage == %{
             execution_id: "exec-1",
             installation_id: "inst-1",
             subject_id: "subject-1",
             trace_id: "trace-1"
           }

    assert result.result.run_id == "run-1"

    assert_received {:fetch_run,
                     [
                       %TenantScope{tenant_id: "tenant-1", installation_id: "inst-1"},
                       "run-1"
                     ]}
  end

  test "dispatch_read passes tenant scope to the substrate read slice and fails closed on mismatch" do
    store_lineage!()

    Process.put(:integration_bridge_test_responses, %{
      fetch_run: fn [%TenantScope{tenant_id: "tenant-other"}, _run_id] ->
        {:error, :tenant_mismatch}
      end
    })

    intent =
      ReadIntent.new!(%{
        intent_id: "read-tenant-mismatch",
        read_type: :lower_fact,
        subject: %{
          actor_id: "actor-1",
          tenant_id: "tenant-other",
          installation_id: "inst-1",
          execution_id: "exec-1"
        },
        query: %{operation: :fetch_run}
      })

    assert {:error, :tenant_mismatch} =
             IntegrationBridge.dispatch_read(intent, lower_facts: LowerFactsStub)

    assert_received {:fetch_run,
                     [
                       %TenantScope{tenant_id: "tenant-other", installation_id: "inst-1"},
                       "run-1"
                     ]}
  end

  test "dispatch_read denies lower reads when installation context does not match the stored lineage" do
    store_lineage!()

    intent =
      ReadIntent.new!(%{
        intent_id: "read-2",
        read_type: :lower_fact,
        subject: %{
          actor_id: "actor-1",
          tenant_id: "tenant-1",
          installation_id: "inst-2",
          execution_id: "exec-1"
        },
        query: %{operation: :fetch_run}
      })

    assert {:error, :unauthorized_lower_read} =
             IntegrationBridge.dispatch_read(intent, lower_facts: LowerFactsStub)

    refute_received {:fetch_run, _args}
  end

  test "dispatch_read forbids caller supplied lower run ids from becoming the primary lookup surface" do
    intent =
      ReadIntent.new!(%{
        intent_id: "read-3",
        read_type: :lower_fact,
        subject: %{
          actor_id: "actor-1",
          tenant_id: "tenant-1",
          installation_id: "inst-1",
          execution_id: "exec-1"
        },
        query: %{
          operation: :fetch_run,
          run_id: "run-override"
        }
      })

    assert {:error, {:lower_identifier_override_forbidden, :run_id}} =
             IntegrationBridge.dispatch_read(intent, lower_facts: LowerFactsStub)
  end

  test "dispatch_read rejects mismatched lower artifacts even after authorization succeeds" do
    store_lineage!()

    Process.put(:integration_bridge_test_responses, %{
      fetch_artifact: fn [%TenantScope{}, artifact_id] ->
        {:ok, %{artifact_id: artifact_id, run_id: "run-other"}}
      end
    })

    intent =
      ReadIntent.new!(%{
        intent_id: "read-4",
        read_type: :lower_fact,
        subject: %{
          actor_id: "actor-1",
          tenant_id: "tenant-1",
          installation_id: "inst-1",
          execution_id: "exec-1"
        },
        query: %{
          operation: :fetch_artifact,
          artifact_id: "artifact-1"
        }
      })

    assert {:error,
            {:mismatched_lower_fact,
             %{
               operation: :fetch_artifact,
               field: :run_id,
               expected: "run-1",
               actual: "run-other"
             }}} =
             IntegrationBridge.dispatch_read(intent, lower_facts: LowerFactsStub)

    assert_received {:fetch_artifact, [%TenantScope{tenant_id: "tenant-1"}, "artifact-1"]}
  end

  test "event translation maps direct platform outcomes to audit attrs" do
    mapped =
      IntegrationBridge.to_audit_attrs(
        %{status: :started, run_id: "run-1", payload: %{"attempt" => 1}},
        %{program_id: "program-1", work_object_id: "work-1"}
      )

    assert mapped.event_kind == :run_started
    assert mapped.program_id == "program-1"
    assert mapped.work_object_id == "work-1"
  end

  defp store_lineage! do
    lineage =
      ExecutionLineage.new!(%{
        trace_id: "trace-1",
        installation_id: "inst-1",
        subject_id: "subject-1",
        execution_id: "exec-1",
        ji_submission_key: "submission-1",
        lower_run_id: "run-1",
        lower_attempt_id: "attempt-1",
        artifact_refs: ["artifact-1"]
      })

    assert {:ok, _stored} = ExecutionLineageStore.store(lineage)
  end
end
