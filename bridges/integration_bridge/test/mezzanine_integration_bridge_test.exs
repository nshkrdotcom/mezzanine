defmodule Mezzanine.IntegrationBridgeTest do
  use ExUnit.Case, async: false

  alias Ecto.Adapters.SQL.Sandbox
  alias Jido.Integration.V2.TenantScope
  alias Mezzanine.Audit.{ExecutionLineage, ExecutionLineageStore, Repo}
  alias Mezzanine.IntegrationBridge
  alias Mezzanine.IntegrationBridge.AuthorizedInvocation
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

  test "invoke_run_intent dispatches only an authorized invocation envelope" do
    invocation = authorized_invocation()

    invoke_fun = fn capability, input, opts ->
      send(self(), {:invoke, capability, input, opts})
      {:ok, %{capability: capability, input: input}}
    end

    assert {:ok, %{capability: "linear.issue.execute"}} =
             IntegrationBridge.invoke_run_intent(
               invocation,
               invoke_fun: invoke_fun,
               invoke_opts: [connection_id: "conn-1"]
             )

    assert_received {:invoke, "linear.issue.execute", input, [connection_id: "conn-1"]}
    assert input.invocation_request == invocation.invocation_request
    assert input.idempotency_key == "idem-1"
    assert input.submission_dedupe_key == "dedupe-1"
    assert input.authority.permission_decision_ref == "mock-decision-123"
    assert input.authority.policy_version == "mock-v1"
  end

  test "dispatch_effect dispatches only an authorized invocation envelope" do
    invocation = authorized_invocation()

    invoke_fun = fn capability, input, _opts ->
      {:ok, %{capability: capability, input: input}}
    end

    assert {:ok, %{capability: "linear.issue.update"}} =
             IntegrationBridge.dispatch_effect(invocation,
               invoke_fun: invoke_fun,
               capability_id: "linear.issue.update"
             )
  end

  test "direct dispatch rejects old RunIntent and generic map inputs before Jido invocation" do
    intent =
      RunIntent.new!(%{
        intent_id: "intent-run-1",
        program_id: "program-1",
        work_id: "work-1",
        capability: "linear.issue.execute",
        input: %{"issue_id" => "ENG-42"}
      })

    invoke_fun = fn _capability, _input, _opts -> flunk("provider invoke must not run") end

    assert_raise FunctionClauseError, fn ->
      IntegrationBridge.invoke_run_intent(intent, invoke_fun: invoke_fun)
    end

    assert_raise FunctionClauseError, fn ->
      IntegrationBridge.invoke_run_intent(%{}, invoke_fun: invoke_fun)
    end
  end

  test "effect dispatch rejects old EffectIntent and unauthorized capability inputs" do
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

    invoke_fun = fn _capability, _input, _opts -> flunk("provider invoke must not run") end

    assert_raise FunctionClauseError, fn ->
      IntegrationBridge.dispatch_effect(intent, invoke_fun: invoke_fun)
    end

    assert_error_contains("not present in Citadel authority", fn ->
      IntegrationBridge.dispatch_effect(authorized_invocation(),
        invoke_fun: invoke_fun,
        capability_id: "github.pr.merge"
      )
    end)
  end

  test "authorized invocation requires mock-valid authority and governance packets" do
    attrs =
      authorized_invocation_attrs()
      |> put_in([:invocation_request, :authority_packet], %{})

    assert_error_contains("missing required field :contract_version", fn ->
      AuthorizedInvocation.new!(attrs)
    end)
  end

  test "authorized invocation rejects stale installation revision when caller supplies one" do
    assert %AuthorizedInvocation{} =
             AuthorizedInvocation.new!(
               Map.put(authorized_invocation_attrs(), :expected_installation_revision, 3)
             )

    assert_error_contains("stale installation_revision", fn ->
      authorized_invocation_attrs()
      |> Map.put(:expected_installation_revision, 2)
      |> AuthorizedInvocation.new!()
    end)
  end

  test "authorized invocation binds M2 lower submission to the Citadel for_action_ref" do
    attrs =
      authorized_invocation_attrs()
      |> Map.put(:action_ref, "action://agent-loop/turn-1")
      |> put_in([:invocation_request, :authority_packet, :extensions, "citadel"], %{
        "for_action_ref" => "action://agent-loop/turn-1"
      })
      |> put_in([:invocation_request, :execution_governance, :extensions, "citadel"], %{
        "for_action_ref" => "action://agent-loop/turn-1"
      })

    invocation = AuthorizedInvocation.new!(attrs)
    input = AuthorizedInvocation.invoke_input(invocation, "linear.issue.update")

    assert input.authority.for_action_ref == "action://agent-loop/turn-1"

    assert_error_contains("action_ref mismatch", fn ->
      attrs
      |> Map.put(:action_ref, "action://agent-loop/other")
      |> AuthorizedInvocation.new!()
    end)

    assert_error_contains("for_action_ref mismatch", fn ->
      attrs
      |> put_in([:invocation_request, :execution_governance, :extensions, "citadel"], %{
        "for_action_ref" => "action://agent-loop/other"
      })
      |> AuthorizedInvocation.new!()
    end)
  end

  defp assert_error_contains(fragment, fun) do
    error = assert_raise(ArgumentError, fun)

    assert Exception.message(error) |> String.contains?(fragment)
  end

  test "authorized invocation preserves the older M1 per-execution authority path" do
    assert %AuthorizedInvocation{} = AuthorizedInvocation.new!(authorized_invocation_attrs())
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
    assert result.staleness_class == :lower_fresh
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
    store_lineage!(tenant_id: "tenant-other")

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

  test "dispatch_read rejects missing execution lineage before lower fact access" do
    intent =
      ReadIntent.new!(%{
        intent_id: "read-missing-lineage",
        read_type: :lower_fact,
        subject: %{
          actor_id: "actor-1",
          tenant_id: "tenant-1",
          installation_id: "inst-1",
          execution_id: "exec-missing"
        },
        query: %{operation: :fetch_run}
      })

    assert {:error, :unknown_execution_lineage} =
             IntegrationBridge.dispatch_read(intent, lower_facts: LowerFactsStub)

    refute_received {:fetch_run, _args}
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

  test "dispatch_read denies cross-tenant lineage reuse before lower fact access" do
    store_lineage!(tenant_id: "tenant-1")

    intent =
      ReadIntent.new!(%{
        intent_id: "read-cross-tenant-lineage",
        read_type: :lower_fact,
        subject: %{
          actor_id: "actor-1",
          tenant_id: "tenant-2",
          installation_id: "inst-1",
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
             IntegrationBridge.dispatch_read(intent,
               lower_facts: LowerFactsStub,
               fetch_lineage: fn _execution_id ->
                 send(self(), :lineage_fetch_called)
                 {:error, :should_not_fetch}
               end
             )

    refute_received :lineage_fetch_called
    refute_received {:fetch_run, _args}
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

  defp store_lineage!(opts \\ []) do
    lineage =
      ExecutionLineage.new!(%{
        trace_id: "trace-1",
        tenant_id: Keyword.get(opts, :tenant_id, "tenant-1"),
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

  defp authorized_invocation do
    AuthorizedInvocation.new!(authorized_invocation_attrs())
  end

  defp authorized_invocation_attrs do
    %{
      tenant_id: "tenant-1",
      installation_id: "inst-1",
      subject_id: "subject-1",
      execution_id: "exec-1",
      trace_id: "trace-1",
      idempotency_key: "idem-1",
      submission_dedupe_key: "dedupe-1",
      invocation_request: invocation_request()
    }
  end

  defp invocation_request do
    %{
      schema_version: 2,
      invocation_request_id: "invoke-1",
      request_id: "request-1",
      session_id: "session-1",
      tenant_id: "tenant-1",
      trace_id: "trace-1",
      actor_id: "actor-1",
      target_id: "target-1",
      target_kind: "runtime_target",
      selected_step_id: "step-1",
      allowed_operations: ["linear.issue.execute", "linear.issue.update"],
      authority_packet: authority_packet(),
      boundary_intent: %{},
      topology_intent: %{},
      execution_governance: execution_governance(),
      extensions: %{
        "citadel" => %{
          "execution_envelope" => %{
            "installation_id" => "inst-1",
            "installation_revision" => 3,
            "subject_id" => "subject-1",
            "execution_id" => "exec-1",
            "submission_dedupe_key" => "dedupe-1"
          }
        }
      }
    }
  end

  defp authority_packet do
    %{
      contract_version: "v1",
      decision_id: "mock-decision-123",
      tenant_id: "tenant-1",
      request_id: "request-1",
      policy_version: "mock-v1",
      boundary_class: "workspace_session",
      trust_profile: "baseline",
      approval_profile: "standard",
      egress_profile: "restricted",
      workspace_profile: "workspace",
      resource_profile: "standard",
      decision_hash: String.duplicate("a", 64),
      extensions: %{"citadel" => %{}}
    }
  end

  defp execution_governance do
    %{
      contract_version: "v1",
      execution_governance_id: "mock-governance-123",
      authority_ref: %{"decision_id" => "mock-decision-123"},
      sandbox: %{"allowed_tools" => ["linear.issue.update"]},
      boundary: %{},
      topology: %{},
      workspace: %{},
      resources: %{},
      placement: %{},
      operations: %{"allowed_operations" => ["linear.issue.execute", "linear.issue.update"]},
      extensions: %{"citadel" => %{}}
    }
  end
end
