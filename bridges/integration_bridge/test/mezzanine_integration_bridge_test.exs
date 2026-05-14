defmodule Mezzanine.IntegrationBridgeTest do
  use ExUnit.Case, async: false

  alias Ecto.Adapters.SQL.Sandbox
  alias Jido.Integration.V2.GovernedLowerDenial
  alias Jido.Integration.V2.GovernedLowerEnvelope
  alias Jido.Integration.V2.GovernedLowerReceipt
  alias Jido.Integration.V2.TenantScope
  alias Mezzanine.Audit.{ExecutionLineage, ExecutionLineageStore, Repo}
  alias Mezzanine.IntegrationBridge
  alias Mezzanine.IntegrationBridge.AuthorizedInvocation
  alias Mezzanine.IntegrationBridge.CodexAgentRuntime
  alias Mezzanine.IntegrationBridge.GitHubPrEvidenceRuntime
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

    assert {:ok, %{capability: "linear.issues.retrieve"}} =
             IntegrationBridge.invoke_run_intent(
               invocation,
               invoke_fun: invoke_fun,
               invoke_opts: [connection_id: "conn-1"]
             )

    assert_received {:invoke, "linear.issues.retrieve", input, opts}
    assert Keyword.fetch!(opts, :connection_id) == "conn-1"
    assert Keyword.fetch!(opts, :tenant_id) == invocation.tenant_id
    assert Keyword.fetch!(opts, :trace_id) == invocation.trace_id
    assert Keyword.fetch!(opts, :actor_id) == "actor-1"
    assert Keyword.fetch!(opts, :environment) == :prod

    assert Keyword.fetch!(opts, :allowed_operations) == [
             "linear.issues.retrieve",
             "linear.issues.update"
           ]

    assert input.invocation_request == invocation.invocation_request
    assert input.idempotency_key == "idem-1"
    assert input.submission_dedupe_key == "dedupe-1"
    assert input.authority.permission_decision_ref == "mock-decision-123"
    assert input.authority.policy_version == "mock-v1"
  end

  test "invoke_run_intent returns a governed lower denial for dry-run writes before provider dispatch" do
    invocation = authorized_invocation_allowing(["linear.comments.create"])

    invoke_fun = fn _capability, _input, _opts ->
      send(self(), :unexpected_provider_dispatch)
      {:ok, %{}}
    end

    assert {:error, %GovernedLowerDenial{} = denial} =
             IntegrationBridge.invoke_run_intent(invocation,
               capability_id: "linear.comments.create",
               input: %{issue_id: "lin-issue-321", body: "Ready for review"},
               dry_run?: true,
               invoke_fun: invoke_fun
             )

    refute_received :unexpected_provider_dispatch
    assert denial.denial_class == :policy_denied
    assert denial.capability_id == "linear.comments.create"
    assert denial.lower_request_ref == "lower-request://exec-1/linear.comments.create"
    assert denial.reason =~ "dry run"
  end

  test "Linear GraphQL dynamic tool returns the Codex tool response shape" do
    invocation = authorized_invocation_allowing(["linear.graphql.execute"])

    invoke_fun = fn capability, input, opts ->
      send(self(), {:linear_graphql_invoke, capability, input, opts})

      {:ok,
       %{
         output: %{
           data: %{"viewer" => %{"id" => "usr-linear-viewer"}}
         }
       }}
    end

    assert {:ok, result} =
             IntegrationBridge.execute_dynamic_tool(
               invocation,
               "linear_graphql",
               %{
                 "query" => "query Viewer { viewer { id } }",
                 "variables" => %{"includeTeams" => false}
               },
               invoke_fun: invoke_fun,
               credential_redeemed?: true
             )

    assert_received {:linear_graphql_invoke, "linear.graphql.execute", input, opts}
    assert input.query == "query Viewer { viewer { id } }"
    assert input.variables == %{"includeTeams" => false}
    assert Keyword.fetch!(opts, :allowed_operations) == ["linear.graphql.execute"]

    assert result.operation == "linear.graphql.execute"
    assert result.tool_name == "linear_graphql"
    assert result.success? == true
    assert result.provider_request_sent? == true
    assert result.provider_response_received? == true
    assert result.credential_redeemed? == true

    assert result.dynamic_tool_response["success"] == true

    assert result.dynamic_tool_response["contentItems"] == [
             %{"type" => "inputText", "text" => result.dynamic_tool_response["output"]}
           ]

    assert Jason.decode!(result.dynamic_tool_response["output"]) == %{
             "data" => %{"viewer" => %{"id" => "usr-linear-viewer"}}
           }
  end

  test "Linear GraphQL dynamic tool marks partial GraphQL error responses unsuccessful" do
    invocation = authorized_invocation_allowing(["linear.graphql.execute"])

    invoke_fun = fn _capability, _input, _opts ->
      {:ok,
       %{
         output: %{
           data: %{"viewer" => nil},
           errors: [
             %{
               "message" => "Cannot resolve viewer",
               "extensions" => %{"code" => "UNAUTHENTICATED"}
             }
           ]
         }
       }}
    end

    assert {:ok, result} =
             IntegrationBridge.execute_dynamic_tool(
               invocation,
               "linear_graphql",
               "query Viewer { viewer { id } }",
               invoke_fun: invoke_fun
             )

    assert result.success? == false
    assert result.provider_request_sent? == true
    assert result.provider_response_received? == true

    assert Jason.decode!(result.dynamic_tool_response["output"]) == %{
             "data" => %{"viewer" => nil},
             "errors" => [
               %{
                 "message" => "Cannot resolve viewer",
                 "extensions" => %{"code" => "UNAUTHENTICATED"}
               }
             ]
           }
  end

  test "Linear GraphQL dynamic tool rejects invalid variables before provider dispatch" do
    invocation = authorized_invocation_allowing(["linear.graphql.execute"])

    invoke_fun = fn _capability, _input, _opts ->
      send(self(), :unexpected_graphql_dispatch)
      {:ok, %{}}
    end

    assert {:ok, result} =
             IntegrationBridge.execute_dynamic_tool(
               invocation,
               "linear_graphql",
               %{
                 "query" => "query Viewer { viewer { id } }",
                 "variables" => ["not", "an", "object"]
               },
               invoke_fun: invoke_fun
             )

    assert result.success? == false
    assert result.provider_request_sent? == false
    assert result.provider_response_received? == false

    assert Jason.decode!(result.dynamic_tool_response["output"]) == %{
             "error" => %{
               "message" => "`linear_graphql.variables` must be a JSON object when provided."
             }
           }

    refute_received :unexpected_graphql_dispatch
  end

  test "Linear GraphQL dynamic tool preserves GraphQL error bodies as failed tool output" do
    invocation = authorized_invocation_allowing(["linear.graphql.execute"])

    invoke_fun = fn _capability, _input, _opts ->
      {:error,
       %{
         code: "linear.not_found",
         message: "[not_found] Issue not found",
         upstream_context: %{
           http_status: 200,
           body: %{
             "errors" => [
               %{
                 "message" => "Issue not found",
                 "extensions" => %{"code" => "NOT_FOUND"}
               }
             ]
           }
         }
       }}
    end

    assert {:ok, result} =
             IntegrationBridge.execute_dynamic_tool(
               invocation,
               "linear_graphql",
               "query Viewer { viewer { id } }",
               invoke_fun: invoke_fun
             )

    assert result.success? == false
    assert result.provider_request_sent? == true
    assert result.provider_response_received? == true

    assert Jason.decode!(result.dynamic_tool_response["output"]) == %{
             "errors" => [
               %{
                 "message" => "Issue not found",
                 "extensions" => %{"code" => "NOT_FOUND"}
               }
             ]
           }
  end

  test "unsupported dynamic tools return a tool failure without provider dispatch" do
    invocation = authorized_invocation_allowing(["linear.graphql.execute"])

    invoke_fun = fn _capability, _input, _opts ->
      send(self(), :unexpected_dynamic_tool_dispatch)
      {:ok, %{}}
    end

    assert {:ok, result} =
             IntegrationBridge.execute_dynamic_tool(invocation, "not_a_real_tool", %{},
               invoke_fun: invoke_fun
             )

    assert result.success? == false
    assert result.provider_request_sent? == false
    assert result.provider_response_received? == false

    assert Jason.decode!(result.dynamic_tool_response["output"]) == %{
             "error" => %{
               "message" => "Unsupported dynamic tool: \"not_a_real_tool\".",
               "supportedTools" => ["linear_graphql"]
             }
           }

    refute_received :unexpected_dynamic_tool_dispatch
  end

  test "Linear source candidate fetch uses governed direct connector dispatch and SourceEngine normalization" do
    invocation = authorized_invocation_allowing(["linear.issues.list"])
    source_binding = put_in(source_binding(), [:candidate_filters, :team_id], "team-eng")

    invoke_fun = fn capability, input, opts ->
      send(self(), {:invoke, capability, input, opts})

      {:ok,
       %{
         output: %{
           issues: [linear_issue()],
           page_info: %{has_next_page: false}
         },
         artifact_refs: ["artifact://linear/issues-list"]
       }}
    end

    assert {:ok, result} =
             IntegrationBridge.fetch_linear_candidates(
               invocation,
               source_binding,
               invoke_fun: invoke_fun,
               viewer: %{id: "usr-linear-viewer"}
             )

    assert_received {:invoke, "linear.issues.list", input, opts}
    assert input.filter.state_names == ["Todo", "Backlog"]
    assert input.filter.team_id == "team-eng"
    assert input.filter.assignee_id == "usr-linear-viewer"
    assert input.governed_lower_envelope["lower_runtime_kind"] == "direct_connector"
    assert Keyword.fetch!(opts, :governed_lower_envelope).capability_id == "linear.issues.list"

    assert result.source_intake.operation == "linear.issues.list"
    assert [%{source_ref: "linear://inst-1/issue/ENG-321"}] = result.source_intake.subject_attrs
  end

  test "Linear API key credential ingress prepares an authorized invocation and connection opts" do
    api_key = "lin_api_live_secret"

    assert {:ok, prepared} =
             IntegrationBridge.prepare_linear_api_key_invocation(api_key, %{
               tenant_id: "tenant-linear-live",
               installation_id: "inst-linear-live",
               subject_id: "subject-linear-live",
               execution_id: "exec-linear-live",
               trace_id: "trace-linear-live",
               idempotency_key: "idem-linear-live",
               submission_dedupe_key: "dedupe-linear-live",
               actor_id: "operator-linear-live",
               allowed_operations: ["linear.issues.list"],
               subject: "linear-live-proof"
             })

    assert %AuthorizedInvocation{} = prepared.authorized_invocation
    assert is_binary(prepared.connection_id)
    assert prepared.connection_id != ""

    assert Keyword.fetch!(prepared.source_opts, :invoke_opts)[:connection_id] ==
             prepared.connection_id

    refute inspect(prepared) =~ api_key

    invoke_fun = fn capability, input, opts ->
      send(self(), {:invoke, capability, input, opts})

      {:ok,
       %{
         output: %{
           issues: [linear_issue()],
           page_info: %{has_next_page: false}
         }
       }}
    end

    assert {:ok, result} =
             IntegrationBridge.fetch_linear_candidates(
               prepared.authorized_invocation,
               source_binding(),
               Keyword.merge(prepared.source_opts,
                 invoke_fun: invoke_fun,
                 viewer: %{id: "usr-linear-viewer"}
               )
             )

    assert_received {:invoke, "linear.issues.list", _input, opts}
    assert Keyword.fetch!(opts, :connection_id) == prepared.connection_id
    assert result.credential_redeemed? == true
    assert result.provider_request_sent? == true
    assert result.provider_response_received? == true
    assert is_binary(result.lower_request_ref)
    assert is_binary(result.lower_receipt_ref)
    assert result.source_intake.operation == "linear.issues.list"
  end

  test "Linear API key credential ingress defaults include workflow-state lookup" do
    assert {:ok, prepared} =
             IntegrationBridge.prepare_linear_api_key_invocation("lin_api_live_secret", %{
               tenant_id: "tenant-linear-live-defaults",
               installation_id: "inst-linear-live-defaults",
               subject_id: "subject-linear-live-defaults",
               execution_id: "exec-linear-live-defaults",
               trace_id: "trace-linear-live-defaults",
               idempotency_key: "idem-linear-live-defaults",
               submission_dedupe_key: "dedupe-linear-live-defaults",
               actor_id: "operator-linear-live-defaults",
               subject: "linear-live-proof-defaults"
             })

    assert "linear.workflow_states.list" in prepared.authorized_invocation.invocation_request.allowed_operations

    assert "linear.issues.update" in prepared.authorized_invocation.invocation_request.allowed_operations
  end

  test "Codex agent runtime invokes Jido codex.session.turn and returns AppKit readback projection" do
    attrs = %{
      tenant_ref: "tenant://sample-app",
      installation_ref: "installation://sample-app/codex",
      subject_ref: "subject://sample-app/codex",
      run_ref: "run://sample-app/codex",
      trace_id: "trace://sample-app/codex",
      idempotency_key: "idem-codex",
      authority_context_ref: "authority-context://sample-app/codex"
    }

    invoke_fun = fn capability_id, input, opts ->
      send(self(), {:codex_invoke, capability_id, input, opts})

      {:ok,
       %{
         run: %{run_id: "jido-run-codex"},
         attempt: %{attempt_id: "jido-attempt-codex"},
         output: %{
           text: "Sample App headless Codex live path is operational.",
           provider_session_id: "codex-provider-session-1",
           status: :completed
         }
       }}
    end

    assert {:ok, projection} =
             CodexAgentRuntime.run(attrs,
               invoke_fun: invoke_fun,
               connection_id: "conn-codex",
               start_runtime_router?: false,
               register_connector?: false
             )

    assert_received {:codex_invoke, "codex.session.turn", input, opts}
    assert input.prompt =~ "governed Codex runtime path"
    assert input.provider_metadata["app_server"] == true
    assert input.authority_metadata["capability_id"] == "codex.session.turn"
    assert Keyword.fetch!(opts, :connection_id) == "conn-codex"
    assert Keyword.fetch!(opts, :allowed_operations) == ["codex.session.turn"]
    assert Keyword.fetch!(opts, :tenant_id) == "tenant://sample-app"

    assert projection.run_ref == "run://sample-app/codex"
    assert projection.workflow_ref == "workflow://codex-agent-runtime/run-sample-app-codex"
    assert projection.status == "completed"
    assert [turn] = projection.turn_states
    assert turn.operation == "codex.session.turn"
    assert turn.credential_redeemed? == true
    assert turn.provider_request_sent? == true
    assert turn.provider_response_received? == true
    assert turn.session_ref == "session://codex/codex-provider-session-1"
    assert turn.lower_request_ref == "lower-request://jido-run-codex/codex.session.turn"

    assert turn.lower_receipt_ref ==
             "lower-receipt://jido-attempt-codex/codex.session.turn/succeeded"
  end

  test "Codex agent runtime uses caller supplied first prompt and redacts prompt readback" do
    prompt = "Implement the governed first prompt path. SECRET_PROMPT_BODY_DO_NOT_EXPOSE"
    prompt_hash = sha256(prompt)

    attrs = %{
      tenant_ref: "tenant://sample-app",
      installation_ref: "installation://sample-app/codex",
      subject_ref: "subject://sample-app/codex-first-prompt",
      run_ref: "run://sample-app/codex-first-prompt",
      trace_id: "trace://sample-app/codex-first-prompt",
      idempotency_key: "idem-codex-first-prompt",
      authority_context_ref: "authority-context://sample-app/codex-first-prompt",
      initial_input_body: prompt,
      initial_input_ref: "prompt://sample-app/task/first-turn",
      initial_input_hash: prompt_hash,
      initial_input_source_ref: "workflow://sample-app/default",
      initial_input_rendered?: true,
      initial_input_body_redacted?: true
    }

    invoke_fun = fn capability_id, input, opts ->
      send(self(), {:codex_first_prompt_invoke, capability_id, input, opts})

      {:ok,
       %{
         run: %{run_id: "jido-run-codex-first-prompt"},
         attempt: %{attempt_id: "jido-attempt-codex-first-prompt"},
         output: %{
           text: "prompt received",
           provider_session_id: "codex-provider-session-first-prompt",
           provider_turn_id: "codex-provider-turn-first-prompt",
           status: :completed
         }
       }}
    end

    assert {:ok, projection} =
             CodexAgentRuntime.run(attrs,
               invoke_fun: invoke_fun,
               connection_id: "conn-codex",
               start_runtime_router?: false,
               register_connector?: false
             )

    assert_received {:codex_first_prompt_invoke, "codex.session.turn", input, opts}
    assert input.prompt == prompt
    assert Keyword.fetch!(opts, :connection_id) == "conn-codex"

    assert [turn] = projection.turn_states
    assert turn.first_prompt_confirmed? == true
    assert turn.prompt_ref == "prompt://sample-app/task/first-turn"
    assert turn.prompt_hash == prompt_hash
    assert turn.prompt_source_ref == "workflow://sample-app/default"
    assert turn.prompt_rendered? == true
    assert turn.prompt_body_redacted? == true
    assert turn.prompt_body_included? == false

    prompt_evidence = projection.extensions["codex_first_prompt"]
    assert prompt_evidence["confirmed?"] == true
    assert prompt_evidence["prompt_ref"] == "prompt://sample-app/task/first-turn"
    assert prompt_evidence["prompt_hash"] == prompt_hash
    assert prompt_evidence["prompt_body_redacted?"] == true
    assert prompt_evidence["prompt_body_included?"] == false
    refute Map.has_key?(prompt_evidence, "prompt")
    refute inspect(projection) =~ "SECRET_PROMPT_BODY_DO_NOT_EXPOSE"

    assert Enum.any?(
             projection.runtime_events,
             &(&1.event_kind == "codex.first_prompt.confirmed")
           )
  end

  test "Codex agent runtime sends continuation guidance without resending first prompt" do
    first_prompt = "FIRST_PROMPT_SECRET_BODY should only be sent to turn one."

    continuation_guidance =
      "Continuation guidance: resume from current workspace state and stop at max turns."

    attrs = %{
      tenant_ref: "tenant://sample-app",
      installation_ref: "installation://sample-app/codex",
      subject_ref: "subject://sample-app/codex-continuation",
      run_ref: "run://sample-app/codex-continuation",
      trace_id: "trace://sample-app/codex-continuation",
      idempotency_key: "idem-codex-continuation",
      authority_context_ref: "authority-context://sample-app/codex-continuation",
      max_turns: 2,
      initial_input_body: first_prompt,
      initial_input_ref: "prompt://sample-app/task/first-turn",
      initial_input_hash: sha256(first_prompt),
      initial_input_source_ref: "workflow://sample-app/default",
      initial_input_rendered?: true,
      initial_input_body_redacted?: true,
      continuation_policy: %{mode: "until_max_turns", active_state?: true},
      continuation_input_body: continuation_guidance,
      continuation_input_ref: "continuation-guidance://sample-app/task/2",
      continuation_input_hash: sha256(continuation_guidance),
      continuation_input_source_ref: "workflow://sample-app/default",
      continuation_input_rendered?: true,
      continuation_input_body_redacted?: true
    }

    invoke_fun = fn capability_id, input, opts ->
      turn_index = Process.get(:codex_continuation_turn_index, 0) + 1
      Process.put(:codex_continuation_turn_index, turn_index)
      send(self(), {:codex_continuation_invoke, turn_index, capability_id, input, opts})

      {:ok,
       %{
         run: %{run_id: "jido-run-codex-continuation-#{turn_index}"},
         attempt: %{attempt_id: "jido-attempt-codex-continuation-#{turn_index}"},
         output: %{
           text: "turn #{turn_index} received",
           provider_session_id: "provider-session-continuation",
           provider_turn_id: "provider-turn-#{turn_index}",
           status: :completed
         }
       }}
    end

    on_exit(fn -> Process.delete(:codex_continuation_turn_index) end)

    assert {:ok, projection} =
             CodexAgentRuntime.run(attrs,
               invoke_fun: invoke_fun,
               connection_id: "conn-codex",
               start_runtime_router?: false,
               register_connector?: false
             )

    assert_received {:codex_continuation_invoke, 1, "codex.session.turn", first_input, _opts}
    assert_received {:codex_continuation_invoke, 2, "codex.session.turn", second_input, _opts}
    assert first_input.prompt == first_prompt
    assert second_input.prompt == continuation_guidance
    refute second_input.prompt =~ "FIRST_PROMPT_SECRET_BODY"

    assert second_input.continuation == %{
             strategy: :exact,
             provider_session_id: "provider-session-continuation"
           }

    assert [first_turn, second_turn] = projection.turn_states
    assert first_turn.turn_index == 1
    assert second_turn.turn_index == 2
    assert second_turn.continuation? == true
    assert second_turn.continuation_guidance_ref == "continuation-guidance://sample-app/task/2"
    assert second_turn.continuation_prompt_body_included? == false
    assert projection.budget_state == %{"turns_remaining" => 0}

    continuation = projection.extensions["codex_continuation"]
    assert continuation["confirmed?"] == true
    assert continuation["turn_count"] == 2
    assert continuation["continuation_turn_count"] == 1
    assert continuation["max_turns"] == 2
    assert continuation["max_turns_reached?"] == true
    refute inspect(projection) =~ "FIRST_PROMPT_SECRET_BODY"
  end

  test "Codex agent runtime projects lower app-server session start evidence" do
    attrs = %{
      tenant_ref: "tenant://sample-app",
      installation_ref: "installation://sample-app/codex",
      subject_ref: "subject://sample-app/codex",
      run_ref: "run://sample-app/codex-session-start",
      trace_id: "trace://sample-app/codex-session-start",
      idempotency_key: "idem-codex-session-start",
      authority_context_ref: "authority-context://sample-app/codex-session-start"
    }

    invoke_fun = fn capability_id, input, opts ->
      send(self(), {:codex_session_start_invoke, capability_id, input, opts})

      {:ok,
       %{
         run: %{run_id: "jido-run-codex-session"},
         attempt: %{attempt_id: "jido-attempt-codex-session"},
         output: %{
           text: "Sample App headless Codex live path is operational.",
           provider_session_id: "codex-provider-session-42",
           status: :completed
         }
       }}
    end

    events_fun = fn "jido-run-codex-session" ->
      [
        %{
          event_id: "event-session-started",
          type: "session.started",
          session_id: "asm-session-42",
          runtime_ref_id: "asm-session-42",
          payload: %{operation: :start, status: :ready}
        }
      ]
    end

    assert {:ok, projection} =
             CodexAgentRuntime.run(attrs,
               invoke_fun: invoke_fun,
               events_fun: events_fun,
               connection_id: "conn-codex",
               start_runtime_router?: false,
               register_connector?: false
             )

    assert_received {:codex_session_start_invoke, "codex.session.turn", _input, _opts}

    assert projection.extensions["codex_app_server_session_start"] == %{
             "confirmed?" => true,
             "operation" => "codex.session.start",
             "lifecycle" => "started",
             "runtime_control_session_id" => "asm-session-42",
             "runtime_control_session_ref" => "runtime-session://asm-session-42",
             "lower_event_ref" => "event-session-started",
             "lower_request_ref" => "lower-request://jido-run-codex-session/codex.session.start",
             "lower_receipt_ref" =>
               "lower-receipt://jido-run-codex-session/codex.session.start/asm-session-42/started"
           }

    assert Enum.any?(projection.action_receipts, fn receipt ->
             receipt.operation == "codex.session.start" and
               receipt.lower_receipt_ref ==
                 "lower-receipt://jido-run-codex-session/codex.session.start/asm-session-42/started"
           end)

    assert Enum.any?(projection.runtime_events, fn event ->
             event.event_kind == "codex.session.started" and
               event.session_ref == "runtime-session://asm-session-42" and
               event.extensions.lower_event_ref == "event-session-started"
           end)

    assert "lower-request://jido-run-codex-session/codex.session.start" in projection.receipt_ref_set.lower_request_refs

    assert "lower-receipt://jido-run-codex-session/codex.session.start/asm-session-42/started" in projection.receipt_ref_set.lower_receipt_refs
  end

  test "Codex agent runtime projects app-server protocol initialization evidence" do
    attrs = %{
      tenant_ref: "tenant://sample-app",
      installation_ref: "installation://sample-app/codex",
      subject_ref: "subject://sample-app/codex",
      run_ref: "run://sample-app/codex-protocol",
      trace_id: "trace://sample-app/codex-protocol",
      idempotency_key: "idem-codex-protocol",
      authority_context_ref: "authority-context://sample-app/codex-protocol"
    }

    invoke_fun = fn capability_id, input, opts ->
      send(self(), {:codex_protocol_invoke, capability_id, input, opts})

      {:ok,
       %{
         run: %{run_id: "jido-run-codex-protocol"},
         attempt: %{attempt_id: "jido-attempt-codex-protocol"},
         output: %{
           text: "Sample App headless Codex live path is operational.",
           provider_session_id: "codex-provider-thread-99",
           provider_turn_id: "codex-provider-turn-99",
           status: :completed
         }
       }}
    end

    events_fun = fn "jido-run-codex-protocol" ->
      [
        %{
          event_id: "event-session-started-protocol",
          type: "session.started",
          session_id: "asm-session-99",
          runtime_ref_id: "asm-session-99",
          payload: %{operation: :start, status: :ready}
        }
      ]
    end

    assert {:ok, projection} =
             CodexAgentRuntime.run(attrs,
               invoke_fun: invoke_fun,
               events_fun: events_fun,
               connection_id: "conn-codex",
               start_runtime_router?: false,
               register_connector?: false
             )

    assert_received {:codex_protocol_invoke, "codex.session.turn", input, _opts}
    assert input.provider_metadata["app_server"] == true
    assert input.cwd == "/tmp/jido_codex_cli_workspace"

    assert [turn] = projection.turn_states
    assert turn.app_server_initialization_confirmed? == true
    assert turn.app_server_thread_start_confirmed? == true
    assert turn.app_server_turn_start_confirmed? == true
    assert turn.app_server_transport == "app_server"

    assert turn.app_server_jsonrpc_methods == [
             "initialize",
             "initialized",
             "thread/start",
             "turn/start"
           ]

    assert turn.app_server_cwd_validation_confirmed? == true
    assert turn.provider_session_id == "codex-provider-thread-99"
    assert turn.provider_turn_id == "codex-provider-turn-99"

    assert turn.app_server_lower_request_ref ==
             "lower-request://jido-run-codex-protocol/codex.session.turn"

    assert turn.app_server_lower_receipt_ref ==
             "lower-receipt://jido-attempt-codex-protocol/codex.session.turn/succeeded"

    assert projection.extensions["codex_app_server_protocol"] == %{
             "confirmed?" => true,
             "transport" => "app_server",
             "jsonrpc_methods" => ["initialize", "initialized", "thread/start", "turn/start"],
             "initialization_confirmed?" => true,
             "thread_start_confirmed?" => true,
             "turn_start_confirmed?" => true,
             "cwd_validation_confirmed?" => true,
             "command_launch_owner" => "lower_runtime",
             "timeout_policy_owner" => "lower_runtime",
             "provider_session_id" => "codex-provider-thread-99",
             "provider_turn_id" => "codex-provider-turn-99",
             "runtime_control_session_ref" => "runtime-session://asm-session-99",
             "lower_request_ref" => "lower-request://jido-run-codex-protocol/codex.session.turn",
             "lower_receipt_ref" =>
               "lower-receipt://jido-attempt-codex-protocol/codex.session.turn/succeeded"
           }

    assert Enum.any?(projection.runtime_events, fn event ->
             event.event_kind == "codex.app_server.protocol.confirmed" and
               event.session_ref == "runtime-session://asm-session-99" and
               event.extensions.jsonrpc_methods == [
                 "initialize",
                 "initialized",
                 "thread/start",
                 "turn/start"
               ]
           end)
  end

  test "Codex agent runtime projects app-server event stream evidence" do
    attrs = %{
      tenant_ref: "tenant://sample-app",
      installation_ref: "installation://sample-app/codex",
      subject_ref: "subject://sample-app/codex-events",
      run_ref: "run://sample-app/codex-events",
      trace_id: "trace://sample-app/codex-events",
      idempotency_key: "idem-codex-events",
      authority_context_ref: "authority-context://sample-app/codex-events",
      max_turns: 2,
      continuation_policy: %{mode: "until_max_turns", active_state?: true}
    }

    invoke_fun = fn capability_id, input, opts ->
      turn_index = Process.get(:codex_event_stream_turn_index, 0) + 1
      Process.put(:codex_event_stream_turn_index, turn_index)
      send(self(), {:codex_event_stream_invoke, turn_index, capability_id, input, opts})

      {:ok,
       %{
         run: %{run_id: "jido-run-codex-events-#{turn_index}"},
         attempt: %{attempt_id: "jido-attempt-codex-events-#{turn_index}"},
         output: %{
           provider_session_id: "codex-provider-events",
           provider_turn_id: "codex-provider-events-#{turn_index}",
           status: if(turn_index == 1, do: :completed, else: :failed)
         },
         events: codex_event_stream_fixture(turn_index)
       }}
    end

    on_exit(fn -> Process.delete(:codex_event_stream_turn_index) end)

    assert {:ok, projection} =
             CodexAgentRuntime.run(attrs,
               invoke_fun: invoke_fun,
               connection_id: "conn-codex",
               start_runtime_router?: false,
               register_connector?: false
             )

    assert_received {:codex_event_stream_invoke, 1, "codex.session.turn", _first_input, _opts}
    assert_received {:codex_event_stream_invoke, 2, "codex.session.turn", _second_input, _opts}

    assert projection.status == "failed"

    event_stream = projection.extensions["codex_event_stream"]
    assert event_stream["confirmed?"] == true
    assert event_stream["event_count"] == 12
    assert event_stream["terminal_status"] == "failed"
    assert event_stream["completed_event_count"] == 1
    assert event_stream["failed_event_count"] == 1
    assert event_stream["cancelled_event_count"] == 1
    assert event_stream["malformed_event_count"] == 1
    assert event_stream["timeout_event_count"] == 1
    assert event_stream["approval_event_count"] == 2
    assert event_stream["approval_required_count"] == 1
    assert event_stream["approval_auto_approved_count"] == 1
    assert event_stream["user_input_event_count"] == 2
    assert event_stream["user_input_required_count"] == 1
    assert event_stream["user_input_auto_answered_count"] == 1

    assert event_stream["token_usage"] == %{
             "input_tokens" => 10,
             "output_tokens" => 4,
             "total_tokens" => 14,
             "source" => "thread_token_usage_total"
           }

    assert event_stream["rate_limits_present?"] == true
    assert event_stream["rate_limit_id"] == "codex"
    assert event_stream["rate_limit_primary_remaining"] == 90
    assert event_stream["rate_limit_primary_limit"] == 100

    assert event_stream["last_message"] == %{
             "event_kind" => "codex.agent_message.updated",
             "summary" => "codex agent message updated",
             "body_redacted?" => true,
             "body_included?" => false
           }

    event_kinds = Enum.map(projection.runtime_events, & &1.event_kind)

    assert "codex.turn.completed" in event_kinds
    assert "codex.turn.failed" in event_kinds
    assert "codex.turn.cancelled" in event_kinds
    assert "codex.protocol.malformed" in event_kinds
    assert "codex.turn.timeout" in event_kinds
    assert "codex.approval.required" in event_kinds
    assert "codex.approval.auto_approved" in event_kinds
    assert "codex.user_input.required" in event_kinds
    assert "codex.user_input.auto_answered" in event_kinds
    assert "codex.token_usage.updated" in event_kinds
    assert "codex.rate_limits.updated" in event_kinds
    assert "codex.agent_message.updated" in event_kinds

    assert List.last(projection.runtime_events).message_summary == "codex session turn failed"
    refute inspect(projection) =~ "STREAM_BODY_DO_NOT_EXPOSE"
  end

  test "Codex agent runtime derives run token totals from accepted absolute snapshots" do
    attrs = %{
      tenant_ref: "tenant://sample-app",
      installation_ref: "installation://sample-app/codex",
      subject_ref: "subject://sample-app/codex-token-accounting",
      run_ref: "run://sample-app/codex-token-accounting",
      trace_id: "trace://sample-app/codex-token-accounting",
      idempotency_key: "idem-codex-token-accounting",
      authority_context_ref: "authority-context://sample-app/codex-token-accounting",
      max_turns: 2,
      continuation_policy: %{mode: "until_max_turns", active_state?: true}
    }

    invoke_fun = fn capability_id, input, opts ->
      turn_index = Process.get(:codex_token_accounting_turn_index, 0) + 1
      Process.put(:codex_token_accounting_turn_index, turn_index)
      send(self(), {:codex_token_accounting_invoke, turn_index, capability_id, input, opts})

      {:ok,
       %{
         run: %{run_id: "jido-run-codex-token-accounting-#{turn_index}"},
         attempt: %{attempt_id: "jido-attempt-codex-token-accounting-#{turn_index}"},
         output: %{
           provider_session_id: "codex-provider-token-accounting",
           provider_turn_id: "codex-provider-token-accounting-#{turn_index}",
           status: :completed
         },
         events: codex_token_accounting_fixture(turn_index)
       }}
    end

    on_exit(fn -> Process.delete(:codex_token_accounting_turn_index) end)

    assert {:ok, projection} =
             CodexAgentRuntime.run(attrs,
               invoke_fun: invoke_fun,
               connection_id: "conn-codex",
               start_runtime_router?: false,
               register_connector?: false
             )

    assert_received {:codex_token_accounting_invoke, 1, "codex.session.turn", _first_input, _opts}

    assert_received {:codex_token_accounting_invoke, 2, "codex.session.turn", _second_input,
                     _opts}

    assert projection.status == "completed"

    assert projection.token_totals == %{
             total_input_tokens: 12,
             total_output_tokens: 5,
             total_tokens: 17,
             cached_input_tokens: 0,
             source: "runtime:event:codex-token-accounting"
           }

    assert projection.extensions["codex_token_accounting"] == %{
             "confirmed?" => true,
             "source" => "runtime:event:codex-token-accounting",
             "accepted_snapshot_count" => 2,
             "ignored_snapshot_count" => 1,
             "scope_count" => 1,
             "last_scope_ref" => "codex-thread://thread-token-accounting",
             "last_snapshot_source" => "token_count_total_token_usage",
             "total_input_tokens" => 12,
             "total_output_tokens" => 5,
             "total_tokens" => 17,
             "cached_input_tokens" => 0
           }

    refute inspect(projection.token_totals) =~ "9999"
    refute inspect(projection.extensions["codex_token_accounting"]) =~ "9999"
  end

  test "Codex agent runtime accepts absolute token totals from preserved app server usage events" do
    attrs = %{
      tenant_ref: "tenant://sample-app",
      installation_ref: "installation://sample-app/codex",
      subject_ref: "subject://sample-app/codex-app-server-usage",
      run_ref: "run://sample-app/codex-app-server-usage",
      trace_id: "trace://sample-app/codex-app-server-usage",
      idempotency_key: "idem-codex-app-server-usage",
      authority_context_ref: "authority-context://sample-app/codex-app-server-usage",
      max_turns: 1,
      continuation_policy: %{mode: "single_turn", active_state?: false}
    }

    invoke_fun = fn capability_id, input, opts ->
      send(self(), {:codex_app_server_usage_invoke, capability_id, input, opts})

      {:ok,
       %{
         run: %{run_id: "jido-run-codex-app-server-usage"},
         attempt: %{attempt_id: "jido-attempt-codex-app-server-usage"},
         output: %{
           provider_session_id: "codex-provider-app-server-usage",
           provider_turn_id: "codex-provider-app-server-usage-1",
           status: :completed
         },
         events: [
           %{
             type: "raw",
             payload: %{
               "stream" => "codex_usage",
               "content" => %{
                 "type" => "thread/tokenUsage/updated",
                 "thread_id" => "thread-app-server-usage",
                 "usage" => %{
                   "input_tokens" => 7,
                   "output_tokens" => 3,
                   "total_tokens" => 10
                 },
                 "delta" => %{
                   "input_tokens" => 9999,
                   "output_tokens" => 9999,
                   "total_tokens" => 9999
                 }
               },
               "metadata" => %{"usage_scope" => "absolute"}
             }
           },
           %{
             type: "raw",
             payload: %{
               "stream" => "telemetry",
               "content" => %{
                 "usage" => %{
                   "input_tokens" => 9999,
                   "output_tokens" => 9999,
                   "total_tokens" => 9999
                 }
               }
             }
           }
         ]
       }}
    end

    assert {:ok, projection} =
             CodexAgentRuntime.run(attrs,
               invoke_fun: invoke_fun,
               connection_id: "conn-codex",
               start_runtime_router?: false,
               register_connector?: false
             )

    assert_received {:codex_app_server_usage_invoke, "codex.session.turn", _input, _opts}

    assert projection.token_totals == %{
             total_input_tokens: 7,
             total_output_tokens: 3,
             total_tokens: 10,
             cached_input_tokens: 0,
             source: "runtime:event:codex-token-accounting"
           }

    assert projection.extensions["codex_token_accounting"]["last_scope_ref"] ==
             "codex-thread://thread-app-server-usage"

    assert projection.extensions["codex_token_accounting"]["last_snapshot_source"] ==
             "codex_usage_absolute"

    refute inspect(projection.token_totals) =~ "9999"
  end

  test "Codex agent runtime fetches lower result usage as turn-completed token fallback" do
    attrs = %{
      tenant_ref: "tenant://sample-app",
      installation_ref: "installation://sample-app/codex",
      subject_ref: "subject://sample-app/codex-result-usage",
      run_ref: "run://sample-app/codex-result-usage",
      trace_id: "trace://sample-app/codex-result-usage",
      idempotency_key: "idem-codex-result-usage",
      authority_context_ref: "authority-context://sample-app/codex-result-usage",
      max_turns: 1,
      continuation_policy: %{mode: "single_turn", active_state?: false}
    }

    invoke_fun = fn capability_id, input, opts ->
      send(self(), {:codex_result_usage_invoke, capability_id, input, opts})

      {:ok,
       %{
         run: %{run_id: "jido-run-codex-result-usage"},
         attempt: %{attempt_id: "jido-attempt-codex-result-usage"},
         output: %{
           provider_session_id: "codex-provider-result-usage",
           provider_turn_id: "codex-provider-result-usage-1",
           status: :completed
         }
       }}
    end

    events_fun = fn "jido-run-codex-result-usage" ->
      [
        %{
          type: "result",
          stream: :control,
          session_id: "runtime-session-codex-result-usage",
          runtime_ref_id: "runtime-session-codex-result-usage",
          payload: %{
            "status" => "completed",
            "stop_reason" => "end_turn",
            "output" => %{
              "usage" => %{
                "input_tokens" => 43,
                "output_tokens" => 7,
                "total_tokens" => 0
              }
            }
          }
        }
      ]
    end

    assert {:ok, projection} =
             CodexAgentRuntime.run(attrs,
               invoke_fun: invoke_fun,
               events_fun: events_fun,
               connection_id: "conn-codex",
               start_runtime_router?: false,
               register_connector?: false
             )

    assert_received {:codex_result_usage_invoke, "codex.session.turn", _input, _opts}

    assert projection.token_totals == %{
             total_input_tokens: 43,
             total_output_tokens: 7,
             total_tokens: 50,
             cached_input_tokens: 0,
             source: "runtime:event:codex-token-accounting"
           }

    assert projection.extensions["codex_event_stream"]["completed_event_count"] == 1

    assert projection.extensions["codex_event_stream"]["token_usage"]["source"] ==
             "turn_completed_usage"

    assert projection.extensions["codex_token_accounting"]["last_snapshot_source"] ==
             "turn_completed_usage"
  end

  test "Codex agent runtime does not double count turn result usage when explicit usage events exist" do
    attrs = %{
      tenant_ref: "tenant://sample-app",
      installation_ref: "installation://sample-app/codex",
      subject_ref: "subject://sample-app/codex-result-dedupe",
      run_ref: "run://sample-app/codex-result-dedupe",
      trace_id: "trace://sample-app/codex-result-dedupe",
      idempotency_key: "idem-codex-result-dedupe",
      authority_context_ref: "authority-context://sample-app/codex-result-dedupe",
      max_turns: 1,
      continuation_policy: %{mode: "single_turn", active_state?: false}
    }

    invoke_fun = fn _capability_id, _input, _opts ->
      {:ok,
       %{
         run: %{run_id: "jido-run-codex-result-dedupe"},
         attempt: %{attempt_id: "jido-attempt-codex-result-dedupe"},
         output: %{
           provider_session_id: "codex-provider-result-dedupe",
           provider_turn_id: "codex-provider-result-dedupe-1",
           status: :completed
         }
       }}
    end

    events_fun = fn "jido-run-codex-result-dedupe" ->
      [
        %{
          type: "raw",
          payload: %{
            "stream" => "codex_usage",
            "content" => %{
              "type" => "thread/tokenUsage/updated",
              "thread_id" => "thread-result-dedupe",
              "usage" => %{
                "input_tokens" => 12,
                "output_tokens" => 5,
                "total_tokens" => 17
              }
            },
            "metadata" => %{"usage_scope" => "absolute"}
          }
        },
        %{
          type: "result",
          stream: :control,
          session_id: "runtime-session-codex-result-dedupe",
          runtime_ref_id: "runtime-session-codex-result-dedupe",
          payload: %{
            "status" => "completed",
            "output" => %{
              "usage" => %{
                "input_tokens" => 12,
                "output_tokens" => 5,
                "total_tokens" => 0
              }
            }
          }
        }
      ]
    end

    assert {:ok, projection} =
             CodexAgentRuntime.run(attrs,
               invoke_fun: invoke_fun,
               events_fun: events_fun,
               connection_id: "conn-codex",
               start_runtime_router?: false,
               register_connector?: false
             )

    assert projection.token_totals == %{
             total_input_tokens: 12,
             total_output_tokens: 5,
             total_tokens: 17,
             cached_input_tokens: 0,
             source: "runtime:event:codex-token-accounting"
           }

    assert projection.extensions["codex_token_accounting"]["accepted_snapshot_count"] == 1

    assert projection.extensions["codex_token_accounting"]["last_snapshot_source"] ==
             "codex_usage_absolute"
  end

  test "Codex agent runtime marks active no-event runs stalled from run start timeout" do
    now = ~U[2026-05-14 00:10:00Z]
    started_at = ~U[2026-05-14 00:09:55Z]

    attrs =
      codex_agent_attrs()
      |> Map.merge(%{
        subject_ref: "subject://neutral/codex-no-event-stall",
        run_ref: "run://neutral/codex-no-event-stall",
        attempt_ref: "attempt://neutral/codex-no-event-stall/1",
        worker_ref: "worker://neutral/codex-no-event-stall",
        workspace_ref: "workspace://neutral/codex-no-event-stall",
        started_at: started_at,
        timeout_policy: %{stall_timeout_ms: 1_000}
      })

    invoke_fun = fn capability_id, input, opts ->
      send(self(), {:codex_no_event_stall_invoke, capability_id, input, opts})

      {:ok,
       %{
         run: %{run_id: "jido-run-codex-no-event-stall", started_at: started_at},
         attempt: %{attempt_id: "jido-attempt-codex-no-event-stall", started_at: started_at},
         output: %{
           provider_session_id: "provider-session-no-event-stall",
           provider_turn_id: "provider-turn-no-event-stall",
           status: :running
         },
         events: []
       }}
    end

    assert {:ok, projection} =
             CodexAgentRuntime.run(attrs,
               invoke_fun: invoke_fun,
               now: now,
               connection_id: "conn-codex",
               start_runtime_router?: false,
               register_connector?: false
             )

    assert_received {:codex_no_event_stall_invoke, "codex.session.turn", _input, _opts}
    assert projection.status == "stalled"
    assert projection.terminal_state == "stalled"

    assert %{} = stall = projection.stall_decision
    assert stall.runtime_state == "stalled"
    assert stall.status_reason == "stall_timeout"
    assert stall.elapsed_ms == 5_000
    assert stall.stall_timeout_ms == 1_000
    assert stall.last_activity_at == started_at
    assert stall.activity_source == "started_at"
    assert stall.safe_action == "terminate_lower_and_schedule_retry"
    assert stall.workflow_signal == "operator.cancel"
    assert stall.cancel_lower_run? == true
    assert stall.cleanup_workspace? == false
    assert stall.attempt_ref == "attempt://neutral/codex-no-event-stall/1"
    assert stall.session_ref == "session://codex/provider-session-no-event-stall"

    assert stall.retry.status == "scheduled"
    assert stall.retry.reason == "stall_timeout"
    assert stall.retry.scheduled_at == now
    assert stall.retry.due_at == DateTime.add(now, 10_000, :millisecond)
    assert stall.retry.delay_ms == 10_000
    assert stall.retry.delay_type == "failure_backoff"
    assert stall.retry.metadata.safe_action == "terminate_lower_and_schedule_retry"
    assert stall.diagnostic.code == "runtime_stall_timeout"
  end

  test "Codex agent runtime leaves active runs unstalled when stall timeout is disabled" do
    for timeout <- [0, -1] do
      attrs =
        codex_agent_attrs()
        |> Map.merge(%{
          run_ref: "run://neutral/codex-disabled-stall-#{timeout}",
          started_at: ~U[2026-05-14 00:00:00Z],
          timeout_policy: %{stall_timeout_ms: timeout}
        })

      invoke_fun = fn _capability_id, _input, _opts ->
        {:ok,
         %{
           run: %{run_id: "jido-run-disabled-stall-#{timeout}"},
           attempt: %{attempt_id: "jido-attempt-disabled-stall-#{timeout}"},
           output: %{provider_session_id: "provider-disabled-stall-#{timeout}", status: :running},
           events: []
         }}
      end

      assert {:ok, projection} =
               CodexAgentRuntime.run(attrs,
                 invoke_fun: invoke_fun,
                 now: ~U[2026-05-14 00:10:00Z],
                 connection_id: "conn-codex",
                 start_runtime_router?: false,
                 register_connector?: false
               )

      assert projection.status == "running"
      refute Map.has_key?(projection, :stall_decision)
    end
  end

  test "Codex agent runtime marks active runs stalled from stale lower event timestamp" do
    now = ~U[2026-05-14 00:10:00Z]
    started_at = ~U[2026-05-14 00:00:00Z]
    last_event_at = ~U[2026-05-14 00:04:30Z]

    attrs =
      codex_agent_attrs()
      |> Map.merge(%{
        subject_ref: "subject://neutral/codex-stale-event-stall",
        run_ref: "run://neutral/codex-stale-event-stall",
        attempt_ref: "attempt://neutral/codex-stale-event-stall/1",
        started_at: started_at,
        timeout_policy: %{stall_timeout_ms: 300_000}
      })

    invoke_fun = fn _capability_id, _input, _opts ->
      {:ok,
       %{
         run: %{run_id: "jido-run-codex-stale-event-stall", started_at: started_at},
         attempt: %{attempt_id: "jido-attempt-codex-stale-event-stall"},
         output: %{
           provider_session_id: "provider-session-stale-event-stall",
           provider_turn_id: "provider-turn-stale-event-stall",
           status: :running
         },
         events: [
           %{
             event_id: "event-codex-stale-agent-message",
             type: "codex.app_server.message",
             observed_at: last_event_at,
             payload: %{
               "method" => "codex/event/agent_message_delta",
               "params" => %{"msg" => %{"delta" => "STALE_BODY_DO_NOT_EXPOSE"}}
             }
           }
         ]
       }}
    end

    assert {:ok, projection} =
             CodexAgentRuntime.run(attrs,
               invoke_fun: invoke_fun,
               now: now,
               connection_id: "conn-codex",
               start_runtime_router?: false,
               register_connector?: false
             )

    assert projection.status == "stalled"
    assert projection.stall_decision.elapsed_ms == 330_000
    assert projection.stall_decision.stall_timeout_ms == 300_000
    assert projection.stall_decision.last_activity_at == last_event_at
    assert projection.stall_decision.activity_source == "last_runtime_event_at"

    assert projection.stall_decision.session_ref ==
             "session://codex/provider-session-stale-event-stall"

    refute inspect(projection) =~ "STALE_BODY_DO_NOT_EXPOSE"
  end

  test "Codex agent runtime does not stall terminal runs even when activity is stale" do
    for status <- [:completed, :failed, :cancelled, :timeout] do
      attrs =
        codex_agent_attrs()
        |> Map.merge(%{
          run_ref: "run://neutral/codex-terminal-no-stall-#{status}",
          started_at: ~U[2026-05-14 00:00:00Z],
          timeout_policy: %{stall_timeout_ms: 1_000}
        })

      invoke_fun = fn _capability_id, _input, _opts ->
        {:ok,
         %{
           run: %{run_id: "jido-run-terminal-no-stall-#{status}"},
           attempt: %{attempt_id: "jido-attempt-terminal-no-stall-#{status}"},
           output: %{provider_session_id: "provider-terminal-no-stall-#{status}", status: status},
           events: []
         }}
      end

      assert {:ok, projection} =
               CodexAgentRuntime.run(attrs,
                 invoke_fun: invoke_fun,
                 now: ~U[2026-05-14 00:10:00Z],
                 connection_id: "conn-codex",
                 start_runtime_router?: false,
                 register_connector?: false
               )

      assert projection.status == Atom.to_string(status)
      refute Map.has_key?(projection, :stall_decision)
    end
  end

  test "Codex agent runtime gates lower invocation behind before-run workspace hooks" do
    parent = self()
    workspace_root = tmp_dir("codex-before-run")

    attrs =
      codex_agent_attrs()
      |> Map.put(:workspace_ref, "workspace://codex-before-run")
      |> Map.put(:run_ref, "run://neutral/codex-before-run")

    hook_specs = [
      %{
        "hook_ref" => "preflight",
        "stage" => "before_run",
        "timeout_ms" => 100,
        "env_refs" => ["env://SAFE_TOKEN"],
        "attrs" => %{"command" => "echo ready"}
      }
    ]

    prepare_workspace_fun = fn root ->
      send(parent, {:codex_phase, :prepare_workspace, root})
      File.mkdir_p(root)
    end

    hook_runner = fn hook, context ->
      send(
        parent,
        {:codex_phase, :before_run_hook, hook.hook_ref, context.cwd, context.env_refs,
         Map.has_key?(context, :env), context.run_ref, context.workflow_ref}
      )

      {:ok, %{stdout: "ready secret-token", stderr: ""}}
    end

    invoke_fun = fn capability_id, input, opts ->
      send(parent, {:codex_phase, :invoke, capability_id, input.cwd, opts})

      {:ok,
       %{
         run: %{run_id: "jido-run-before-run"},
         attempt: %{attempt_id: "jido-attempt-before-run"},
         output: %{provider_session_id: "codex-session-before-run", status: :completed}
       }}
    end

    assert {:ok, projection} =
             CodexAgentRuntime.run(attrs,
               cwd: workspace_root,
               workspace_hook_specs: hook_specs,
               workspace_hook_runner: hook_runner,
               hook_redactions: ["secret-token"],
               prepare_workspace_fun: prepare_workspace_fun,
               invoke_fun: invoke_fun,
               connection_id: "conn-codex",
               start_runtime_router?: false,
               register_connector?: false
             )

    assert_receive {:codex_phase, :prepare_workspace, ^workspace_root}

    assert_receive {:codex_phase, :before_run_hook, "preflight", ^workspace_root,
                    ["env://SAFE_TOKEN"], false, "run://neutral/codex-before-run",
                    "workflow://codex-agent-runtime/run-neutral-codex-before-run"}

    assert_receive {:codex_phase, :invoke, "codex.session.turn", ^workspace_root, invoke_opts}
    assert Keyword.fetch!(invoke_opts, :connection_id) == "conn-codex"

    assert [hook_event] =
             Enum.filter(
               projection.runtime_events,
               &(&1.event_kind == "workspace.hook.before_run")
             )

    assert hook_event.event_seq == 0
    assert hook_event.extensions.path_redacted? == true
    assert [receipt] = hook_event.extensions.hook_receipts
    assert receipt.stage == :before_run
    assert receipt.status == :succeeded
    assert receipt.result.stdout == "ready [REDACTED]"
    refute Map.has_key?(receipt, :cwd)
  end

  test "Codex agent runtime blocks lower invocation when before-run hook fails" do
    parent = self()

    attrs =
      codex_agent_attrs()
      |> Map.put(:workspace_ref, "workspace://codex-before-run-failure")
      |> Map.put(:run_ref, "run://neutral/codex-before-run-failure")

    invoke_fun = fn _capability_id, _input, _opts ->
      send(parent, :unexpected_codex_invoke)
      {:ok, %{}}
    end

    assert {:error, {:hook_failed, receipt}} =
             CodexAgentRuntime.run(attrs,
               cwd: tmp_dir("codex-before-run-failure"),
               workspace_hook_specs: [
                 %{"hook_ref" => "preflight", "stage" => "before_run", "timeout_ms" => 100}
               ],
               workspace_hook_runner: fn _hook, _context ->
                 {:error, %{stdout: "blocked secret-token", stderr: "nope"}}
               end,
               hook_redactions: ["secret-token"],
               invoke_fun: invoke_fun,
               connection_id: "conn-codex",
               start_runtime_router?: false,
               register_connector?: false
             )

    refute_received :unexpected_codex_invoke
    assert receipt.stage == :before_run
    assert receipt.status == :failed
    assert receipt.fatal? == true
    assert receipt.action == :halt
    assert receipt.reason.stdout == "blocked [REDACTED]"
    assert receipt.reason.stderr == "nope"
  end

  test "Codex agent runtime blocks lower invocation when before-run hook times out" do
    parent = self()

    attrs =
      codex_agent_attrs()
      |> Map.put(:workspace_ref, "workspace://codex-before-run-timeout")
      |> Map.put(:run_ref, "run://neutral/codex-before-run-timeout")

    invoke_fun = fn _capability_id, _input, _opts ->
      send(parent, :unexpected_codex_invoke)
      {:ok, %{}}
    end

    assert {:error, {:hook_timeout, receipt}} =
             CodexAgentRuntime.run(attrs,
               cwd: tmp_dir("codex-before-run-timeout"),
               workspace_hook_specs: [
                 %{"hook_ref" => "preflight", "stage" => "before_run", "timeout_ms" => 1}
               ],
               workspace_hook_runner: fn _hook, _context ->
                 Process.sleep(50)
                 :ok
               end,
               invoke_fun: invoke_fun,
               connection_id: "conn-codex",
               start_runtime_router?: false,
               register_connector?: false
             )

    refute_received :unexpected_codex_invoke
    assert receipt.stage == :before_run
    assert receipt.status == :timed_out
    assert receipt.fatal? == true
    assert receipt.action == :halt
  end

  test "Codex agent runtime runs after-run hook after successful lower invocation" do
    parent = self()
    workspace_root = tmp_dir("codex-after-run-success")

    attrs =
      codex_agent_attrs()
      |> Map.put(:workspace_ref, "workspace://codex-after-run-success")
      |> Map.put(:run_ref, "run://neutral/codex-after-run-success")

    invoke_fun = fn capability_id, input, _opts ->
      send(parent, {:codex_phase, :invoke, capability_id, input.cwd})

      {:ok,
       %{
         run: %{run_id: "jido-run-after-run"},
         attempt: %{attempt_id: "jido-attempt-after-run"},
         output: %{provider_session_id: "codex-session-after-run", status: :completed}
       }}
    end

    hook_runner = fn hook, context ->
      send(parent, {:codex_phase, :after_run_hook, hook.hook_ref, context.cwd})
      {:error, %{stdout: "cleanup failed secret-token", stderr: "kept as receipt"}}
    end

    assert {:ok, projection} =
             CodexAgentRuntime.run(attrs,
               cwd: workspace_root,
               workspace_hook_specs: [
                 %{"hook_ref" => "cleanup", "stage" => "after_run", "timeout_ms" => 100}
               ],
               workspace_hook_runner: hook_runner,
               hook_redactions: ["secret-token"],
               invoke_fun: invoke_fun,
               connection_id: "conn-codex",
               start_runtime_router?: false,
               register_connector?: false
             )

    assert_receive {:codex_phase, :invoke, "codex.session.turn", ^workspace_root}
    assert_receive {:codex_phase, :after_run_hook, "cleanup", ^workspace_root}
    assert projection.status == "completed"

    event_kinds = Enum.map(projection.runtime_events, & &1.event_kind)

    assert event_kinds == [
             "codex.first_prompt.confirmed",
             "run.terminal",
             "workspace.hook.after_run"
           ]

    after_event = List.last(projection.runtime_events)
    assert after_event.event_seq == 3
    assert [receipt] = after_event.extensions.hook_receipts
    assert receipt.stage == :after_run
    assert receipt.status == :failed
    assert receipt.action == :continue
    assert receipt.reason.stdout == "cleanup failed [REDACTED]"
  end

  test "Codex agent runtime preserves lower failure and cancellation when after-run hook succeeds" do
    for reason <- [:provider_failed, :cancelled] do
      parent = self()
      expected_run_ref = "run://neutral/codex-after-run-#{reason}"

      attrs =
        codex_agent_attrs()
        |> Map.put(:workspace_ref, "workspace://codex-after-run-#{reason}")
        |> Map.put(:run_ref, expected_run_ref)

      invoke_fun = fn _capability_id, _input, _opts ->
        send(parent, {:codex_phase, :invoke_failed, reason})
        {:error, reason}
      end

      hook_runner = fn hook, context ->
        send(parent, {:codex_phase, :after_run_hook, reason, hook.hook_ref, context.run_ref})
        {:ok, %{stdout: "after #{reason}", stderr: ""}}
      end

      assert {:error, {:codex_agent_runtime_failed, ^reason, evidence}} =
               CodexAgentRuntime.run(attrs,
                 cwd: tmp_dir("codex-after-run-#{reason}"),
                 workspace_hook_specs: [
                   %{"hook_ref" => "cleanup", "stage" => "after_run", "timeout_ms" => 100}
                 ],
                 workspace_hook_runner: hook_runner,
                 invoke_fun: invoke_fun,
                 connection_id: "conn-codex",
                 start_runtime_router?: false,
                 register_connector?: false
               )

      assert_receive {:codex_phase, :invoke_failed, ^reason}

      assert_receive {:codex_phase, :after_run_hook, ^reason, "cleanup", ^expected_run_ref}

      assert [receipt] = evidence.after_run_hook_receipts
      assert receipt.stage == :after_run
      assert receipt.status == :succeeded
      assert receipt.result.stdout == "after #{reason}"
    end
  end

  test "Codex agent runtime runs after-run hook after before-run gate failure" do
    parent = self()

    attrs =
      codex_agent_attrs()
      |> Map.put(:workspace_ref, "workspace://codex-after-run-before-failure")
      |> Map.put(:run_ref, "run://neutral/codex-after-run-before-failure")

    invoke_fun = fn _capability_id, _input, _opts ->
      send(parent, :unexpected_codex_invoke)
      {:ok, %{}}
    end

    hook_runner = fn
      %{stage: :before_run}, _context ->
        {:error, %{stdout: "preflight failed", stderr: ""}}

      %{stage: :after_run}, _context ->
        Process.sleep(50)
        :ok
    end

    assert {:error,
            {:codex_agent_runtime_failed, {:hook_failed, before_receipt},
             %{after_run_hook_receipts: [after_receipt]}}} =
             CodexAgentRuntime.run(attrs,
               cwd: tmp_dir("codex-after-run-before-failure"),
               workspace_hook_specs: [
                 %{"hook_ref" => "preflight", "stage" => "before_run", "timeout_ms" => 100},
                 %{"hook_ref" => "cleanup", "stage" => "after_run", "timeout_ms" => 1}
               ],
               workspace_hook_runner: hook_runner,
               invoke_fun: invoke_fun,
               connection_id: "conn-codex",
               start_runtime_router?: false,
               register_connector?: false
             )

    refute_received :unexpected_codex_invoke
    assert before_receipt.stage == :before_run
    assert before_receipt.status == :failed
    assert after_receipt.stage == :after_run
    assert after_receipt.status == :timed_out
    assert after_receipt.action == :continue
  end

  test "Codex agent runtime installs credentials in the run tenant and requests connector sandbox" do
    attrs = %{
      tenant_ref: "tenant://sample-app-live",
      installation_ref: "installation://sample-app/codex",
      subject_ref: "subject://sample-app/codex",
      run_ref: "run://sample-app/codex-live",
      trace_id: "trace://sample-app/codex-live",
      idempotency_key: "idem-codex-live",
      authority_context_ref: "authority-context://sample-app/codex-live",
      source_ref: "actor://sample-app/operator"
    }

    start_install_fun = fn connector_id, tenant_id, install_attrs ->
      send(self(), {:codex_start_install, connector_id, tenant_id, install_attrs})

      {:ok,
       %{
         install: %{install_id: "install-codex-live"},
         connection: %{connection_id: "conn-codex-live"}
       }}
    end

    complete_install_fun = fn install_id, complete_attrs ->
      send(self(), {:codex_complete_install, install_id, complete_attrs})

      {:ok,
       %{
         connection: %{connection_id: "conn-codex-live"}
       }}
    end

    invoke_fun = fn capability_id, input, opts ->
      send(self(), {:codex_live_invoke, capability_id, input, opts})

      {:ok,
       %{
         run: %{run_id: "jido-run-codex-live"},
         attempt: %{attempt_id: "jido-attempt-codex-live"},
         output: %{
           text: "Sample App headless Codex live path is operational.",
           provider_session_id: "codex-provider-session-live",
           status: :completed
         }
       }}
    end

    prepare_workspace_fun = fn workspace_root ->
      send(self(), {:codex_prepare_workspace, workspace_root})
      :ok
    end

    assert {:ok, _projection} =
             CodexAgentRuntime.run(attrs,
               invoke_fun: invoke_fun,
               start_install_fun: start_install_fun,
               complete_install_fun: complete_install_fun,
               prepare_workspace_fun: prepare_workspace_fun,
               start_runtime_router?: false,
               register_connector?: false
             )

    assert_received {:codex_start_install, "codex_cli", "tenant://sample-app-live", install_attrs}

    assert install_attrs.actor_id == "actor://sample-app/operator"

    assert install_attrs.requested_scopes == [
             "session:execute",
             "session:control",
             "session:tools"
           ]

    assert_received {:codex_complete_install, "install-codex-live", complete_attrs}

    assert complete_attrs.granted_scopes == [
             "session:execute",
             "session:control",
             "session:tools"
           ]

    assert_received {:codex_prepare_workspace, "/tmp/jido_codex_cli_workspace"}
    assert_received {:codex_live_invoke, "codex.session.turn", input, opts}
    assert input.cwd == "/tmp/jido_codex_cli_workspace"
    assert input.provider_metadata["skip_git_repo_check"] == true
    refute Map.has_key?(input, :dynamic_tool_manifest)
    assert Keyword.fetch!(opts, :connection_id) == "conn-codex-live"
    assert Keyword.fetch!(opts, :tenant_id) == "tenant://sample-app-live"

    assert Keyword.fetch!(opts, :sandbox) == %{
             level: :strict,
             egress: :restricted,
             approvals: :manual,
             file_scope: "/tmp/jido_codex_cli_workspace",
             allowed_tools: ["codex.session.turn"]
           }
  end

  test "Linear source candidate fetch resolves viewer before assignee-me intake" do
    invocation = authorized_invocation_allowing(["linear.users.get_self", "linear.issues.list"])

    invoke_fun = fn
      "linear.users.get_self", input, _opts ->
        send(self(), {:invoke, "linear.users.get_self", input})
        {:ok, %{output: %{user: %{id: "usr-linear-viewer", name: "Taylor Automation"}}}}

      "linear.issues.list", input, _opts ->
        send(self(), {:invoke, "linear.issues.list", input})
        {:ok, %{output: %{issues: [linear_issue()]}}}
    end

    assert {:ok, result} =
             IntegrationBridge.fetch_linear_candidates(
               invocation,
               source_binding(),
               invoke_fun: invoke_fun
             )

    assert_received {:invoke, "linear.users.get_self", %{}}

    assert_received {:invoke, "linear.issues.list",
                     %{filter: %{assignee_id: "usr-linear-viewer"}}}

    assert result.viewer_resolution.output.user.id == "usr-linear-viewer"
    assert result.viewer_resolution.provider_request_sent? == true
    assert result.viewer_resolution.provider_response_received? == true
    assert is_binary(result.viewer_resolution.lower_request_ref)
    assert is_binary(result.viewer_resolution.lower_receipt_ref)
    assert [%{lifecycle_state: "submitted"}] = result.source_intake.subject_attrs
  end

  test "Linear current-state lookup deduplicates, batches, and preserves requested order" do
    invocation = authorized_invocation_allowing(["linear.issues.list"])

    invoke_fun = fn "linear.issues.list", input, _opts ->
      send(self(), {:invoke, "linear.issues.list", input})

      issues =
        case get_in(input, [:filter, :issue_ids]) do
          ["lin-issue-321", "lin-issue-654"] -> [linear_issue("lin-issue-654"), linear_issue()]
          ["lin-issue-777"] -> []
        end

      {:ok, %{output: %{issues: issues, page_info: %{has_next_page: false}}}}
    end

    assert {:ok, result} =
             IntegrationBridge.fetch_linear_current_issue_states(
               invocation,
               ["lin-issue-321", "lin-issue-654", "lin-issue-321", "lin-issue-777"],
               source_binding(),
               invoke_fun: invoke_fun,
               viewer: %{id: "usr-linear-viewer"},
               page_size: 2
             )

    assert_received {:invoke, "linear.issues.list",
                     %{filter: %{issue_ids: ["lin-issue-321", "lin-issue-654"]}, first: 2}}

    assert_received {:invoke, "linear.issues.list",
                     %{filter: %{issue_ids: ["lin-issue-777"]}, first: 1}}

    assert Enum.map(result.source_current_state.subject_attrs, & &1.provider_external_ref) == [
             "lin-issue-321",
             "lin-issue-654"
           ]

    assert result.source_current_state.missing_issue_ids == ["lin-issue-777"]
    assert result.provider_request_sent? == true
    assert result.provider_response_received? == true
    assert is_binary(result.lower_request_ref)
    assert is_binary(result.lower_receipt_ref)
  end

  test "Linear issue refresh normalizes provider output into source subject attrs" do
    invocation = authorized_invocation_allowing(["linear.issues.retrieve"])

    invoke_fun = fn capability, input, _opts ->
      send(self(), {:invoke, capability, input})
      {:ok, %{output: %{issue: linear_issue()}}}
    end

    assert {:ok, result} =
             IntegrationBridge.refresh_linear_issue(
               invocation,
               "lin-issue-321",
               source_binding(),
               invoke_fun: invoke_fun
             )

    assert_received {:invoke, "linear.issues.retrieve", %{issue_id: "lin-issue-321"}}
    assert result.source_refresh.operation == "linear.issues.retrieve"
    assert result.source_refresh.subject_attrs.provider_external_ref == "lin-issue-321"
  end

  test "Linear source publication emits a public-safe governed publication receipt" do
    invocation = authorized_invocation_allowing(["linear.comments.update"])

    invoke_fun = fn capability, input, _opts ->
      send(self(), {:invoke, capability, input})
      {:ok, %{output: %{success: true, comment: %{id: "comment-1"}}}}
    end

    assert {:ok, result} =
             IntegrationBridge.publish_linear_source(
               invocation,
               %{
                 source_publish_ref: "linear_workpad_review",
                 source_binding_id: "linear-primary",
                 source_ref: "linear://inst-1/issue/ENG-321",
                 comment_id: "comment-1",
                 body: "Ready for review",
                 redaction_manifest_ref: "redaction://linear/workpad"
               },
               invoke_fun: invoke_fun
             )

    assert_received {:invoke, "linear.comments.update", %{comment_id: "comment-1"}}

    receipt = result.source_publication_receipt
    assert receipt.status == "published"
    assert receipt.capability_id == "linear.comments.update"
    assert receipt.lower_runtime_kind == "direct_connector"
    assert receipt.authority_ref == "authority-decision://mock-decision-123"
    assert receipt.connector_manifest_ref == "manifest://jido/connectors/linear@local"
    assert receipt.redaction_manifest_ref == "redaction://linear/workpad"
    assert receipt.workpad_refs == ["linear-comment://comment-1"]
  end

  test "Linear source publication dry-run returns a governed denial receipt without provider dispatch" do
    invocation = authorized_invocation_allowing(["linear.comments.create"])

    invoke_fun = fn _capability, _input, _opts ->
      send(self(), :unexpected_provider_dispatch)
      {:ok, %{}}
    end

    assert {:ok, result} =
             IntegrationBridge.publish_linear_source(
               invocation,
               %{
                 source_publish_ref: "linear_workpad_review",
                 source_binding_id: "linear-primary",
                 source_ref: "linear://inst-1/issue/ENG-321",
                 issue_id: "lin-issue-321",
                 body: "Ready for review",
                 allow_create_fallback?: true
               },
               dry_run?: true,
               credential_redeemed?: true,
               invoke_fun: invoke_fun
             )

    refute_received :unexpected_provider_dispatch
    assert result.provider_request_sent? == false
    assert result.provider_response_received? == false
    assert result.credential_redeemed? == true
    assert %GovernedLowerDenial{} = result.governed_lower_denial
    assert result.lower_denial_ref == result.governed_lower_denial.lower_denial_ref

    receipt = result.source_publication_receipt
    assert receipt.status == "dry_run_denied"
    assert receipt.capability_id == "linear.comments.create"
    assert receipt.lower_denial_ref == result.governed_lower_denial.lower_denial_ref
    assert receipt.provider_request_sent? == false
  end

  test "Linear source publication can create a workpad comment after update miss" do
    invocation =
      authorized_invocation_allowing(["linear.comments.update", "linear.comments.create"])

    invoke_fun = fn
      "linear.comments.update", input, _opts ->
        send(self(), {:invoke, "linear.comments.update", input})
        {:error, %{reason: %{code: "linear.not_found"}}}

      "linear.comments.create", input, _opts ->
        send(self(), {:invoke, "linear.comments.create", input})
        {:ok, %{output: %{success: true, comment: %{id: "comment-created"}}}}
    end

    assert {:ok, result} =
             IntegrationBridge.publish_linear_source(
               invocation,
               %{
                 source_publish_ref: "linear_workpad_review",
                 source_binding_id: "linear-primary",
                 source_ref: "linear://inst-1/issue/ENG-321",
                 issue_id: "lin-issue-321",
                 comment_id: "stale-comment",
                 body: "Ready for review",
                 allow_create_fallback?: true
               },
               invoke_fun: invoke_fun
             )

    assert_received {:invoke, "linear.comments.update", %{comment_id: "stale-comment"}}
    assert_received {:invoke, "linear.comments.create", %{issue_id: "lin-issue-321"}}

    assert result.source_publication_receipt.capability_id == "linear.comments.create"
    assert result.source_publication_receipt.fallback_from == "linear.comments.update"
    assert result.source_publication_receipt.workpad_refs == ["linear-comment://comment-created"]
  end

  test "Linear source publication fallback recognizes missing-comment input errors" do
    invocation =
      authorized_invocation_allowing(["linear.comments.update", "linear.comments.create"])

    invoke_fun = fn
      "linear.comments.update", input, _opts ->
        send(self(), {:invoke, "linear.comments.update", input})

        {:error,
         %{
           reason: %{
             code: "linear.input_error",
             message: "[input_error] Entity not found: Comment"
           }
         }}

      "linear.comments.create", input, _opts ->
        send(self(), {:invoke, "linear.comments.create", input})
        {:ok, %{output: %{success: true, comment: %{id: "comment-created"}}}}
    end

    assert {:ok, result} =
             IntegrationBridge.publish_linear_source(
               invocation,
               %{
                 source_publish_ref: "linear_workpad_review",
                 source_binding_id: "linear-primary",
                 source_ref: "linear://inst-1/issue/ENG-321",
                 issue_id: "lin-issue-321",
                 comment_id: "stale-comment",
                 body: "Ready for review",
                 allow_create_fallback?: true
               },
               invoke_fun: invoke_fun
             )

    assert_received {:invoke, "linear.comments.update", %{comment_id: "stale-comment"}}
    assert_received {:invoke, "linear.comments.create", %{issue_id: "lin-issue-321"}}

    assert result.source_publication_receipt.capability_id == "linear.comments.create"
    assert result.source_publication_receipt.fallback_from == "linear.comments.update"
  end

  test "Linear issue state publication resolves state names before governed update" do
    invocation =
      authorized_invocation_allowing(["linear.workflow_states.list", "linear.issues.update"])

    invoke_fun = fn
      "linear.workflow_states.list", input, _opts ->
        send(self(), {:invoke, "linear.workflow_states.list", input})

        {:ok,
         %{
           output: %{
             workflow_states: [
               %{id: "state-backlog", name: "Backlog", team: %{id: "team-linear"}},
               %{id: "state-done", name: "Done", team: %{id: "team-linear"}}
             ],
             page_info: %{has_next_page: false}
           }
         }}

      "linear.issues.update", input, _opts ->
        send(self(), {:invoke, "linear.issues.update", input})

        {:ok,
         %{
           output: %{
             success: true,
             issue: %{id: "lin-issue-321", identifier: "ENG-321"}
           }
         }}
    end

    assert {:ok, result} =
             IntegrationBridge.update_linear_issue_state(
               invocation,
               %{
                 source_publish_ref: "linear_state_update",
                 source_binding_id: "linear-primary",
                 source_ref: "linear://inst-1/issue/ENG-321",
                 issue_id: "lin-issue-321",
                 state_name: "Done",
                 team_id: "team-linear"
               },
               invoke_fun: invoke_fun
             )

    assert_received {:invoke, "linear.workflow_states.list",
                     %{filter: %{state_names: ["Done"], team_id: "team-linear"}, first: 10}}

    assert_received {:invoke, "linear.issues.update",
                     %{issue_id: "lin-issue-321", state_id: "state-done"}}

    receipt = result.source_publication_receipt
    assert receipt.status == "published"
    assert receipt.capability_id == "linear.issues.update"
    assert receipt.issue_id == "lin-issue-321"
    assert receipt.state_id == "state-done"
    assert receipt.state_name == "Done"
  end

  test "GitHub PR creation uses governed direct connector dispatch" do
    invocation = authorized_invocation_allowing(["github.pr.create"])

    invoke_fun = fn capability, input, opts ->
      send(self(), {:invoke, capability, input, opts})
      {:ok, %{output: github_pr(), artifact_refs: ["artifact://github/pr-create"]}}
    end

    assert {:ok, result} =
             IntegrationBridge.create_github_pr(
               invocation,
               %{
                 repo: "nshkrdotcom/sample-app",
                 title: "Governed GitHub PR",
                 body: "Created through the direct connector lane",
                 head: "phase-7",
                 base: "main",
                 draft: true
               },
               invoke_fun: invoke_fun
             )

    assert_received {:invoke, "github.pr.create", input, opts}
    assert input.repo == "nshkrdotcom/sample-app"
    assert input.governed_lower_envelope["lower_runtime_kind"] == "direct_connector"

    assert input.governed_lower_envelope["connector_manifest_ref"] ==
             "manifest://jido/connectors/github@local"

    envelope = Keyword.fetch!(opts, :governed_lower_envelope)
    assert envelope.capability_id == "github.pr.create"
    assert envelope.connector_ref == "jido/connectors/github"

    assert result.github_operation_receipt.capability_id == "github.pr.create"
    assert result.github_operation_receipt.capability_negotiation_ref =~ "cap-neg://"
    assert result.github_operation_receipt.provider_response_ref == "artifact://github/pr-create"
  end

  test "GitHub PR feedback sweep reads reviews, comments, statuses, and checks" do
    capabilities = [
      "github.pr.reviews.list",
      "github.pr.review_comments.list",
      "github.commit.statuses.get_combined",
      "github.check_runs.list_for_ref"
    ]

    invocation = authorized_invocation_allowing(capabilities)

    invoke_fun = fn
      "github.pr.reviews.list", input, _opts ->
        send(self(), {:invoke, "github.pr.reviews.list", input})
        {:ok, %{output: github_reviews(), artifact_refs: ["artifact://github/reviews"]}}

      "github.pr.review_comments.list", input, _opts ->
        send(self(), {:invoke, "github.pr.review_comments.list", input})
        {:ok, %{output: github_review_comments(), artifact_refs: ["artifact://github/comments"]}}

      "github.commit.statuses.get_combined", input, _opts ->
        send(self(), {:invoke, "github.commit.statuses.get_combined", input})
        {:ok, %{output: github_status(), artifact_refs: ["artifact://github/status"]}}

      "github.check_runs.list_for_ref", input, _opts ->
        send(self(), {:invoke, "github.check_runs.list_for_ref", input})
        {:ok, %{output: github_checks(), artifact_refs: ["artifact://github/checks"]}}
    end

    assert {:ok, %{github_feedback_sweep: sweep}} =
             IntegrationBridge.sweep_github_pr_feedback(
               invocation,
               %{repo: "nshkrdotcom/sample-app", pull_number: 17, ref: "head-sha"},
               invoke_fun: invoke_fun
             )

    assert_received {:invoke, "github.pr.reviews.list",
                     %{repo: "nshkrdotcom/sample-app", pull_number: 17}}

    assert_received {:invoke, "github.commit.statuses.get_combined",
                     %{repo: "nshkrdotcom/sample-app", ref: "head-sha"}}

    assert sweep.review_count == 2
    assert sweep.review_comment_count == 1
    assert sweep.combined_state == "success"
    assert sweep.check_run_count == 1
    assert Enum.map(sweep.operation_receipts, & &1.capability_id) == capabilities
  end

  test "GitHub PR evidence runtime fetches evidence through governed read dispatches only" do
    invoke_fun = fn
      "github.pr.fetch", input, opts ->
        send(self(), {:invoke, "github.pr.fetch", input, opts})
        {:ok, %{output: github_pr(), artifact_refs: ["artifact://github/pr-fetch"]}}

      "github.pr.reviews.list", input, opts ->
        send(self(), {:invoke, "github.pr.reviews.list", input, opts})
        {:ok, %{output: github_reviews(), artifact_refs: ["artifact://github/reviews"]}}

      "github.pr.review_comments.list", input, opts ->
        send(self(), {:invoke, "github.pr.review_comments.list", input, opts})
        {:ok, %{output: github_review_comments(), artifact_refs: ["artifact://github/comments"]}}

      "github.commit.statuses.get_combined", input, opts ->
        send(self(), {:invoke, "github.commit.statuses.get_combined", input, opts})
        {:ok, %{output: github_status(), artifact_refs: ["artifact://github/status"]}}

      "github.check_runs.list_for_ref", input, opts ->
        send(self(), {:invoke, "github.check_runs.list_for_ref", input, opts})
        {:ok, %{output: github_checks(), artifact_refs: ["artifact://github/checks"]}}
    end

    assert {:ok, receipt} =
             GitHubPrEvidenceRuntime.fetch(
               %{
                 tenant_id: "tenant-1",
                 installation_id: "inst-1",
                 subject_id: "subject-1",
                 execution_id: "exec-1",
                 actor_id: "actor-1",
                 trace_id: "trace-1",
                 repo: "nshkrdotcom/sample-app",
                 pull_number: 17,
                 ref: "head-sha"
               },
               connection_id: "github-conn-1",
               invoke_fun: invoke_fun,
               collect?: false,
               start_runtime?: false,
               register_connector?: false
             )

    assert_received {:invoke, "github.pr.fetch", fetch_input, fetch_opts}
    assert_received {:invoke, "github.pr.reviews.list", reviews_input, reviews_opts}
    assert_received {:invoke, "github.pr.review_comments.list", comments_input, comments_opts}
    assert_received {:invoke, "github.commit.statuses.get_combined", status_input, status_opts}
    assert_received {:invoke, "github.check_runs.list_for_ref", checks_input, checks_opts}

    assert fetch_input.repo == "nshkrdotcom/sample-app"
    assert fetch_input.pull_number == 17
    assert reviews_input.pull_number == 17
    assert comments_input.pull_number == 17
    assert status_input.ref == "head-sha"
    assert checks_input.ref == "head-sha"

    for opts <- [fetch_opts, reviews_opts, comments_opts, status_opts, checks_opts] do
      assert Keyword.fetch!(opts, :connection_id) == "github-conn-1"
      assert "github.pr.fetch" in Keyword.fetch!(opts, :allowed_operations)

      sandbox = Keyword.fetch!(opts, :sandbox)
      assert "github.api.pr.fetch" in Map.fetch!(sandbox, :allowed_tools)
      assert "github.api.pr.reviews.list" in Map.fetch!(sandbox, :allowed_tools)
      refute "github.pr.fetch" in Map.fetch!(sandbox, :allowed_tools)
    end

    refute_received {:invoke, "github.pr.create", _input, _opts}
    refute_received {:invoke, "github.git.ref.delete", _input, _opts}

    assert receipt.status == :receipt_recorded
    assert receipt.provider == "github"
    assert receipt.effect == "github_pr_evidence"
    assert receipt.repo == "nshkrdotcom/sample-app"
    assert receipt.pull_number == 17
    assert receipt.head_sha == "head-sha"
    assert receipt.provider_request_sent? == true
    assert receipt.provider_response_received? == true
    assert receipt.receipt_recorded? == true
    assert receipt.write_operations == []
    assert receipt.fixture_setup_required? == false
    assert receipt.counts.review_count == 2
    assert receipt.counts.review_comment_count == 1
    assert receipt.counts.check_run_count == 1
    assert receipt.provider_ids.pull_request == "17"
    assert receipt.provider_refs.pull_request =~ "/pull/17"
    assert receipt.receipt_refs.lower_request_refs |> length() == 5
    assert Enum.map(receipt.operation_receipts, & &1.capability_id) == receipt.capability_ids
  end

  test "GitHub PR evidence runtime refuses hidden write fixture setup" do
    assert {:error, :github_evidence_write_fixture_requires_separate_command} =
             GitHubPrEvidenceRuntime.fetch(
               %{
                 tenant_id: "tenant-1",
                 installation_id: "inst-1",
                 subject_id: "subject-1",
                 execution_id: "exec-1",
                 actor_id: "actor-1",
                 trace_id: "trace-1",
                 repo: "nshkrdotcom/sample-app",
                 setup_fixture?: true
               },
               connection_id: "github-conn-1",
               start_runtime?: false,
               register_connector?: false
             )
  end

  test "GitHub branch cleanup is a governed delete-ref operation" do
    invocation = authorized_invocation_allowing(["github.git.ref.delete"])

    invoke_fun = fn capability, input, _opts ->
      send(self(), {:invoke, capability, input})
      {:ok, %{output: %{repo: input.repo, ref: input.ref, deleted?: true}}}
    end

    assert {:ok, result} =
             IntegrationBridge.cleanup_github_branch(
               invocation,
               %{repo: "nshkrdotcom/sample-app", ref: "heads/phase-7"},
               invoke_fun: invoke_fun
             )

    assert_received {:invoke, "github.git.ref.delete",
                     %{repo: "nshkrdotcom/sample-app", ref: "heads/phase-7"}}

    assert result.github_operation_receipt.capability_id == "github.git.ref.delete"
    assert result.github_operation_receipt.lower_runtime_kind == "direct_connector"
  end

  test "invoke_run_intent builds a governed lower envelope and receipt around dispatch" do
    invocation = authorized_invocation()

    invoke_fun = fn capability, input, opts ->
      send(self(), {:invoke, capability, input, opts})
      {:ok, %{capability: capability, input: input}}
    end

    assert {:ok,
            %{
              governed_lower_envelope: %GovernedLowerEnvelope{} = envelope,
              governed_lower_receipt: %GovernedLowerReceipt{} = receipt
            }} =
             IntegrationBridge.invoke_run_intent(
               invocation,
               invoke_fun: invoke_fun,
               capability_id: "linear.issues.retrieve",
               lower_runtime_kind: :deterministic_fixture,
               policy_bundle_ref: "policy-bundle://sample-app/default",
               policy_bundle_hash:
                 "sha256:dddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddd",
               cedar_schema_ref: "cedar-schema://sample-app/source",
               cedar_schema_hash:
                 "sha256:eeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee",
               script_ref: "script://linear/retrieve",
               script_hash:
                 "sha256:ffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffff",
               package_refs: ["package://sample-app/coding-ops"],
               sandbox_profile_ref: "sandbox://local/strict",
               attestation_requirement_ref: "attestation://local/dev"
             )

    assert envelope.capability_id == "linear.issues.retrieve"
    assert envelope.lower_runtime_kind == :deterministic_fixture
    assert envelope.authority_ref == "authority-decision://mock-decision-123"
    assert envelope.authority_decision_hash == String.duplicate("a", 64)
    assert envelope.allowed_operations == ["linear.issues.retrieve", "linear.issues.update"]
    assert envelope.resource_scope_refs == ["workspace://work_object/subject-1"]
    assert receipt.status == :succeeded
    assert receipt.policy_bundle_ref == "policy-bundle://sample-app/default"
    assert receipt.cedar_schema_ref == "cedar-schema://sample-app/source"
    assert receipt.script_ref == "script://linear/retrieve"
    assert receipt.package_refs == ["package://sample-app/coding-ops"]
    assert receipt.resource_scope_refs == ["workspace://work_object/subject-1"]
    assert receipt.sandbox_profile_ref == "sandbox://local/strict"
    assert receipt.attestation_requirement_ref == "attestation://local/dev"
    assert GovernedLowerReceipt.matches_envelope?(receipt, envelope)

    assert_received {:invoke, "linear.issues.retrieve", input, opts}
    assert input.governed_lower_envelope["lower_request_ref"] == envelope.lower_request_ref
    assert Keyword.fetch!(opts, :governed_lower_envelope) == envelope
  end

  test "governed lower envelope and receipt carry workspace root and cwd metadata" do
    attrs =
      authorized_invocation_attrs()
      |> put_in([:invocation_request, :execution_governance, :workspace], %{
        "workspace_profile" => "workspace_attached",
        "logical_workspace_ref" => "workspace://tenant-1/root",
        "mutability" => "read_write"
      })
      |> put_in([:invocation_request, :execution_governance, :sandbox], %{
        "allowed_tools" => ["linear.issues.retrieve"],
        "file_scope_ref" => "workspace://tenant-1/root",
        "file_scope_hint" => "/tmp/sample-app/subject-1"
      })
      |> put_in([:invocation_request, :extensions, "citadel", "execution_intent"], %{
        "cwd" => "/tmp/sample-app/subject-1",
        "workspace_root" => "/tmp/sample-app/subject-1"
      })

    invocation = AuthorizedInvocation.new!(attrs)

    assert {:ok, %GovernedLowerEnvelope{} = envelope} =
             AuthorizedInvocation.governed_lower_envelope(
               invocation,
               "linear.issues.retrieve"
             )

    assert envelope.extensions["workspace"] == %{
             "workspace_ref" => "workspace://tenant-1/root",
             "workspace_root_ref" => "workspace://tenant-1/root",
             "file_scope_ref" => "workspace://tenant-1/root",
             "workspace_root" => "/tmp/sample-app/subject-1",
             "cwd" => "/tmp/sample-app/subject-1",
             "path_redacted?" => true,
             "placement_ref" => "target-1"
           }

    receipt = AuthorizedInvocation.governed_lower_receipt!(envelope, :succeeded, %{})

    assert receipt.extensions["workspace"] == envelope.extensions["workspace"]
    assert receipt.extensions["mezzanine"]["dispatch_status"] == "succeeded"
    assert GovernedLowerReceipt.matches_envelope?(receipt, envelope)
  end

  test "governed lower envelope inherits Citadel TRE policy refs" do
    tre_policy = tre_policy()

    attrs =
      authorized_invocation_attrs()
      |> put_in(
        [:invocation_request, :authority_packet, :extensions, "citadel", "tre_policy"],
        tre_policy
      )
      |> put_in(
        [:invocation_request, :execution_governance, :extensions, "citadel", "tre_policy"],
        tre_policy
      )
      |> put_in([:invocation_request, :extensions, "citadel", "tre_policy"], tre_policy)

    invocation = AuthorizedInvocation.new!(attrs)

    assert {:ok, %GovernedLowerEnvelope{} = envelope} =
             AuthorizedInvocation.governed_lower_envelope(
               invocation,
               "linear.issues.retrieve",
               lower_runtime_kind: :tre_rhai
             )

    assert envelope.policy_profile_ref == "tre-policy-profile://coding-ops/standard"
    assert envelope.policy_bundle_ref == "tre-policy-bundle://coding-ops/coding-ops-2026-04-25/1"

    assert envelope.policy_bundle_hash ==
             "sha256:1111111111111111111111111111111111111111111111111111111111111111"

    assert envelope.cedar_schema_ref == "cedar-schema://nshkr_tre/coding_ops/v1"

    assert envelope.cedar_schema_hash ==
             "sha256:2222222222222222222222222222222222222222222222222222222222222222"

    assert envelope.declared_actions == ["tre.run", "process.spawn"]
  end

  test "dispatch_effect dispatches only an authorized invocation envelope" do
    invocation = authorized_invocation()

    invoke_fun = fn capability, input, _opts ->
      {:ok, %{capability: capability, input: input}}
    end

    assert {:ok, %{capability: "linear.issues.update"}} =
             IntegrationBridge.dispatch_effect(invocation,
               invoke_fun: invoke_fun,
               capability_id: "linear.issues.update"
             )
  end

  test "Codex Linear and GitHub lower paths do not require a TRE adapter" do
    invoke_fun = fn capability, input, opts ->
      refute Keyword.has_key?(opts, :tre_adapter)
      {:ok, %{capability: capability, input: input, output: %{accepted: true}}}
    end

    cases = [
      {
        authorized_invocation_allowing(["linear.issues.retrieve"]),
        "linear.issues.retrieve",
        :direct_connector,
        "jido/connectors/linear"
      },
      {
        authorized_invocation_allowing(["github.pr.create"]),
        "github.pr.create",
        :direct_connector,
        "jido/connectors/github"
      },
      {
        authorized_invocation_allowing(["codex.session.turn"]),
        "codex.session.turn",
        :codex_session,
        "jido/connectors/codex_cli"
      }
    ]

    for {invocation, capability_id, lower_runtime_kind, connector_ref} <- cases do
      assert {:ok, result} =
               IntegrationBridge.invoke_run_intent(invocation,
                 invoke_fun: invoke_fun,
                 capability_id: capability_id
               )

      assert result.capability == capability_id
      assert result.governed_lower_envelope.lower_runtime_kind == lower_runtime_kind
      assert result.governed_lower_envelope.connector_ref == connector_ref
      assert result.governed_lower_receipt.lower_runtime_kind == lower_runtime_kind
    end
  end

  test "TRE lower dispatch is allowed only when a TRE adapter is explicitly forwarded" do
    invoke_fun = fn capability, input, opts ->
      assert Keyword.fetch!(opts, :tre_adapter) == Jido.Integration.V2.ControlPlane.FakeTreAdapter

      {:ok,
       %{
         capability: capability,
         lower_runtime_kind: input.governed_lower_envelope["lower_runtime_kind"],
         output: %{
           accepted: true,
           execution_plane_receipt: %{
             "receipt_ref" => "execution-plane-tre-receipt://trace-123/succeeded",
             "status" => "succeeded",
             "artifact_refs" => ["tre-artifact://trace-123/runner-output"],
             "event_refs" => ["tre-event://trace-123/succeeded"]
           },
           governed_lower_receipt: %{
             "lower_receipt_ref" => "lower-receipt://execution-plane-tre/lower-req-1/succeeded",
             "status" => "succeeded",
             "artifact_refs" => ["tre-artifact://trace-123/runner-output"],
             "event_refs" => ["tre-event://trace-123/succeeded"]
           }
         }
       }}
    end

    assert {:ok, result} =
             IntegrationBridge.invoke_run_intent(authorized_invocation(),
               invoke_fun: invoke_fun,
               invoke_opts: [tre_adapter: Jido.Integration.V2.ControlPlane.FakeTreAdapter],
               lower_runtime_kind: :tre_rhai
             )

    assert result.lower_runtime_kind == "tre_rhai"
    assert result.governed_lower_envelope.lower_runtime_kind == :tre_rhai
    assert result.governed_lower_receipt.lower_runtime_kind == :tre_rhai

    assert result.governed_lower_receipt.artifact_refs == [
             "tre-artifact://trace-123/runner-output"
           ]

    assert result.governed_lower_receipt.event_refs == ["tre-event://trace-123/succeeded"]

    assert result.governed_lower_receipt.extensions["mezzanine"][
             "jido_governed_lower_receipt_ref"
           ] ==
             "lower-receipt://execution-plane-tre/lower-req-1/succeeded"

    assert result.governed_lower_receipt.extensions["mezzanine"][
             "execution_plane_receipt_ref"
           ] == "execution-plane-tre-receipt://trace-123/succeeded"
  end

  test "direct lower dispatch returns governed denials before side effects" do
    never = fn _capability, _input, _opts -> flunk("provider invoke must not run") end

    assert {:error, %GovernedLowerDenial{denial_class: :capability_denied}} =
             IntegrationBridge.invoke_run_intent(authorized_invocation(),
               invoke_fun: never,
               capability_id: "linear.issues.delete"
             )

    assert {:error, %GovernedLowerDenial{denial_class: :manifest_stale}} =
             IntegrationBridge.invoke_run_intent(authorized_invocation(),
               invoke_fun: never,
               capability_id: "linear.issues.update",
               side_effect_class: :write,
               idempotency_class: :non_idempotent,
               connector_manifest_state: :stale
             )

    assert {:error,
            %GovernedLowerDenial{
              denial_class: :lower_runtime_unavailable,
              lower_runtime_kind: :tre_rhai
            }} =
             IntegrationBridge.invoke_run_intent(authorized_invocation(),
               invoke_fun: never,
               lower_runtime_kind: :tre_rhai
             )

    assert {:error, %GovernedLowerDenial{denial_class: :resource_scope_unresolvable}} =
             IntegrationBridge.invoke_run_intent(authorized_invocation(),
               invoke_fun: never,
               resource_scope_refs: ["unresolved://workspace/main"]
             )

    assert {:error, %GovernedLowerDenial{denial_class: :sandbox_downgrade}} =
             IntegrationBridge.invoke_run_intent(authorized_invocation_with_governance_posture(),
               invoke_fun: never,
               sandbox_level: :none
             )

    assert {:error, %GovernedLowerDenial{denial_class: :attestation_unsatisfied}} =
             IntegrationBridge.invoke_run_intent(authorized_invocation_with_governance_posture(),
               invoke_fun: never,
               acceptable_attestation: ["attestation://unexpected"]
             )
  end

  test "direct dispatch rejects old RunIntent and generic map inputs before Jido invocation" do
    intent =
      RunIntent.new!(%{
        intent_id: "intent-run-1",
        program_id: "program-1",
        work_id: "work-1",
        capability: "linear.issues.retrieve",
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
          capability_id: "linear.issues.update",
          input: %{"id" => "ENG-42", "state" => "done"}
        }
      })

    invoke_fun = fn _capability, _input, _opts -> flunk("provider invoke must not run") end

    assert_raise FunctionClauseError, fn ->
      IntegrationBridge.dispatch_effect(intent, invoke_fun: invoke_fun)
    end

    assert {:error, %GovernedLowerDenial{denial_class: :capability_denied}} =
             IntegrationBridge.dispatch_effect(authorized_invocation(),
               invoke_fun: invoke_fun,
               capability_id: "github.pr.merge"
             )
  end

  test "authorized invocation requires mock-valid authority and governance packets" do
    attrs =
      authorized_invocation_attrs()
      |> put_in([:invocation_request, :authority_packet], %{})

    assert_error_contains("missing required field :contract_version", fn ->
      AuthorizedInvocation.new!(attrs)
    end)
  end

  test "authorized invocation rejects tenant and trace mismatches before lower dispatch" do
    assert_error_contains("tenant_id mismatch", fn ->
      authorized_invocation_attrs()
      |> put_in([:invocation_request, :tenant_id], "tenant-other")
      |> AuthorizedInvocation.new!()
    end)

    assert_error_contains("trace_id mismatch", fn ->
      authorized_invocation_attrs()
      |> put_in([:invocation_request, :trace_id], "trace-other")
      |> AuthorizedInvocation.new!()
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
    input = AuthorizedInvocation.invoke_input(invocation, "linear.issues.update")

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

  test "authorized invocation builds governed Codex turn input from Citadel execution intent" do
    attrs =
      authorized_invocation_attrs()
      |> put_in([:invocation_request, :allowed_operations], [
        "codex.session.turn",
        "linear.comments.update"
      ])
      |> put_in([:invocation_request, :execution_governance, :operations], %{
        "allowed_operations" => ["codex.session.turn", "linear.comments.update"]
      })
      |> put_in([:invocation_request, :execution_governance, :sandbox], %{
        "allowed_tools" => ["codex.session.turn", "linear.api.comments.update"],
        "file_scope_hint" => "/home/dev/sample-app",
        "file_scope_ref" => "workspace://work_object/subject-1"
      })
      |> put_in([:invocation_request, :extensions, "citadel", "execution_intent"], %{
        "prompt" => "Implement the governed slice",
        "cwd" => "/home/dev/sample-app",
        "continuation" => %{"strategy" => "latest"},
        "provider_metadata" => %{"model" => "gpt-5.4", "app_server" => true},
        "memory_context" => %{
          "context_pack_ref" => "context-pack://app-kit/run-1",
          "context_hash" => "sha256:context-pack",
          "fragment_refs" => ["fragment-memory-1"],
          "memory_query_ref" => "memory-query://run-1",
          "memory_evidence_refs" => ["memory-evidence://workspace/main/1"],
          "redaction_policy_ref" => "policy://hash"
        },
        "dynamic_tool_manifest" => %{"tools" => ["linear.comment.update"]},
        "host_tools" => [
          %{
            "name" => "linear_comment_update",
            "inputSchema" => %{"type" => "object"}
          }
        ]
      })

    invocation = AuthorizedInvocation.new!(attrs)
    input = AuthorizedInvocation.invoke_input(invocation, "codex.session.turn")

    assert input.prompt == "Implement the governed slice"
    assert input.cwd == "/home/dev/sample-app"
    assert input.continuation == %{"strategy" => "latest"}

    assert input.host_tools == [
             %{"name" => "linear_comment_update", "inputSchema" => %{"type" => "object"}}
           ]

    assert input.provider_metadata["model"] == "gpt-5.4"
    assert input.provider_metadata["app_server"] == true

    assert input.provider_metadata["memory_context"] == %{
             "context_pack_ref" => "context-pack://app-kit/run-1",
             "context_hash" => "sha256:context-pack",
             "fragment_refs" => ["fragment-memory-1"],
             "memory_query_ref" => "memory-query://run-1",
             "memory_evidence_refs" => ["memory-evidence://workspace/main/1"],
             "redaction_policy_ref" => "policy://hash"
           }

    assert input.provider_metadata["dynamic_tool_manifest"] == %{
             "tools" => ["linear.comment.update"]
           }

    assert input.dynamic_tool_manifest == %{"tools" => ["linear.comment.update"]}
    assert input.authority_metadata["authority_ref"] == "authority-decision://mock-decision-123"
    assert input.authority_metadata["authority_decision_hash"] == String.duplicate("a", 64)

    assert input.authority_metadata["allowed_operations"] == [
             "codex.session.turn",
             "linear.comments.update"
           ]
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

  defp authorized_invocation_allowing(allowed_operations) do
    attrs =
      authorized_invocation_attrs()
      |> put_in([:invocation_request, :allowed_operations], allowed_operations)
      |> put_in([:invocation_request, :execution_governance, :operations], %{
        "allowed_operations" => allowed_operations
      })

    AuthorizedInvocation.new!(attrs)
  end

  defp authorized_invocation_with_governance_posture do
    attrs =
      authorized_invocation_attrs()
      |> put_in([:invocation_request, :execution_governance, :sandbox], %{
        "level" => "strict",
        "egress" => "restricted",
        "approvals" => "manual",
        "acceptable_attestation" => ["attestation://required"],
        "allowed_tools" => ["linear.issues.update"],
        "file_scope_ref" => "workspace://work_object/subject-1",
        "file_scope_hint" => nil
      })

    AuthorizedInvocation.new!(attrs)
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
      allowed_operations: ["linear.issues.retrieve", "linear.issues.update"],
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
      sandbox: %{"allowed_tools" => ["linear.issues.update"]},
      boundary: %{},
      topology: %{},
      workspace: %{},
      resources: %{},
      placement: %{},
      operations: %{"allowed_operations" => ["linear.issues.retrieve", "linear.issues.update"]},
      extensions: %{"citadel" => %{}}
    }
  end

  defp tre_policy do
    %{
      "selection_mode" => "prebuilt_bundle_ref",
      "policy_profile_ref" => "tre-policy-profile://coding-ops/standard",
      "policy_bundle_ref" => "tre-policy-bundle://coding-ops/coding-ops-2026-04-25/1",
      "policy_bundle_hash" =>
        "sha256:1111111111111111111111111111111111111111111111111111111111111111",
      "cedar_schema_ref" => "cedar-schema://nshkr_tre/coding_ops/v1",
      "cedar_schema_hash" =>
        "sha256:2222222222222222222222222222222222222222222222222222222222222222",
      "allowed_actions" => ["tre.run", "process.spawn"],
      "denied_actions" => []
    }
  end

  defp source_binding do
    %{
      source_binding_id: "linear-primary",
      installation_id: "inst-1",
      provider: "linear",
      connection_ref: "linear-primary",
      candidate_filters: %{project_slug: "ops-automation", assignee: "me"},
      state_mapping: %{
        "submitted" => ["Todo", "Backlog"],
        "retry_submission" => ["Todo"],
        "completed" => ["Done", "Completed"],
        "rejected" => ["Canceled", "Duplicate"]
      }
    }
  end

  defp linear_issue do
    %{
      id: "lin-issue-321",
      identifier: "ENG-321",
      title: "Investigate source publication",
      description: "Keep workpad in sync",
      priority: 2,
      labels: ["Automation"],
      branch_name: "eng-321-source-publication",
      url: "https://linear.app/acme/issue/ENG-321",
      created_at: "2026-03-12T09:15:00Z",
      updated_at: "2026-03-12T10:00:00Z",
      state: %{id: "state-todo", name: "Todo", type: "unstarted"},
      assignee: %{id: "usr-linear-viewer", name: "Taylor Automation"},
      blockers: []
    }
  end

  defp linear_issue("lin-issue-654") do
    %{
      linear_issue()
      | id: "lin-issue-654",
        identifier: "ENG-654",
        title: "Audit release checklist",
        branch_name: "eng-654-audit-release-checklist",
        url: "https://linear.app/acme/issue/ENG-654"
    }
  end

  defp github_pr do
    %{
      repo: "nshkrdotcom/sample-app",
      pull_number: 17,
      title: "Governed GitHub PR",
      state: "open",
      html_url: "https://github.com/nshkrdotcom/sample-app/pull/17",
      head: %{ref: "phase-7", sha: "head-sha"},
      base: %{ref: "main", sha: "base-sha"}
    }
  end

  defp github_reviews do
    %{
      repo: "nshkrdotcom/sample-app",
      pull_number: 17,
      reviews: [
        %{review_id: 1, state: "APPROVED"},
        %{review_id: 2, state: "CHANGES_REQUESTED"}
      ]
    }
  end

  defp github_review_comments do
    %{
      repo: "nshkrdotcom/sample-app",
      pull_number: 17,
      comments: [%{comment_id: 11, path: "lib/sample-app.ex"}]
    }
  end

  defp github_status do
    %{
      repo: "nshkrdotcom/sample-app",
      ref: "head-sha",
      state: "success",
      statuses: [%{context: "mix ci", state: "success"}]
    }
  end

  defp github_checks do
    %{
      repo: "nshkrdotcom/sample-app",
      ref: "head-sha",
      check_runs: [%{name: "mix ci", status: "completed", conclusion: "success"}]
    }
  end

  defp sha256(value) when is_binary(value) do
    digest = :crypto.hash(:sha256, value)
    "sha256:" <> Base.encode16(digest, case: :lower)
  end

  defp codex_event_stream_fixture(1) do
    [
      %{
        event_id: "event-codex-completed",
        type: "codex.app_server.message",
        payload: %{"method" => "turn/completed", "params" => %{"status" => "completed"}}
      },
      %{
        event_id: "event-codex-malformed",
        type: "protocol.malformed",
        payload: %{raw: "{not json"}
      },
      %{
        event_id: "event-codex-approval-required",
        type: "codex.app_server.message",
        payload: %{
          "method" => "item/commandExecution/requestApproval",
          "params" => %{"command" => "mix test"}
        }
      },
      %{
        event_id: "event-codex-approval-auto",
        type: "approval.auto_approved",
        payload: %{decision: "acceptForSession"}
      },
      %{
        event_id: "event-codex-user-input-required",
        type: "codex.app_server.message",
        payload: %{
          "method" => "item/tool/requestUserInput",
          "params" => %{"questions" => [%{"id" => "q-1"}]}
        }
      },
      %{
        event_id: "event-codex-user-input-auto",
        type: "tool_input.auto_answered",
        payload: %{answer: "This is a non-interactive session. Operator input is unavailable."}
      },
      %{
        event_id: "event-codex-token-usage",
        type: "codex.app_server.message",
        payload: %{
          "method" => "thread/tokenUsage/updated",
          "params" => %{
            "tokenUsage" => %{
              "total" => %{"inputTokens" => 10, "outputTokens" => 4, "totalTokens" => 14},
              "last" => %{"inputTokens" => 2, "outputTokens" => 1, "totalTokens" => 3}
            }
          }
        }
      },
      %{
        event_id: "event-codex-rate-limits",
        type: "codex.app_server.message",
        payload: %{
          "method" => "account/rateLimits/updated",
          "params" => %{
            "rateLimits" => %{
              "limit_id" => "codex",
              "primary" => %{"remaining" => 90, "limit" => 100}
            }
          }
        }
      },
      %{
        event_id: "event-codex-agent-message",
        type: "codex.app_server.message",
        payload: %{
          "method" => "codex/event/agent_message_delta",
          "params" => %{"msg" => %{"delta" => "STREAM_BODY_DO_NOT_EXPOSE"}}
        }
      }
    ]
  end

  defp codex_event_stream_fixture(2) do
    [
      %{
        event_id: "event-codex-failed",
        type: "codex.app_server.message",
        payload: %{
          "method" => "turn/failed",
          "params" => %{"error" => %{"message" => "provider failed"}}
        }
      },
      %{
        event_id: "event-codex-cancelled",
        type: "codex.app_server.message",
        payload: %{"method" => "turn/cancelled", "params" => %{"reason" => "operator"}}
      },
      %{
        event_id: "event-codex-timeout",
        type: "turn.timeout",
        payload: %{timeout_ms: 1_000}
      }
    ]
  end

  defp codex_token_accounting_fixture(1) do
    [
      %{
        event_id: "event-codex-token-thread-total",
        type: "codex.app_server.message",
        payload: %{
          "method" => "thread/tokenUsage/updated",
          "params" => %{
            "thread_id" => "thread-token-accounting",
            "tokenUsage" => %{
              "total" => %{"inputTokens" => 10, "outputTokens" => 4, "totalTokens" => 14},
              "last" => %{"inputTokens" => 9999, "outputTokens" => 9999, "totalTokens" => 9999}
            }
          }
        }
      },
      %{
        event_id: "event-codex-generic-usage",
        type: "codex.app_server.message",
        payload: %{
          "method" => "telemetry/usage",
          "params" => %{
            "thread_id" => "thread-token-accounting",
            "usage" => %{"inputTokens" => 9999, "outputTokens" => 9999, "totalTokens" => 9999}
          }
        }
      },
      %{
        event_id: "event-codex-turn-completed-usage",
        type: "codex.app_server.message",
        payload: %{
          "method" => "turn/completed",
          "params" => %{
            "thread_id" => "thread-token-accounting",
            "usage" => %{"inputTokens" => 9999, "outputTokens" => 9999, "totalTokens" => 9999}
          }
        }
      }
    ]
  end

  defp codex_token_accounting_fixture(2) do
    [
      %{
        event_id: "event-codex-lower-thread-total",
        type: "codex.app_server.message",
        payload: %{
          "method" => "thread/tokenUsage/updated",
          "params" => %{
            "thread_id" => "thread-token-accounting",
            "tokenUsage" => %{
              "total" => %{"inputTokens" => 9, "outputTokens" => 3, "totalTokens" => 12},
              "last" => %{"inputTokens" => 9999, "outputTokens" => 9999, "totalTokens" => 9999}
            }
          }
        }
      },
      %{
        event_id: "event-codex-token-count-total",
        type: "codex.app_server.message",
        payload: %{
          "method" => "codex/event/token_count",
          "params" => %{
            "msg" => %{
              "thread_id" => "thread-token-accounting",
              "info" => %{
                "total_token_usage" => %{
                  "input_tokens" => 12,
                  "output_tokens" => 5,
                  "total_tokens" => 17
                },
                "last_token_usage" => %{
                  "input_tokens" => 9999,
                  "output_tokens" => 9999,
                  "total_tokens" => 9999
                }
              }
            }
          }
        }
      }
    ]
  end

  defp codex_agent_attrs do
    %{
      tenant_ref: "tenant://neutral",
      installation_ref: "installation://neutral/codex",
      subject_ref: "subject://neutral/codex",
      run_ref: "run://neutral/codex",
      trace_id: "trace://neutral/codex",
      idempotency_key: "idem-codex-neutral",
      authority_context_ref: "authority-context://neutral/codex"
    }
  end

  defp tmp_dir(label) do
    path =
      Path.join(
        System.tmp_dir!(),
        "mezzanine-integration-#{label}-#{System.unique_integer([:positive])}"
      )

    File.rm_rf!(path)
    path
  end
end
