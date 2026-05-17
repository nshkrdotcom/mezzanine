defmodule Mezzanine.IntegrationBridge.ProviderAdapters.CodexCli.AgentRuntime do
  @moduledoc """
  Lower-owned Codex agent runtime for AppKit AgentIntake.

  The runtime translates an AppKit/Mezzanine agent-run spec into a governed Jido
  `codex.session.turn` invocation and returns an M2 projection shape that AppKit
  can expose through its headless readback DTOs.
  """

  alias Jido.Integration.V2
  alias Jido.Integration.V2.Connectors.CodexCli
  alias Jido.Integration.V2.RuntimeRouter
  alias Mezzanine.IntegrationBridge.ProviderAuthorityAdmission
  alias Mezzanine.WorkspaceEngine.{Hooks, LocalCommandRunner, WorkspaceRecord}

  @capability_id "codex.session.turn"
  @session_start_capability_id "codex.session.start"
  @session_stop_capability_id "codex.session.stop"
  @connector_id "codex_cli"
  @codex_workspace_root "/tmp/jido_codex_cli_workspace"
  @scopes ["session:execute", "session:control", "session:tools"]
  @app_server_protocol_methods ["initialize", "initialized", "thread/start", "turn/start"]
  @token_accounting_source "runtime:event:codex-token-accounting"
  @codex_event_name_categories %{
    "approval_auto_approved" => :approval_auto_approved,
    "approval_required" => :approval_required,
    "malformed" => :malformed,
    "timeout" => :turn_timeout,
    "tool_input_auto_answered" => :user_input_auto_answered,
    "turn_cancelled" => :turn_cancelled,
    "turn_canceled" => :turn_cancelled,
    "turn_completed" => :turn_completed,
    "turn_failed" => :turn_failed,
    "turn_input_required" => :user_input_required,
    "turn_timeout" => :turn_timeout
  }
  @codex_event_type_categories %{
    "approval.auto_approved" => :approval_auto_approved,
    "attempt.completed" => :turn_completed,
    "malformed" => :malformed,
    "protocol.malformed" => :malformed,
    "result" => :turn_completed,
    "timeout" => :turn_timeout,
    "tool_input.auto_answered" => :user_input_auto_answered,
    "turn.cancelled" => :turn_cancelled,
    "turn.canceled" => :turn_cancelled,
    "turn.completed" => :turn_completed,
    "turn.failed" => :turn_failed,
    "turn.timeout" => :turn_timeout
  }
  @codex_terminal_method_categories %{
    "turn/cancelled" => :turn_cancelled,
    "turn/canceled" => :turn_cancelled,
    "turn/completed" => :turn_completed,
    "turn/failed" => :turn_failed,
    "turn/input_required" => :user_input_required
  }

  @spec run(map() | keyword()) :: {:ok, map()} | {:error, term()}
  def run(attrs), do: run(attrs, [])

  @spec run(map() | keyword(), keyword()) :: {:ok, map()} | {:error, term()}
  def run(attrs, opts) when is_list(opts) do
    attrs = normalize(attrs)
    invoke_fun = Keyword.get(opts, :invoke_fun, &V2.invoke/3)
    workspace_root = codex_workspace_root(opts)

    with :ok <- maybe_start_runtime_router(opts),
         :ok <- maybe_register_connector(opts),
         {:ok, connection_id} <- codex_connection_id(attrs, opts),
         :ok <- prepare_workspace(workspace_root, opts) do
      workspace = workspace_for_hooks(attrs, opts, workspace_root)

      attrs
      |> run_codex_attempt(opts, invoke_fun, workspace_root, connection_id, workspace)
      |> complete_codex_attempt(attrs, workspace, opts)
    end
  end

  def run(_attrs, _opts), do: {:error, :invalid_codex_agent_runtime_opts}

  defp maybe_start_runtime_router(opts) do
    if Keyword.get(opts, :start_runtime_router?, true) do
      if Code.ensure_loaded?(RuntimeRouter) and function_exported?(RuntimeRouter, :start!, 0) do
        RuntimeRouter.start!()
      else
        {:error, :codex_runtime_router_not_available}
      end
    else
      :ok
    end
  end

  defp maybe_register_connector(opts) do
    if Keyword.get(opts, :register_connector?, true) do
      if Code.ensure_loaded?(CodexCli) and function_exported?(CodexCli, :manifest, 0) do
        V2.register_connector(CodexCli)
      else
        {:error, :codex_connector_not_available}
      end
    else
      :ok
    end
  end

  defp codex_connection_id(attrs, opts) do
    case Keyword.get(opts, :connection_id) do
      connection_id when is_binary(connection_id) and connection_id != "" ->
        {:ok, connection_id}

      _missing ->
        install_codex_connection(attrs, opts)
    end
  end

  defp install_codex_connection(attrs, opts) do
    start_install_fun = Keyword.get(opts, :start_install_fun, &V2.start_install/3)
    complete_install_fun = Keyword.get(opts, :complete_install_fun, &V2.complete_install/2)
    tenant_id = Keyword.get(opts, :tenant_id, map_value(attrs, :tenant_ref))
    actor_id = Keyword.get(opts, :actor_id, actor_id(attrs))
    subject = Keyword.get(opts, :subject, "codex-cli-native-auth")

    with {:ok, %{install: install, connection: connection}} <-
           start_install_fun.(@connector_id, tenant_id, %{
             actor_id: actor_id,
             auth_type: :api_token,
             profile_id: "native_codex_cli",
             subject: subject,
             requested_scopes: @scopes
           }),
         {:ok, %{connection: completed_connection}} <-
           complete_install_fun.(install.install_id, %{
             subject: subject,
             granted_scopes: @scopes,
             secret: %{access_token: "codex-native-auth-redacted"}
           }) do
      {:ok,
       map_value(completed_connection, :connection_id) || map_value(connection, :connection_id)}
    end
  end

  defp prepare_workspace(workspace_root, opts) when is_binary(workspace_root) do
    prepare_workspace_fun = Keyword.get(opts, :prepare_workspace_fun, &File.mkdir_p/1)
    prepare_workspace_fun.(workspace_root)
  end

  defp prepare_workspace(_workspace_root, _opts), do: :ok

  defp workspace_for_hooks(attrs, opts, workspace_root) do
    hook_specs = workspace_hook_specs(attrs, opts)

    if hook_specs == [] do
      nil
    else
      opts
      |> Keyword.get(:workspace_record)
      |> workspace_record(attrs, workspace_root, hook_specs)
    end
  end

  defp run_codex_attempt(attrs, opts, invoke_fun, workspace_root, connection_id, workspace) do
    with {:ok, before_run_receipts} <- run_workspace_hooks(workspace, :before_run, opts),
         invoke_opts <- codex_invoke_opts(attrs, connection_id, opts, workspace_root),
         {:ok, turn_attempts} <-
           run_codex_turns(attrs, opts, invoke_fun, workspace_root, invoke_opts),
         {:ok, session_stop} <-
           maybe_stop_codex_session(attrs, opts, invoke_fun, invoke_opts, turn_attempts) do
      {:ok, turn_attempts, invoke_opts, before_run_receipts, session_stop}
    end
  end

  defp complete_codex_attempt(attempt_result, attrs, workspace, opts) do
    after_run_receipts = run_after_run_hooks(workspace, opts)

    case {attempt_result, after_run_receipts} do
      {{:ok, turn_attempts, invoke_opts, before_run_receipts, session_stop}, receipts} ->
        {:ok,
         projection(
           attrs,
           turn_attempts,
           invoke_opts,
           before_run_receipts,
           receipts,
           session_stop,
           opts
         )}

      {{:error, reason}, []} ->
        {:error, reason}

      {{:error, reason}, receipts} ->
        {:error, {:codex_agent_runtime_failed, reason, %{after_run_hook_receipts: receipts}}}
    end
  end

  defp run_workspace_hooks(nil, _stage, _opts), do: {:ok, []}

  defp run_workspace_hooks(%WorkspaceRecord{} = workspace, stage, opts) do
    Hooks.run(workspace, stage,
      runner: workspace_hook_runner(opts),
      redactions: Keyword.get(opts, :hook_redactions, []),
      max_output_bytes: Keyword.get(opts, :hook_max_output_bytes, 4_096)
    )
  end

  defp run_after_run_hooks(workspace, opts) do
    case run_workspace_hooks(workspace, :after_run, opts) do
      {:ok, receipts} -> receipts
      {:error, {_reason, receipt}} -> [receipt]
    end
  end

  defp workspace_hook_specs(attrs, opts) do
    opts
    |> Keyword.get(:workspace_hook_specs, map_value(attrs, :workspace_hook_specs))
    |> case do
      nil -> map_value(attrs, :hook_specs)
      specs -> specs
    end
    |> List.wrap()
    |> Enum.reject(&is_nil/1)
  end

  defp workspace_hook_runner(opts) do
    Keyword.get(
      opts,
      :workspace_hook_runner,
      LocalCommandRunner.runner(env: Keyword.get(opts, :workspace_hook_env, %{}))
    )
  end

  defp workspace_record(%WorkspaceRecord{} = record, _attrs, _workspace_root, hook_specs),
    do: %{record | hook_specs: hook_specs}

  defp workspace_record(_record, attrs, workspace_root, hook_specs) do
    subject_ref = map_value(attrs, :subject_ref)
    installation_id = map_value(attrs, :installation_ref) || "installation://unknown"
    subject_id = subject_ref || "subject://unknown"
    workspace_id = workspace_id(attrs, workspace_root)

    %WorkspaceRecord{
      workspace_id: workspace_id,
      installation_id: installation_id,
      subject_id: subject_id,
      subject_ref: subject_ref,
      logical_ref: "workspace:#{installation_id}:#{subject_id}",
      concrete_root: workspace_root,
      concrete_path: workspace_root,
      slug: ref_suffix(workspace_id),
      placement_kind: :local,
      cleanup_policy: :never,
      safety_hash: digest([workspace_id, workspace_root]),
      file_scope: %{writable_roots: [workspace_root], read_roots: [workspace_root]},
      hook_specs: hook_specs,
      remote_hints: %{
        run_ref: map_value(attrs, :run_ref),
        workflow_ref: "workflow://codex-agent-runtime/#{ref_suffix(map_value(attrs, :run_ref))}"
      },
      created_now?: false,
      reuse?: true
    }
  end

  defp workspace_id(attrs, workspace_root) do
    case map_value(attrs, :workspace_ref) do
      "workspace://" <> id when id != "" -> id
      value when is_binary(value) and value != "" -> value
      _missing -> "codex-agent-runtime-#{ref_suffix(workspace_root)}"
    end
  end

  defp codex_invoke_opts(attrs, connection_id, opts, workspace_root) do
    [
      connection_id: connection_id,
      actor_id: Keyword.get(opts, :actor_id, actor_id(attrs)),
      tenant_id: Keyword.get(opts, :tenant_id, map_value(attrs, :tenant_ref)),
      trace_id: map_value(attrs, :trace_id),
      environment: Keyword.get(opts, :environment, :prod),
      allowed_operations: Keyword.get(opts, :allowed_operations, [@capability_id]),
      sandbox:
        Keyword.get(opts, :sandbox, %{
          level: :strict,
          egress: :restricted,
          approvals: :manual,
          file_scope: workspace_root,
          allowed_tools: [@capability_id]
        })
    ]
    |> put_present(:runtime_binding, Keyword.get(opts, :runtime_binding))
    |> put_present(:runtime_auth_mode, Keyword.get(opts, :runtime_auth_mode))
    |> put_present(:runtime_auth_scope, Keyword.get(opts, :runtime_auth_scope))
  end

  defp codex_input(attrs, opts, workspace_root) do
    codex_input(attrs, opts, workspace_root, 1, nil)
  end

  defp codex_input(attrs, opts, workspace_root, 1, _previous_result) do
    %{
      prompt: first_turn_prompt(attrs, opts),
      cwd: workspace_root,
      provider_metadata: %{"app_server" => true, "skip_git_repo_check" => true},
      authority_metadata: authority_metadata(attrs),
      host_tools: []
    }
    |> put_present(:dynamic_tool_manifest, Keyword.get(opts, :dynamic_tool_manifest))
  end

  defp codex_input(attrs, opts, workspace_root, turn_index, previous_result) do
    max_turns = requested_max_turns(attrs)

    %{
      prompt: continuation_guidance(attrs, turn_index, max_turns),
      cwd: workspace_root,
      provider_metadata: %{"app_server" => true, "skip_git_repo_check" => true},
      authority_metadata: authority_metadata(attrs, turn_index, max_turns),
      host_tools: [],
      continuation: continuation_metadata(previous_result, turn_index, max_turns)
    }
    |> put_present(:dynamic_tool_manifest, Keyword.get(opts, :dynamic_tool_manifest))
  end

  defp first_turn_prompt(attrs, opts) do
    Keyword.get(opts, :prompt) ||
      non_empty(map_value(attrs, :initial_input_body)) ||
      non_empty(map_value(attrs, :prompt)) ||
      "Return one concise sentence confirming the governed Codex runtime path is operational. Do not modify files."
  end

  defp run_codex_turns(attrs, opts, invoke_fun, workspace_root, invoke_opts) do
    turn_limit = codex_turn_limit(attrs)

    1..turn_limit
    |> Enum.reduce_while({:ok, [], nil}, fn turn_index, {:ok, attempts, previous_result} ->
      case run_codex_turn(
             attrs,
             opts,
             invoke_fun,
             workspace_root,
             invoke_opts,
             turn_index,
             previous_result
           ) do
        {:ok, attempt, result} ->
          next_codex_turn_step(turn_index, turn_limit, [attempt | attempts], result)

        {:error, reason} ->
          {:halt, {:error, reason}}
      end
    end)
    |> case do
      {:ok, attempts, _previous_result} -> {:ok, Enum.reverse(attempts)}
      {:error, reason} -> {:error, reason}
    end
  end

  defp run_codex_turn(
         attrs,
         opts,
         invoke_fun,
         workspace_root,
         invoke_opts,
         turn_index,
         previous_result
       ) do
    input = codex_turn_input(attrs, opts, workspace_root, turn_index, previous_result)

    with {:ok, authority_handoff} <-
           ProviderAuthorityAdmission.authorize_codex_dispatch(
             attrs,
             @capability_id,
             invoke_opts,
             opts
           ),
         {:ok, result} <- invoke_fun.(@capability_id, input, invoke_opts) do
      {:ok,
       %{
         turn_index: turn_index,
         input: input,
         result: result,
         authority_handoff: authority_handoff
       }, result}
    end
  end

  defp codex_turn_input(attrs, opts, workspace_root, 1, _previous_result),
    do: codex_input(attrs, opts, workspace_root)

  defp codex_turn_input(attrs, opts, workspace_root, turn_index, previous_result),
    do: codex_input(attrs, opts, workspace_root, turn_index, previous_result)

  defp maybe_stop_codex_session(attrs, opts, invoke_fun, invoke_opts, turn_attempts) do
    first_turn_attempt = List.first(turn_attempts)
    final_turn_attempt = List.last(turn_attempts)
    output = turn_attempt_output(final_turn_attempt)
    event_stream = codex_event_stream_evidence(attrs, turn_attempts, opts, output)
    status = terminal_status(output, event_stream)
    run_id = turn_run_id(first_turn_attempt, map_value(attrs, :run_ref))

    session_start =
      first_turn_attempt
      |> turn_attempt_result()
      |> lower_runtime_events(opts)
      |> session_start_evidence(run_id)

    cond do
      is_nil(session_start) ->
        {:ok, nil}

      not stop_after_terminal_status?(status) ->
        {:ok, nil}

      true ->
        stop_codex_session(session_start, attrs, opts, invoke_fun, invoke_opts)
    end
  end

  defp stop_codex_session(session_start, attrs, opts, invoke_fun, invoke_opts) do
    stop_input = %{session_id: session_start.runtime_control_session_id}
    stop_opts = codex_session_stop_invoke_opts(invoke_opts)

    with {:ok, authority_handoff} <-
           ProviderAuthorityAdmission.authorize_codex_dispatch(
             attrs,
             @session_stop_capability_id,
             stop_opts,
             opts
           ) do
      case invoke_fun.(@session_stop_capability_id, stop_input, stop_opts) do
        {:ok, result} ->
          {:ok, session_stop_evidence(session_start, result, authority_handoff)}

        {:error, _reason} ->
          {:ok, session_stop_failure_evidence(session_start, authority_handoff)}
      end
    end
  end

  defp stop_after_terminal_status?(status) do
    status in ["completed", "failed", "cancelled", "canceled", "timeout"]
  end

  defp codex_session_stop_invoke_opts(invoke_opts) do
    invoke_opts
    |> Keyword.put(:allowed_operations, [@session_stop_capability_id])
    |> Keyword.update(:sandbox, %{allowed_tools: [@session_stop_capability_id]}, fn
      %{} = sandbox -> Map.put(sandbox, :allowed_tools, [@session_stop_capability_id])
      other -> other
    end)
  end

  defp next_codex_turn_step(turn_index, turn_limit, attempts, result) do
    if continue_after_turn?(turn_index, turn_limit, result) do
      {:cont, {:ok, attempts, result}}
    else
      {:halt, {:ok, attempts, result}}
    end
  end

  defp codex_turn_limit(attrs) do
    max_turns = requested_max_turns(attrs)

    if continuation_enabled?(attrs) do
      max_turns
    else
      1
    end
  end

  defp requested_max_turns(attrs) do
    case map_value(attrs, :max_turns) do
      value when is_integer(value) and value > 0 ->
        value

      value when is_binary(value) ->
        case Integer.parse(value) do
          {parsed, ""} when parsed > 0 -> parsed
          _other -> 1
        end

      _other ->
        1
    end
  end

  defp continuation_enabled?(attrs) do
    policy = map_value(attrs, :continuation_policy) || %{}
    mode = map_value(policy, :mode)

    requested_max_turns(attrs) > 1 and
      (mode in [:until_max_turns, "until_max_turns", :while_active, "while_active"] or
         truthy?(map_value(policy, :enabled?)) or truthy?(map_value(policy, :active_state?)))
  end

  defp continue_after_turn?(turn_index, turn_limit, result) do
    turn_index < turn_limit and output_status(map_value(result, :output) || %{}) == "completed"
  end

  defp continuation_guidance(attrs, turn_index, max_turns) do
    non_empty(map_value(attrs, :continuation_input_body)) ||
      """
      Continuation guidance:

      - The previous Codex turn completed normally, but the subject is still active.
      - This is continuation turn ##{turn_index} of #{max_turns} for the current agent run.
      - Resume from the current workspace and workpad state instead of restarting from scratch.
      - The original task instructions and prior turn context are already present in this thread, so do not restate them before acting.
      - Focus on the remaining work and do not end the turn while the subject stays active unless you are truly blocked.
      """
      |> String.trim()
  end

  defp continuation_metadata(previous_result, _turn_index, _max_turns) do
    previous_output = previous_result |> map_value(:output) || %{}
    provider_session_id = map_value(previous_output, :provider_session_id)

    %{
      strategy: if(present_binary?(provider_session_id), do: :exact, else: :latest),
      provider_session_id: provider_session_id
    }
    |> compact_map()
  end

  defp codex_workspace_root(opts) do
    sandbox_file_scope =
      opts
      |> Keyword.get(:sandbox, %{})
      |> map_value(:file_scope)

    Keyword.get(opts, :cwd) || sandbox_file_scope ||
      @codex_workspace_root
  end

  defp authority_metadata(attrs) do
    authority_metadata(attrs, 1, requested_max_turns(attrs))
  end

  defp authority_metadata(attrs, turn_index, max_turns) do
    %{
      "authority_context_ref" => map_value(attrs, :authority_context_ref),
      "capability_id" => @capability_id,
      "idempotency_key" => map_value(attrs, :idempotency_key),
      "trace_id" => map_value(attrs, :trace_id),
      "turn_index" => turn_index,
      "max_turns" => max_turns
    }
    |> compact_map()
  end

  defp projection(
         attrs,
         turn_attempts,
         invoke_opts,
         before_run_receipts,
         after_run_receipts,
         session_stop,
         opts
       ) do
    first_turn_attempt = List.first(turn_attempts)
    final_turn_attempt = List.last(turn_attempts)
    result = map_value(final_turn_attempt, :result) || %{}
    output = map_value(result, :output) || %{}
    run_ref = map_value(attrs, :run_ref)
    max_turns = requested_max_turns(attrs)
    actual_turn_count = length(turn_attempts)
    workflow_ref = "workflow://codex-agent-runtime/#{ref_suffix(run_ref)}"
    first_turn_ref = turn_ref(run_ref, 1)

    first_run_id = turn_run_id(first_turn_attempt, run_ref)
    first_lower_request_ref = lower_request_ref(first_run_id, @capability_id)

    session_start =
      first_turn_attempt
      |> turn_attempt_result()
      |> lower_runtime_events(opts)
      |> session_start_evidence(first_run_id)

    first_prompt =
      first_prompt_evidence(
        attrs,
        turn_attempt_input(first_turn_attempt),
        first_lower_request_ref
      )

    continuation = continuation_evidence(attrs, turn_attempts, max_turns)

    app_server_protocol =
      app_server_protocol_evidence(
        turn_attempt_input(first_turn_attempt),
        turn_attempt_output(first_turn_attempt),
        session_start,
        first_lower_request_ref,
        turn_lower_receipt_ref(first_turn_attempt, first_lower_request_ref)
      )

    event_stream = codex_event_stream_evidence(attrs, turn_attempts, opts, output)
    token_accounting = codex_token_accounting_evidence(event_stream)
    observed_at = observed_now(opts)
    base_status = terminal_status(output, event_stream)

    stall_decision =
      codex_stall_decision(
        attrs,
        turn_attempts,
        event_stream,
        base_status,
        observed_at,
        workflow_ref
      )

    status = stalled_runtime_state(stall_decision) || base_status
    session_start_event_count = if session_start, do: 1, else: 0
    first_prompt_event_count = if first_prompt, do: 1, else: 0
    app_server_protocol_event_count = if app_server_protocol, do: 1, else: 0
    continuation_event_count = continuation_event_count(continuation)
    event_stream_event_count = codex_event_stream_event_count(event_stream)
    first_prompt_event_seq = session_start_event_count + 1
    app_server_protocol_event_seq = session_start_event_count + first_prompt_event_count + 1
    continuation_event_seq = app_server_protocol_event_seq + app_server_protocol_event_count
    event_stream_event_seq = continuation_event_seq + continuation_event_count
    session_stop_event_count = if session_stop, do: 1, else: 0
    session_stop_event_seq = event_stream_event_seq + event_stream_event_count

    terminal_event_seq =
      session_start_event_count + first_prompt_event_count + app_server_protocol_event_count +
        continuation_event_count + event_stream_event_count + session_stop_event_count + 1

    after_run_event_seq = terminal_event_seq + 1

    %{
      run_ref: run_ref,
      subject_ref: map_value(attrs, :subject_ref),
      workflow_ref: workflow_ref,
      status: status,
      terminal_state: status,
      token_totals: codex_token_totals(token_accounting),
      turn_states:
        Enum.map(
          turn_attempts,
          &turn_state(
            attrs,
            &1,
            session_start,
            first_prompt,
            app_server_protocol,
            continuation,
            event_stream
          )
        ),
      extensions:
        session_start_extensions(session_start)
        |> Map.merge(first_prompt_extensions(first_prompt))
        |> Map.merge(app_server_protocol_extensions(app_server_protocol))
        |> Map.merge(continuation_extensions(continuation))
        |> Map.merge(codex_event_stream_extensions(event_stream))
        |> Map.merge(codex_token_accounting_extensions(token_accounting))
        |> Map.merge(session_stop_extensions(session_stop)),
      action_receipts:
        session_start_action_receipts(attrs, session_start) ++
          Enum.map(turn_attempts, &action_receipt(attrs, &1, event_stream)) ++
          session_stop_action_receipts(attrs, session_stop),
      runtime_events:
        hook_events(
          attrs,
          workflow_ref,
          first_turn_ref,
          observed_at,
          :before_run,
          0,
          before_run_receipts
        ) ++
          session_start_runtime_events(
            session_start,
            attrs,
            workflow_ref,
            first_turn_ref,
            observed_at
          ) ++
          first_prompt_runtime_events(
            first_prompt,
            attrs,
            workflow_ref,
            first_turn_ref,
            observed_at,
            first_prompt_event_seq
          ) ++
          app_server_protocol_runtime_events(
            app_server_protocol,
            attrs,
            workflow_ref,
            first_turn_ref,
            observed_at,
            app_server_protocol_event_seq
          ) ++
          continuation_runtime_events(
            continuation,
            attrs,
            workflow_ref,
            observed_at,
            continuation_event_seq
          ) ++
          codex_event_stream_runtime_events(
            event_stream,
            attrs,
            workflow_ref,
            observed_at,
            event_stream_event_seq
          ) ++
          session_stop_runtime_events(
            session_stop,
            attrs,
            workflow_ref,
            turn_ref(run_ref, actual_turn_count),
            observed_at,
            session_stop_event_seq
          ) ++
          [
            %{
              event_ref: "event://codex-agent-runtime/#{ref_suffix(run_ref)}/terminal",
              event_seq: terminal_event_seq,
              event_kind: "run.terminal",
              observed_at: observed_at,
              tenant_ref: Keyword.get(invoke_opts, :tenant_id),
              subject_ref: map_value(attrs, :subject_ref),
              run_ref: run_ref,
              workflow_ref: workflow_ref,
              turn_ref: turn_ref(run_ref, actual_turn_count),
              level: terminal_level(status),
              message_summary: terminal_message_summary(status)
            }
          ] ++
          hook_events(
            attrs,
            workflow_ref,
            first_turn_ref,
            observed_at,
            :after_run,
            after_run_event_seq,
            after_run_receipts
          ),
      budget_state: %{"turns_remaining" => max(max_turns - actual_turn_count, 0)},
      candidate_fact_refs:
        Enum.map(
          1..actual_turn_count,
          &"candidate-fact://codex-agent-runtime/#{ref_suffix(run_ref)}/#{&1}"
        ),
      memory_proof_refs: [],
      receipt_ref_set: %{
        lower_request_refs:
          compact_list(
            [session_start_lower_request_ref(session_start)] ++
              Enum.map(turn_attempts, &lower_request_ref(&1)) ++
              [session_stop_lower_request_ref(session_stop)]
          ),
        lower_receipt_refs:
          compact_list(
            [session_start_lower_receipt_ref(session_start)] ++
              Enum.map(turn_attempts, &lower_receipt_ref(&1)) ++
              [session_stop_lower_receipt_ref(session_stop)]
          ),
        workspace_hook_refs: Enum.map(before_run_receipts ++ after_run_receipts, & &1.hook_ref)
      }
    }
    |> put_map_present(:stall_decision, stall_decision)
  end

  defp turn_state(
         attrs,
         turn_attempt,
         session_start,
         first_prompt,
         app_server_protocol,
         continuation,
         event_stream
       ) do
    turn_index = map_value(turn_attempt, :turn_index)
    run_ref = map_value(attrs, :run_ref)
    result = turn_attempt_result(turn_attempt)
    output = map_value(result, :output) || %{}
    lower_request_ref = lower_request_ref(turn_attempt)
    lower_receipt_ref = lower_receipt_ref(turn_attempt)
    status = turn_status(output, event_stream, turn_index)

    %{
      turn_ref: turn_ref(run_ref, turn_index),
      turn_index: turn_index,
      state: status,
      status: status,
      session_ref: session_ref(output, run_ref),
      operation: @capability_id,
      credential_redeemed?: true,
      provider_request_sent?: true,
      provider_response_received?: true,
      lower_request_ref: lower_request_ref,
      lower_receipt_ref: lower_receipt_ref,
      effect_request_ref: lower_request_ref,
      connector_manifest_ref: codex_connector_manifest_ref(),
      capability_negotiation_ref: capability_negotiation_ref(lower_request_ref),
      evidence_profile_ref: codex_evidence_profile_ref(run_ref),
      operation_receipt:
        codex_operation_receipt(
          attrs,
          @capability_id,
          lower_request_ref,
          lower_receipt_ref,
          status,
          output_artifact_refs(result),
          map_value(turn_attempt, :authority_handoff)
        )
    }
    |> Map.merge(
      ProviderAuthorityAdmission.result_fields(map_value(turn_attempt, :authority_handoff))
    )
    |> Map.merge(if(turn_index == 1, do: session_start_turn_fields(session_start), else: %{}))
    |> Map.merge(if(turn_index == 1, do: first_prompt_turn_fields(first_prompt), else: %{}))
    |> Map.merge(
      if(turn_index == 1, do: app_server_protocol_turn_fields(app_server_protocol), else: %{})
    )
    |> Map.merge(continuation_turn_fields(continuation, turn_index))
    |> Map.merge(codex_event_stream_turn_fields(event_stream, turn_index))
  end

  defp action_receipt(attrs, turn_attempt, event_stream) do
    output = turn_attempt_output(turn_attempt)
    turn_index = map_value(turn_attempt, :turn_index)
    status = turn_status(output, event_stream, turn_index)
    lower_request_ref = lower_request_ref(turn_attempt)
    lower_receipt_ref = lower_receipt_ref(turn_attempt)

    %{
      operation: @capability_id,
      status: action_receipt_status(status),
      lower_request_ref: lower_request_ref,
      lower_receipt_ref: lower_receipt_ref,
      output_artifact_refs: output_artifact_refs(turn_attempt_result(turn_attempt)),
      operation_receipt:
        codex_operation_receipt(
          attrs,
          @capability_id,
          lower_request_ref,
          lower_receipt_ref,
          status,
          output_artifact_refs(turn_attempt_result(turn_attempt)),
          map_value(turn_attempt, :authority_handoff)
        )
    }
  end

  defp turn_ref(run_ref, turn_index),
    do: "turn://codex-agent-runtime/#{ref_suffix(run_ref)}/#{turn_index}"

  defp lower_request_ref(turn_attempt) do
    lower_request_ref(turn_run_id(turn_attempt, nil), @capability_id)
  end

  defp lower_receipt_ref(turn_attempt) do
    turn_lower_receipt_ref(turn_attempt, lower_request_ref(turn_attempt))
  end

  defp turn_lower_receipt_ref(turn_attempt, lower_request_ref) do
    turn_attempt
    |> turn_attempt_attempt()
    |> lower_receipt_ref(lower_request_ref)
  end

  defp turn_run_id(turn_attempt, run_ref) do
    turn_attempt
    |> turn_attempt_result()
    |> map_value(:run)
    |> case do
      %{} = run -> map_value(run, :run_id)
      _missing -> nil
    end
    |> case do
      value when is_binary(value) and value != "" -> value
      _missing -> ref_suffix(run_ref || "run")
    end
  end

  defp turn_attempt_result(turn_attempt), do: map_value(turn_attempt, :result) || %{}
  defp turn_attempt_input(turn_attempt), do: map_value(turn_attempt, :input) || %{}

  defp turn_attempt_output(turn_attempt),
    do: map_value(turn_attempt_result(turn_attempt), :output) || %{}

  defp turn_attempt_attempt(turn_attempt),
    do: map_value(turn_attempt_result(turn_attempt), :attempt) || %{}

  defp lower_request_ref(run_id, capability_id), do: "lower-request://#{run_id}/#{capability_id}"

  defp lower_runtime_events(result, opts) do
    case result |> map_value(:events) |> normalize_lower_events() do
      [] ->
        run = map_value(result, :run) || %{}
        run_id = map_value(run, :run_id)

        if present_binary?(run_id) do
          opts
          |> Keyword.get(:events_fun, &safe_lower_events/1)
          |> call_events_fun(run_id)
          |> normalize_lower_events()
        else
          []
        end

      events ->
        events
    end
  end

  defp call_events_fun(events_fun, run_id) when is_function(events_fun, 1),
    do: events_fun.(run_id)

  defp call_events_fun(_events_fun, _run_id), do: []

  defp safe_lower_events(run_id) do
    if Code.ensure_loaded?(V2) and function_exported?(V2, :events, 1) do
      V2.events(run_id)
    else
      []
    end
  rescue
    _error -> []
  catch
    _kind, _reason -> []
  end

  defp normalize_lower_events({:ok, events}), do: normalize_lower_events(events)
  defp normalize_lower_events(events) when is_list(events), do: events
  defp normalize_lower_events(_events), do: []

  defp session_start_evidence(events, run_id) do
    events
    |> Enum.find(&session_start_event?/1)
    |> case do
      nil ->
        nil

      event ->
        lifecycle = event |> map_value(:type) |> session_start_lifecycle()
        session_id = lower_session_id(event)

        if present_binary?(session_id) and present_binary?(lifecycle) do
          lower_request_ref = lower_request_ref(run_id, @session_start_capability_id)

          %{
            operation: @session_start_capability_id,
            lifecycle: lifecycle,
            runtime_control_session_id: session_id,
            runtime_control_session_ref: runtime_control_session_ref(session_id),
            lower_event_ref: lower_event_ref(event, run_id, lifecycle, session_id),
            lower_request_ref: lower_request_ref,
            lower_receipt_ref: session_start_receipt_ref(run_id, session_id, lifecycle)
          }
        end
    end
  end

  defp session_start_event?(event) do
    event_type = map_value(event, :type)
    event_type in ["session.started", "session.reused"]
  end

  defp session_start_lifecycle("session.started"), do: "started"
  defp session_start_lifecycle("session.reused"), do: "reused"
  defp session_start_lifecycle(_event_type), do: nil

  defp lower_session_id(event) do
    payload = map_value(event, :payload) || %{}

    map_value(event, :session_id) ||
      map_value(event, :runtime_ref_id) ||
      map_value(payload, :session_id) ||
      map_value(payload, :runtime_ref_id)
  end

  defp lower_event_ref(event, run_id, lifecycle, session_id) do
    map_value(event, :event_id) ||
      map_value(event, :event_ref) ||
      "event://#{run_id}/#{@session_start_capability_id}/#{session_id}/#{lifecycle}"
  end

  defp runtime_control_session_ref(session_id), do: "runtime-session://#{session_id}"

  defp session_start_receipt_ref(run_id, session_id, lifecycle),
    do: "lower-receipt://#{run_id}/#{@session_start_capability_id}/#{session_id}/#{lifecycle}"

  defp session_start_event_kind(%{lifecycle: "started"}), do: "codex.session.started"
  defp session_start_event_kind(%{lifecycle: "reused"}), do: "codex.session.reused"

  defp session_start_extensions(nil), do: %{}

  defp session_start_extensions(evidence) do
    %{
      "codex_app_server_session_start" => %{
        "confirmed?" => true,
        "operation" => evidence.operation,
        "lifecycle" => evidence.lifecycle,
        "runtime_control_session_id" => evidence.runtime_control_session_id,
        "runtime_control_session_ref" => evidence.runtime_control_session_ref,
        "lower_event_ref" => evidence.lower_event_ref,
        "lower_request_ref" => evidence.lower_request_ref,
        "lower_receipt_ref" => evidence.lower_receipt_ref
      }
    }
  end

  defp session_start_turn_fields(nil), do: %{}

  defp session_start_turn_fields(evidence) do
    %{
      session_start_confirmed?: true,
      runtime_control_session_ref: evidence.runtime_control_session_ref,
      session_start_event_kind: session_start_event_kind(evidence),
      session_start_lower_request_ref: evidence.lower_request_ref,
      session_start_lower_receipt_ref: evidence.lower_receipt_ref
    }
  end

  defp session_start_action_receipts(_attrs, nil), do: []

  defp session_start_action_receipts(attrs, evidence) do
    [
      %{
        operation: evidence.operation,
        status: :succeeded,
        lower_request_ref: evidence.lower_request_ref,
        lower_receipt_ref: evidence.lower_receipt_ref,
        runtime_control_session_ref: evidence.runtime_control_session_ref,
        operation_receipt:
          codex_operation_receipt(
            attrs,
            evidence.operation,
            evidence.lower_request_ref,
            evidence.lower_receipt_ref,
            evidence.lifecycle,
            [],
            nil
          )
      }
    ]
  end

  defp session_start_runtime_events(nil, _attrs, _workflow_ref, _turn_ref, _observed_at), do: []

  defp session_start_runtime_events(evidence, attrs, workflow_ref, turn_ref, observed_at) do
    [
      %{
        event_ref:
          "event://codex-agent-runtime/#{ref_suffix(map_value(attrs, :run_ref))}/session-start",
        event_seq: 1,
        event_kind: session_start_event_kind(evidence),
        observed_at: observed_at,
        tenant_ref: map_value(attrs, :tenant_id) || map_value(attrs, :tenant_ref),
        subject_ref: map_value(attrs, :subject_ref),
        run_ref: map_value(attrs, :run_ref),
        workflow_ref: workflow_ref,
        session_ref: evidence.runtime_control_session_ref,
        turn_ref: turn_ref,
        level: "info",
        message_summary: "codex session start confirmed",
        extensions: %{
          lower_event_ref: evidence.lower_event_ref,
          lower_request_ref: evidence.lower_request_ref,
          lower_receipt_ref: evidence.lower_receipt_ref,
          lifecycle: evidence.lifecycle
        }
      }
    ]
  end

  defp session_start_lower_request_ref(nil), do: nil
  defp session_start_lower_request_ref(evidence), do: evidence.lower_request_ref

  defp session_start_lower_receipt_ref(nil), do: nil
  defp session_start_lower_receipt_ref(evidence), do: evidence.lower_receipt_ref

  defp session_stop_evidence(session_start, result, authority_handoff) do
    output = map_value(result, :output) || %{}
    status = session_stop_status(output)

    session_id =
      non_empty(map_value(output, :session_id)) ||
        session_start.runtime_control_session_id

    run_id = session_stop_run_id(result, session_id)
    lower_request_ref = lower_request_ref(run_id, @session_stop_capability_id)

    %{
      confirmed?: status == "stopped",
      operation: @session_stop_capability_id,
      status: status,
      runtime_control_session_id: session_id,
      runtime_control_session_ref: runtime_control_session_ref(session_id),
      lower_request_ref: lower_request_ref,
      lower_receipt_ref: session_stop_receipt_ref(run_id, session_id, status)
    }
    |> Map.merge(ProviderAuthorityAdmission.result_fields(authority_handoff))
  end

  defp session_stop_failure_evidence(session_start, authority_handoff) do
    session_id = session_start.runtime_control_session_id
    run_id = "session-stop-#{ref_suffix(session_id)}"
    lower_request_ref = lower_request_ref(run_id, @session_stop_capability_id)

    %{
      confirmed?: false,
      operation: @session_stop_capability_id,
      status: "failed",
      runtime_control_session_id: session_id,
      runtime_control_session_ref: runtime_control_session_ref(session_id),
      lower_request_ref: lower_request_ref,
      lower_receipt_ref: session_stop_receipt_ref(run_id, session_id, "failed")
    }
    |> Map.merge(ProviderAuthorityAdmission.result_fields(authority_handoff))
  end

  defp session_stop_status(output) do
    map_value(output, :status)
    |> case do
      nil -> map_value(output, :state)
      status -> status
    end
    |> event_token()
    |> case do
      nil -> "stopped"
      "" -> "stopped"
      status -> status
    end
  end

  defp session_stop_run_id(result, session_id) do
    result
    |> map_value(:run)
    |> map_value(:run_id)
    |> non_empty()
    |> case do
      nil -> "session-stop-#{ref_suffix(session_id)}"
      run_id -> run_id
    end
  end

  defp session_stop_receipt_ref(run_id, session_id, status),
    do: "lower-receipt://#{run_id}/#{@session_stop_capability_id}/#{session_id}/#{status}"

  defp session_stop_extensions(nil), do: %{}

  defp session_stop_extensions(evidence) do
    %{
      "codex_app_server_session_stop" => %{
        "confirmed?" => evidence.confirmed?,
        "operation" => evidence.operation,
        "status" => evidence.status,
        "runtime_control_session_id" => evidence.runtime_control_session_id,
        "runtime_control_session_ref" => evidence.runtime_control_session_ref,
        "lower_request_ref" => evidence.lower_request_ref,
        "lower_receipt_ref" => evidence.lower_receipt_ref
      }
    }
  end

  defp session_stop_action_receipts(_attrs, nil), do: []

  defp session_stop_action_receipts(attrs, evidence) do
    [
      %{
        operation: evidence.operation,
        status: action_receipt_status(evidence.status),
        lower_request_ref: evidence.lower_request_ref,
        lower_receipt_ref: evidence.lower_receipt_ref,
        runtime_control_session_ref: evidence.runtime_control_session_ref,
        operation_receipt:
          codex_operation_receipt(
            attrs,
            evidence.operation,
            evidence.lower_request_ref,
            evidence.lower_receipt_ref,
            evidence.status,
            [],
            evidence
          )
      }
    ]
  end

  defp session_stop_runtime_events(
         nil,
         _attrs,
         _workflow_ref,
         _turn_ref,
         _observed_at,
         _event_seq
       ),
       do: []

  defp session_stop_runtime_events(
         evidence,
         attrs,
         workflow_ref,
         turn_ref,
         observed_at,
         event_seq
       ) do
    [
      %{
        event_ref:
          "event://codex-agent-runtime/#{ref_suffix(map_value(attrs, :run_ref))}/session-stop",
        event_seq: event_seq,
        event_kind: "codex.session.stopped",
        observed_at: observed_at,
        tenant_ref: map_value(attrs, :tenant_id) || map_value(attrs, :tenant_ref),
        subject_ref: map_value(attrs, :subject_ref),
        run_ref: map_value(attrs, :run_ref),
        workflow_ref: workflow_ref,
        session_ref: evidence.runtime_control_session_ref,
        turn_ref: turn_ref,
        level: if(evidence.confirmed?, do: "info", else: "warning"),
        message_summary: "codex session stop confirmed",
        extensions: %{
          lower_request_ref: evidence.lower_request_ref,
          lower_receipt_ref: evidence.lower_receipt_ref,
          status: evidence.status
        }
      }
    ]
  end

  defp session_stop_lower_request_ref(nil), do: nil
  defp session_stop_lower_request_ref(evidence), do: evidence.lower_request_ref

  defp session_stop_lower_receipt_ref(nil), do: nil
  defp session_stop_lower_receipt_ref(evidence), do: evidence.lower_receipt_ref

  defp first_prompt_evidence(attrs, input, lower_request_ref) do
    input
    |> map_value(:prompt)
    |> first_prompt_evidence_for_prompt(attrs, lower_request_ref)
  end

  defp first_prompt_evidence_for_prompt(prompt, _attrs, _lower_request_ref)
       when not is_binary(prompt),
       do: nil

  defp first_prompt_evidence_for_prompt(prompt, attrs, lower_request_ref) do
    case present_binary?(prompt) do
      true -> build_first_prompt_evidence(prompt, attrs, lower_request_ref)
      false -> nil
    end
  end

  defp build_first_prompt_evidence(prompt, attrs, lower_request_ref) do
    computed_hash = prompt_hash(prompt)
    hash = first_prompt_hash(attrs, computed_hash)
    caller_supplied? = present_binary?(map_value(attrs, :initial_input_body))

    %{
      confirmed?: true,
      prompt_ref: first_prompt_ref(attrs),
      prompt_hash: hash,
      prompt_hash_verified?: hash == computed_hash,
      prompt_source_ref: first_prompt_source_ref(attrs),
      prompt_rendered?: first_prompt_rendered?(attrs, caller_supplied?),
      prompt_body_redacted?: true,
      prompt_body_included?: false,
      prompt_source: first_prompt_source(caller_supplied?),
      lower_request_ref: lower_request_ref
    }
    |> compact_map()
  end

  defp first_prompt_hash(attrs, computed_hash) do
    case first_non_empty(attrs, [:initial_input_hash, :prompt_hash]) do
      nil -> computed_hash
      hash -> hash
    end
  end

  defp first_prompt_ref(attrs),
    do: first_non_empty(attrs, [:initial_input_ref, :prompt_ref, :objective])

  defp first_prompt_source_ref(attrs),
    do: first_non_empty(attrs, [:initial_input_source_ref, :prompt_source_ref])

  defp first_prompt_rendered?(attrs, caller_supplied?) do
    [
      caller_supplied?
      | Enum.map([:initial_input_rendered?, :prompt_rendered?], &truthy?(map_value(attrs, &1)))
    ]
    |> Enum.any?()
  end

  defp first_prompt_source(true), do: "caller_supplied"
  defp first_prompt_source(false), do: "runtime_default"

  defp first_non_empty(attrs, keys) do
    Enum.find_value(keys, &non_empty(map_value(attrs, &1)))
  end

  defp first_prompt_extensions(nil), do: %{}

  defp first_prompt_extensions(evidence) do
    %{
      "codex_first_prompt" =>
        %{
          "confirmed?" => evidence.confirmed?,
          "prompt_ref" => map_value(evidence, :prompt_ref),
          "prompt_hash" => map_value(evidence, :prompt_hash),
          "prompt_hash_verified?" => map_value(evidence, :prompt_hash_verified?),
          "prompt_source_ref" => map_value(evidence, :prompt_source_ref),
          "prompt_rendered?" => map_value(evidence, :prompt_rendered?),
          "prompt_body_redacted?" => map_value(evidence, :prompt_body_redacted?),
          "prompt_body_included?" => map_value(evidence, :prompt_body_included?),
          "prompt_source" => map_value(evidence, :prompt_source),
          "lower_request_ref" => map_value(evidence, :lower_request_ref)
        }
        |> compact_map()
    }
  end

  defp first_prompt_turn_fields(nil), do: %{}

  defp first_prompt_turn_fields(evidence) do
    %{
      first_prompt_confirmed?: evidence.confirmed?,
      prompt_ref: map_value(evidence, :prompt_ref),
      prompt_hash: map_value(evidence, :prompt_hash),
      prompt_hash_verified?: map_value(evidence, :prompt_hash_verified?),
      prompt_source_ref: map_value(evidence, :prompt_source_ref),
      prompt_rendered?: map_value(evidence, :prompt_rendered?),
      prompt_body_redacted?: map_value(evidence, :prompt_body_redacted?),
      prompt_body_included?: map_value(evidence, :prompt_body_included?),
      prompt_source: map_value(evidence, :prompt_source),
      first_prompt_lower_request_ref: map_value(evidence, :lower_request_ref)
    }
    |> compact_map()
  end

  defp first_prompt_runtime_events(
         nil,
         _attrs,
         _workflow_ref,
         _turn_ref,
         _observed_at,
         _event_seq
       ),
       do: []

  defp first_prompt_runtime_events(
         evidence,
         attrs,
         workflow_ref,
         turn_ref,
         observed_at,
         event_seq
       ) do
    [
      %{
        event_ref:
          "event://codex-agent-runtime/#{ref_suffix(map_value(attrs, :run_ref))}/first-prompt",
        event_seq: event_seq,
        event_kind: "codex.first_prompt.confirmed",
        observed_at: observed_at,
        tenant_ref: map_value(attrs, :tenant_id) || map_value(attrs, :tenant_ref),
        subject_ref: map_value(attrs, :subject_ref),
        run_ref: map_value(attrs, :run_ref),
        workflow_ref: workflow_ref,
        turn_ref: turn_ref,
        level: "info",
        message_summary: "codex first prompt confirmed",
        extensions:
          %{
            prompt_ref: map_value(evidence, :prompt_ref),
            prompt_hash: map_value(evidence, :prompt_hash),
            prompt_hash_verified?: map_value(evidence, :prompt_hash_verified?),
            prompt_source_ref: map_value(evidence, :prompt_source_ref),
            prompt_rendered?: map_value(evidence, :prompt_rendered?),
            prompt_body_redacted?: map_value(evidence, :prompt_body_redacted?),
            prompt_body_included?: map_value(evidence, :prompt_body_included?),
            prompt_source: map_value(evidence, :prompt_source),
            lower_request_ref: map_value(evidence, :lower_request_ref)
          }
          |> compact_map()
      }
    ]
  end

  defp continuation_evidence(_attrs, turn_attempts, _max_turns) when length(turn_attempts) <= 1,
    do: nil

  defp continuation_evidence(attrs, turn_attempts, max_turns) do
    actual_turn_count = length(turn_attempts)
    continuation_turn_count = max(actual_turn_count - 1, 0)

    %{
      confirmed?: continuation_turn_count > 0,
      turn_count: actual_turn_count,
      continuation_turn_count: continuation_turn_count,
      max_turns: max_turns,
      max_turns_reached?: actual_turn_count >= max_turns,
      continuation_guidance_ref: first_non_empty(attrs, [:continuation_input_ref]),
      continuation_guidance_hash: first_non_empty(attrs, [:continuation_input_hash]),
      continuation_guidance_source_ref: first_non_empty(attrs, [:continuation_input_source_ref]),
      continuation_guidance_rendered?: truthy?(map_value(attrs, :continuation_input_rendered?)),
      continuation_prompt_body_redacted?: true,
      continuation_prompt_body_included?: false,
      first_prompt_reused_on_continuation?: false,
      lower_request_refs: Enum.map(Enum.drop(turn_attempts, 1), &lower_request_ref/1),
      lower_receipt_refs: Enum.map(Enum.drop(turn_attempts, 1), &lower_receipt_ref/1)
    }
    |> compact_map()
  end

  defp continuation_extensions(nil), do: %{}

  defp continuation_extensions(evidence) do
    %{
      "codex_continuation" =>
        %{
          "confirmed?" => evidence.confirmed?,
          "turn_count" => map_value(evidence, :turn_count),
          "continuation_turn_count" => map_value(evidence, :continuation_turn_count),
          "max_turns" => map_value(evidence, :max_turns),
          "max_turns_reached?" => map_value(evidence, :max_turns_reached?),
          "continuation_guidance_ref" => map_value(evidence, :continuation_guidance_ref),
          "continuation_guidance_hash" => map_value(evidence, :continuation_guidance_hash),
          "continuation_guidance_source_ref" =>
            map_value(evidence, :continuation_guidance_source_ref),
          "continuation_guidance_rendered?" =>
            map_value(evidence, :continuation_guidance_rendered?),
          "continuation_prompt_body_redacted?" =>
            map_value(evidence, :continuation_prompt_body_redacted?),
          "continuation_prompt_body_included?" =>
            map_value(evidence, :continuation_prompt_body_included?),
          "first_prompt_reused_on_continuation?" =>
            map_value(evidence, :first_prompt_reused_on_continuation?),
          "lower_request_refs" => map_value(evidence, :lower_request_refs),
          "lower_receipt_refs" => map_value(evidence, :lower_receipt_refs)
        }
        |> compact_map()
    }
  end

  defp continuation_turn_fields(nil, _turn_index), do: %{}
  defp continuation_turn_fields(_evidence, 1), do: %{}

  defp continuation_turn_fields(evidence, _turn_index) do
    %{
      continuation?: true,
      continuation_guidance_ref: map_value(evidence, :continuation_guidance_ref),
      continuation_guidance_hash: map_value(evidence, :continuation_guidance_hash),
      continuation_guidance_source_ref: map_value(evidence, :continuation_guidance_source_ref),
      continuation_guidance_rendered?: map_value(evidence, :continuation_guidance_rendered?),
      continuation_prompt_body_redacted?:
        map_value(evidence, :continuation_prompt_body_redacted?),
      continuation_prompt_body_included?:
        map_value(evidence, :continuation_prompt_body_included?),
      first_prompt_reused_on_continuation?:
        map_value(evidence, :first_prompt_reused_on_continuation?)
    }
    |> compact_map()
  end

  defp continuation_event_count(nil), do: 0
  defp continuation_event_count(evidence), do: map_value(evidence, :continuation_turn_count) || 0

  defp continuation_runtime_events(nil, _attrs, _workflow_ref, _observed_at, _event_seq),
    do: []

  defp continuation_runtime_events(evidence, attrs, workflow_ref, observed_at, event_seq) do
    continuation_count = continuation_event_count(evidence)

    1..continuation_count
    |> Enum.map(fn offset ->
      turn_index = offset + 1

      %{
        event_ref:
          "event://codex-agent-runtime/#{ref_suffix(map_value(attrs, :run_ref))}/continuation-#{turn_index}",
        event_seq: event_seq + offset - 1,
        event_kind: "codex.continuation_turn.confirmed",
        observed_at: observed_at,
        tenant_ref: map_value(attrs, :tenant_id) || map_value(attrs, :tenant_ref),
        subject_ref: map_value(attrs, :subject_ref),
        run_ref: map_value(attrs, :run_ref),
        workflow_ref: workflow_ref,
        turn_ref: turn_ref(map_value(attrs, :run_ref), turn_index),
        level: "info",
        message_summary: "codex continuation turn confirmed",
        extensions:
          %{
            turn_index: turn_index,
            continuation_guidance_ref: map_value(evidence, :continuation_guidance_ref),
            continuation_guidance_hash: map_value(evidence, :continuation_guidance_hash),
            continuation_guidance_source_ref:
              map_value(evidence, :continuation_guidance_source_ref),
            continuation_guidance_rendered?:
              map_value(evidence, :continuation_guidance_rendered?),
            continuation_prompt_body_redacted?:
              map_value(evidence, :continuation_prompt_body_redacted?),
            continuation_prompt_body_included?:
              map_value(evidence, :continuation_prompt_body_included?),
            first_prompt_reused_on_continuation?:
              map_value(evidence, :first_prompt_reused_on_continuation?)
          }
          |> compact_map()
      }
    end)
  end

  defp codex_event_stream_evidence(_attrs, turn_attempts, opts, output) do
    events =
      turn_attempts
      |> Enum.flat_map(fn turn_attempt ->
        turn_attempt
        |> lower_events_for_turn(opts)
        |> Enum.flat_map(&normalize_codex_lower_event(&1, turn_attempt))
      end)
      |> Enum.with_index(1)
      |> Enum.map(fn {event, event_index} -> Map.put(event, :event_index, event_index) end)

    case events do
      [] -> nil
      _events -> codex_event_stream_summary(events, output)
    end
  end

  defp lower_events_for_turn(turn_attempt, opts) do
    turn_attempt
    |> turn_attempt_result()
    |> lower_runtime_events(opts)
  end

  defp normalize_codex_lower_event(event, turn_attempt) do
    payload = map_value(event, :payload) || %{}

    case codex_event_category(event, payload) do
      nil ->
        []

      category ->
        [
          event
          |> codex_event_base(turn_attempt, category)
          |> Map.merge(codex_event_category_fields(category, payload, event))
          |> compact_map()
        ]
    end
  end

  defp codex_event_base(event, turn_attempt, category) do
    %{
      category: category,
      lower_event_ref: codex_lower_event_ref(event, turn_attempt, category),
      turn_index: map_value(turn_attempt, :turn_index),
      event_kind: codex_event_kind(category),
      observed_at: lower_event_observed_at(event),
      status: codex_event_status(category),
      level: codex_event_level(category),
      message_summary: codex_event_message_summary(category)
    }
  end

  defp codex_event_category_fields(:token_usage, payload, _event) do
    case token_usage_evidence(payload) do
      nil ->
        %{}

      token_usage ->
        %{
          token_usage: token_usage,
          token_scope_ref: codex_token_scope_ref(payload)
        }
        |> compact_map()
    end
  end

  defp codex_event_category_fields(:turn_completed, payload, event) do
    case turn_completed_token_usage_evidence(payload) do
      nil ->
        %{}

      token_usage ->
        %{
          token_usage: token_usage,
          token_scope_ref: codex_token_scope_ref(payload, event)
        }
        |> compact_map()
    end
  end

  defp codex_event_category_fields(:rate_limits, payload, _event),
    do: rate_limit_evidence(payload) || %{}

  defp codex_event_category_fields(:agent_message, _payload, _event) do
    %{
      last_message?: true,
      body_redacted?: true,
      body_included?: false
    }
  end

  defp codex_event_category_fields(_category, _payload, _event), do: %{}

  defp codex_event_category(event, payload) do
    event_name = event_token(map_value(event, :event))
    event_type = event_token(map_value(event, :type) || map_value(event, :event_kind))
    method = codex_event_method(event, payload)

    codex_event_name_category(event_name) ||
      codex_event_type_category(event_type) ||
      codex_terminal_method_category(method) ||
      codex_interaction_method_category(method) ||
      codex_payload_category(method, payload)
  end

  defp codex_event_name_category(event_name),
    do: Map.get(@codex_event_name_categories, event_name)

  defp codex_event_type_category(event_type),
    do: Map.get(@codex_event_type_categories, event_type)

  defp codex_terminal_method_category(method),
    do: Map.get(@codex_terminal_method_categories, method)

  defp codex_interaction_method_category(method) do
    cond do
      method in approval_request_methods() -> :approval_required
      method in user_input_request_methods() -> :user_input_required
      true -> nil
    end
  end

  defp codex_payload_category(method, payload) do
    cond do
      not is_nil(token_usage_evidence(payload)) -> :token_usage
      method == "account/rateLimits/updated" -> :rate_limits
      not is_nil(rate_limit_evidence(payload)) -> :rate_limits
      agent_message_method?(method) -> :agent_message
      true -> nil
    end
  end

  defp codex_event_method(event, payload) do
    map_value(payload, :method)
    |> case do
      nil -> map_value(event, :method)
      method -> method
    end
    |> event_token()
  end

  defp approval_request_methods do
    [
      "item/commandExecution/requestApproval",
      "execCommandApproval",
      "applyPatchApproval",
      "item/fileChange/requestApproval"
    ]
  end

  defp user_input_request_methods, do: ["item/tool/requestUserInput", "tool/requestUserInput"]

  defp agent_message_method?(method) when is_binary(method) do
    method in [
      "item/agentMessage/delta",
      "codex/event/agent_message_delta",
      "codex/event/agent_message_content_delta"
    ]
  end

  defp agent_message_method?(_method), do: false

  defp codex_event_kind(:turn_completed), do: "codex.turn.completed"
  defp codex_event_kind(:turn_failed), do: "codex.turn.failed"
  defp codex_event_kind(:turn_cancelled), do: "codex.turn.cancelled"
  defp codex_event_kind(:malformed), do: "codex.protocol.malformed"
  defp codex_event_kind(:turn_timeout), do: "codex.turn.timeout"
  defp codex_event_kind(:approval_required), do: "codex.approval.required"
  defp codex_event_kind(:approval_auto_approved), do: "codex.approval.auto_approved"
  defp codex_event_kind(:user_input_required), do: "codex.user_input.required"
  defp codex_event_kind(:user_input_auto_answered), do: "codex.user_input.auto_answered"
  defp codex_event_kind(:token_usage), do: "codex.token_usage.updated"
  defp codex_event_kind(:rate_limits), do: "codex.rate_limits.updated"
  defp codex_event_kind(:agent_message), do: "codex.agent_message.updated"

  defp codex_event_status(:turn_completed), do: "completed"
  defp codex_event_status(:turn_failed), do: "failed"
  defp codex_event_status(:turn_cancelled), do: "cancelled"
  defp codex_event_status(:turn_timeout), do: "timeout"
  defp codex_event_status(_category), do: nil

  defp codex_event_level(category)
       when category in [
              :turn_failed,
              :malformed,
              :turn_timeout,
              :approval_required,
              :user_input_required
            ],
       do: "warning"

  defp codex_event_level(_category), do: "info"

  defp codex_event_message_summary(:turn_completed), do: "codex turn completed"
  defp codex_event_message_summary(:turn_failed), do: "codex turn failed"
  defp codex_event_message_summary(:turn_cancelled), do: "codex turn cancelled"
  defp codex_event_message_summary(:malformed), do: "codex malformed protocol line"
  defp codex_event_message_summary(:turn_timeout), do: "codex turn timed out"
  defp codex_event_message_summary(:approval_required), do: "codex approval required"
  defp codex_event_message_summary(:approval_auto_approved), do: "codex approval auto-approved"
  defp codex_event_message_summary(:user_input_required), do: "codex user input required"

  defp codex_event_message_summary(:user_input_auto_answered),
    do: "codex user input auto-answered"

  defp codex_event_message_summary(:token_usage), do: "codex token usage updated"
  defp codex_event_message_summary(:rate_limits), do: "codex rate limits updated"
  defp codex_event_message_summary(:agent_message), do: "codex agent message updated"

  defp codex_lower_event_ref(event, turn_attempt, category) do
    map_value(event, :event_id) ||
      map_value(event, :event_ref) ||
      "lower-event://#{turn_run_id(turn_attempt, nil)}/#{codex_event_kind(category)}"
  end

  defp codex_event_stream_summary(events, output) do
    terminal_status = terminal_status(output, %{terminal_status: event_terminal_status(events)})
    token_usage = latest_map(events, :token_usage)
    rate_limits = latest_rate_limit_evidence(events)
    last_message = latest_last_message(events)

    %{
      confirmed?: true,
      event_count: length(events),
      terminal_status: terminal_status,
      completed_event_count: count_codex_events(events, :turn_completed),
      failed_event_count: count_codex_events(events, :turn_failed),
      cancelled_event_count: count_codex_events(events, :turn_cancelled),
      malformed_event_count: count_codex_events(events, :malformed),
      timeout_event_count: count_codex_events(events, :turn_timeout),
      approval_event_count:
        count_codex_events(events, [:approval_required, :approval_auto_approved]),
      approval_required_count: count_codex_events(events, :approval_required),
      approval_auto_approved_count: count_codex_events(events, :approval_auto_approved),
      user_input_event_count:
        count_codex_events(events, [:user_input_required, :user_input_auto_answered]),
      user_input_required_count: count_codex_events(events, :user_input_required),
      user_input_auto_answered_count: count_codex_events(events, :user_input_auto_answered),
      token_usage: token_usage,
      rate_limits_present?: not is_nil(rate_limits),
      rate_limit_id: map_value(rate_limits, :rate_limit_id),
      rate_limit_primary_remaining: map_value(rate_limits, :rate_limit_primary_remaining),
      rate_limit_primary_limit: map_value(rate_limits, :rate_limit_primary_limit),
      last_message: last_message,
      events: events
    }
    |> compact_map()
  end

  defp latest_rate_limit_evidence(events) do
    events
    |> Enum.reverse()
    |> Enum.find(&truthy?(map_value(&1, :rate_limits_present?)))
  end

  defp latest_last_message(events) do
    events
    |> Enum.reverse()
    |> Enum.find(&truthy?(map_value(&1, :last_message?)))
    |> case do
      nil ->
        nil

      event ->
        %{
          event_kind: map_value(event, :event_kind),
          summary: map_value(event, :message_summary),
          body_redacted?: true,
          body_included?: false
        }
    end
  end

  defp latest_map(events, key) do
    events
    |> Enum.reverse()
    |> Enum.find_value(fn event ->
      case map_value(event, key) do
        %{} = value -> value
        _other -> nil
      end
    end)
  end

  defp count_codex_events(events, categories) when is_list(categories) do
    Enum.count(events, &(map_value(&1, :category) in categories))
  end

  defp count_codex_events(events, category), do: count_codex_events(events, [category])

  defp codex_event_stream_extensions(nil), do: %{}

  defp codex_event_stream_extensions(evidence) do
    events = map_value(evidence, :events) || []

    %{
      "codex_event_stream" =>
        %{
          "confirmed?" => map_value(evidence, :confirmed?),
          "event_count" => map_value(evidence, :event_count),
          "terminal_status" => map_value(evidence, :terminal_status),
          "completed_event_count" => map_value(evidence, :completed_event_count),
          "failed_event_count" => map_value(evidence, :failed_event_count),
          "cancelled_event_count" => map_value(evidence, :cancelled_event_count),
          "malformed_event_count" => map_value(evidence, :malformed_event_count),
          "timeout_event_count" => map_value(evidence, :timeout_event_count),
          "approval_event_count" => map_value(evidence, :approval_event_count),
          "approval_required_count" => map_value(evidence, :approval_required_count),
          "approval_auto_approved_count" => map_value(evidence, :approval_auto_approved_count),
          "user_input_event_count" => map_value(evidence, :user_input_event_count),
          "user_input_required_count" => map_value(evidence, :user_input_required_count),
          "user_input_auto_answered_count" =>
            map_value(evidence, :user_input_auto_answered_count),
          "token_usage" => evidence |> map_value(:token_usage) |> string_key_map(),
          "rate_limits_present?" => map_value(evidence, :rate_limits_present?),
          "rate_limit_id" => map_value(evidence, :rate_limit_id),
          "rate_limit_primary_remaining" => map_value(evidence, :rate_limit_primary_remaining),
          "rate_limit_primary_limit" => map_value(evidence, :rate_limit_primary_limit),
          "last_message" => evidence |> map_value(:last_message) |> string_key_map(),
          "event_kinds" => Enum.map(events, &map_value(&1, :event_kind)),
          "events" => Enum.map(events, &codex_event_stream_event_extension/1)
        }
        |> compact_map()
    }
  end

  defp codex_token_accounting_evidence(nil), do: nil

  defp codex_token_accounting_evidence(evidence) do
    evidence
    |> map_value(:events)
    |> List.wrap()
    |> token_accounting_events()
    |> Enum.reduce(token_accounting_initial_state(), &token_accounting_accept_event/2)
    |> token_accounting_summary()
  end

  defp token_accounting_events(events) when is_list(events) do
    events
    |> Enum.group_by(&map_value(&1, :turn_index))
    |> Enum.flat_map(fn {_turn_index, turn_events} ->
      token_accounting_turn_events(turn_events)
    end)
  end

  defp token_accounting_turn_events(turn_events) do
    if Enum.any?(turn_events, &explicit_token_usage_event?/1) do
      Enum.reject(turn_events, &turn_completed_token_usage_event?/1)
    else
      turn_events
    end
  end

  defp explicit_token_usage_event?(event) do
    token_usage_event?(event) and map_value(event, :category) != :turn_completed
  end

  defp turn_completed_token_usage_event?(event) do
    token_usage_event?(event) and map_value(event, :category) == :turn_completed
  end

  defp token_usage_event?(event), do: not is_nil(map_value(event, :token_usage))

  defp token_accounting_initial_state do
    %{
      accepted_snapshot_count: 0,
      ignored_snapshot_count: 0,
      scopes: %{},
      last_snapshot: nil
    }
  end

  defp token_accounting_accept_event(event, state) do
    case token_accounting_snapshot(event) do
      nil ->
        state

      snapshot ->
        scope_key = token_accounting_scope_key(snapshot)
        existing = Map.get(state.scopes, scope_key)

        if token_accounting_accept_snapshot?(snapshot, existing) do
          %{
            state
            | accepted_snapshot_count: state.accepted_snapshot_count + 1,
              scopes: Map.put(state.scopes, scope_key, snapshot),
              last_snapshot: snapshot
          }
        else
          %{state | ignored_snapshot_count: state.ignored_snapshot_count + 1}
        end
    end
  end

  defp token_accounting_accept_snapshot?(_snapshot, nil), do: true

  defp token_accounting_accept_snapshot?(snapshot, existing) do
    map_value(snapshot, :total_tokens) >= map_value(existing, :total_tokens)
  end

  defp token_accounting_snapshot(event) do
    usage = map_value(event, :token_usage)

    if is_map(usage) do
      input = map_value(usage, :input_tokens) || 0
      output = map_value(usage, :output_tokens) || 0
      total = map_value(usage, :total_tokens) || input + output

      if is_integer(total) and total >= 0 do
        %{
          total_input_tokens: max(input, 0),
          total_output_tokens: max(output, 0),
          total_tokens: total,
          cached_input_tokens: max(map_value(usage, :cached_input_tokens) || 0, 0),
          source: @token_accounting_source,
          snapshot_source: map_value(usage, :source),
          scope_ref: map_value(event, :token_scope_ref)
        }
        |> compact_map()
      end
    end
  end

  defp token_accounting_scope_key(snapshot) do
    map_value(snapshot, :scope_ref) || "__run__"
  end

  defp token_accounting_summary(%{accepted_snapshot_count: 0}), do: nil

  defp token_accounting_summary(state) do
    totals =
      state.scopes
      |> Map.values()
      |> Enum.reduce(token_accounting_zero_totals(), &token_accounting_add_totals/2)

    last_snapshot = state.last_snapshot || %{}

    %{
      confirmed?: true,
      source: @token_accounting_source,
      accepted_snapshot_count: state.accepted_snapshot_count,
      ignored_snapshot_count: state.ignored_snapshot_count,
      scope_count: map_size(state.scopes),
      last_scope_ref: map_value(last_snapshot, :scope_ref),
      last_snapshot_source: map_value(last_snapshot, :snapshot_source),
      total_input_tokens: totals.total_input_tokens,
      total_output_tokens: totals.total_output_tokens,
      total_tokens: totals.total_tokens,
      cached_input_tokens: totals.cached_input_tokens
    }
    |> compact_map()
  end

  defp token_accounting_zero_totals do
    %{
      total_input_tokens: 0,
      total_output_tokens: 0,
      total_tokens: 0,
      cached_input_tokens: 0
    }
  end

  defp token_accounting_add_totals(snapshot, totals) do
    %{
      total_input_tokens: totals.total_input_tokens + map_value(snapshot, :total_input_tokens),
      total_output_tokens: totals.total_output_tokens + map_value(snapshot, :total_output_tokens),
      total_tokens: totals.total_tokens + map_value(snapshot, :total_tokens),
      cached_input_tokens: totals.cached_input_tokens + map_value(snapshot, :cached_input_tokens)
    }
  end

  defp codex_token_totals(nil), do: nil

  defp codex_token_totals(evidence) do
    %{
      total_input_tokens: map_value(evidence, :total_input_tokens),
      total_output_tokens: map_value(evidence, :total_output_tokens),
      total_tokens: map_value(evidence, :total_tokens),
      cached_input_tokens: map_value(evidence, :cached_input_tokens) || 0,
      source: @token_accounting_source
    }
    |> compact_map()
  end

  defp codex_token_accounting_extensions(nil), do: %{}

  defp codex_token_accounting_extensions(evidence) do
    %{
      "codex_token_accounting" =>
        %{
          "confirmed?" => map_value(evidence, :confirmed?),
          "source" => map_value(evidence, :source),
          "accepted_snapshot_count" => map_value(evidence, :accepted_snapshot_count),
          "ignored_snapshot_count" => map_value(evidence, :ignored_snapshot_count),
          "scope_count" => map_value(evidence, :scope_count),
          "last_scope_ref" => map_value(evidence, :last_scope_ref),
          "last_snapshot_source" => map_value(evidence, :last_snapshot_source),
          "total_input_tokens" => map_value(evidence, :total_input_tokens),
          "total_output_tokens" => map_value(evidence, :total_output_tokens),
          "total_tokens" => map_value(evidence, :total_tokens),
          "cached_input_tokens" => map_value(evidence, :cached_input_tokens)
        }
        |> compact_map()
    }
  end

  defp codex_event_stream_event_extension(event) do
    %{
      "event_kind" => map_value(event, :event_kind),
      "category" => event |> map_value(:category) |> event_token(),
      "turn_index" => map_value(event, :turn_index),
      "lower_event_ref" => map_value(event, :lower_event_ref),
      "status" => map_value(event, :status),
      "message_summary" => map_value(event, :message_summary),
      "token_usage" => event |> map_value(:token_usage) |> string_key_map(),
      "rate_limits_present?" => map_value(event, :rate_limits_present?),
      "rate_limit_id" => map_value(event, :rate_limit_id),
      "rate_limit_primary_remaining" => map_value(event, :rate_limit_primary_remaining),
      "rate_limit_primary_limit" => map_value(event, :rate_limit_primary_limit),
      "body_redacted?" => map_value(event, :body_redacted?),
      "body_included?" => map_value(event, :body_included?)
    }
    |> compact_map()
  end

  defp codex_event_stream_turn_fields(nil, _turn_index), do: %{}

  defp codex_event_stream_turn_fields(evidence, turn_index) do
    turn_events =
      evidence
      |> map_value(:events)
      |> List.wrap()
      |> Enum.filter(&(map_value(&1, :turn_index) == turn_index))

    %{
      event_stream_confirmed?: turn_events != [],
      event_count: length(turn_events),
      terminal_event_status: turn_event_terminal_status(evidence, turn_index)
    }
    |> compact_map()
  end

  defp codex_event_stream_event_count(nil), do: 0
  defp codex_event_stream_event_count(evidence), do: length(map_value(evidence, :events) || [])

  defp codex_event_stream_runtime_events(nil, _attrs, _workflow_ref, _observed_at, _event_seq),
    do: []

  defp codex_event_stream_runtime_events(evidence, attrs, workflow_ref, observed_at, event_seq) do
    run_ref = map_value(attrs, :run_ref)

    evidence
    |> map_value(:events)
    |> List.wrap()
    |> Enum.map(fn event ->
      index = map_value(event, :event_index)

      %{
        event_ref: "event://codex-agent-runtime/#{ref_suffix(run_ref)}/codex-event-#{index}",
        event_seq: event_seq + index - 1,
        event_kind: map_value(event, :event_kind),
        observed_at: map_value(event, :observed_at) || observed_at,
        tenant_ref: map_value(attrs, :tenant_id) || map_value(attrs, :tenant_ref),
        subject_ref: map_value(attrs, :subject_ref),
        run_ref: run_ref,
        workflow_ref: workflow_ref,
        turn_ref: turn_ref(run_ref, map_value(event, :turn_index)),
        level: map_value(event, :level),
        message_summary: map_value(event, :message_summary),
        extensions: codex_event_stream_event_extension(event)
      }
    end)
  end

  defp token_usage_evidence(payload) do
    token_usage_from_declared_sources(payload) || codex_usage_raw_token_usage(payload)
  end

  defp turn_completed_token_usage_evidence(payload) do
    turn_completed_token_usage_sources()
    |> Enum.find_value(fn path ->
      usage = map_at_path(payload, path)

      if token_usage_map?(usage) do
        usage
        |> token_usage_counts()
        |> Map.put(:source, "turn_completed_usage")
      end
    end)
  end

  defp turn_completed_token_usage_sources do
    [
      [:output, :usage],
      [:usage],
      [:params, :usage]
    ]
  end

  defp token_usage_from_declared_sources(payload) do
    declared_token_usage_sources()
    |> Enum.find_value(fn {path, source} ->
      usage = map_at_path(payload, path)

      if token_usage_map?(usage) do
        usage
        |> token_usage_counts()
        |> Map.put(:source, source)
      end
    end)
  end

  defp declared_token_usage_sources do
    [
      {[:params, :tokenUsage, :total], "thread_token_usage_total"},
      {[:tokenUsage, :total], "thread_token_usage_total"},
      {[:params, :msg, :payload, :info, :total_token_usage], "token_count_total_token_usage"},
      {[:params, :msg, :info, :total_token_usage], "token_count_total_token_usage"}
    ]
  end

  defp codex_usage_raw_token_usage(payload) do
    content = map_value(payload, :content) || %{}
    metadata = map_value(payload, :metadata) || %{}
    usage = map_value(content, :usage) || map_value(content, :token_usage)

    if codex_usage_raw_token_usage?(payload, content, metadata) and token_usage_map?(usage) do
      usage
      |> token_usage_counts()
      |> Map.put(:source, "codex_usage_absolute")
    end
  end

  defp codex_usage_raw_token_usage?(payload, content, metadata) do
    event_token(map_value(payload, :stream)) == "codex_usage" and
      event_token(map_value(content, :type)) in [
        "thread/tokenUsage/updated",
        "thread.tokenUsage.updated"
      ] and
      absolute_usage_scope?(content, metadata)
  end

  defp absolute_usage_scope?(content, metadata) do
    scope =
      map_value(metadata, :usage_scope) ||
        map_value(content, :usage_scope) ||
        content |> map_value(:usage) |> map_value(:usage_scope)

    event_token(scope) not in ["delta", "delta_only", "incremental"]
  end

  defp codex_token_scope_ref(payload, event \\ %{}) do
    payload
    |> token_scope_id(event)
    |> case do
      nil -> nil
      scope_id -> "codex-thread://#{scope_id}"
    end
  end

  defp token_scope_id(payload, event) do
    [
      [:params, :thread_id],
      [:params, :threadId],
      [:params, :thread, :id],
      [:params, :msg, :thread_id],
      [:params, :msg, :threadId],
      [:params, :msg, :payload, :thread_id],
      [:params, :msg, :payload, :threadId],
      [:content, :thread_id],
      [:content, :threadId],
      [:thread_id],
      [:threadId],
      [:session_id],
      [:sessionId]
    ]
    |> Enum.find_value(fn path ->
      payload
      |> map_at_path(path)
      |> non_empty()
    end) ||
      event
      |> map_value(:session_id)
      |> non_empty()
  end

  defp token_usage_map?(%{} = payload) do
    not is_nil(
      token_integer(payload, [:input_tokens, :prompt_tokens, :inputTokens, :promptTokens])
    ) or
      not is_nil(
        token_integer(payload, [
          :output_tokens,
          :completion_tokens,
          :outputTokens,
          :completionTokens
        ])
      ) or
      not is_nil(token_integer(payload, [:total_tokens, :total, :totalTokens]))
  end

  defp token_usage_map?(_payload), do: false

  defp token_usage_counts(usage) do
    input = token_integer(usage, [:input_tokens, :prompt_tokens, :inputTokens, :promptTokens])

    output =
      token_integer(usage, [
        :output_tokens,
        :completion_tokens,
        :outputTokens,
        :completionTokens
      ])

    total = normalized_total_tokens(usage, input, output)

    %{
      input_tokens: input,
      output_tokens: output,
      total_tokens: total
    }
    |> compact_map()
  end

  defp normalized_total_tokens(usage, input, output) do
    total = token_integer(usage, [:total_tokens, :total, :totalTokens])
    derived_total = derived_total_tokens(input, output)

    normalize_total_token_count(total, derived_total)
  end

  defp derived_total_tokens(input, output), do: (input || 0) + (output || 0)

  defp normalize_total_token_count(total, _derived_total) when is_integer(total) and total > 0,
    do: total

  defp normalize_total_token_count(0, derived_total) when derived_total > 0, do: derived_total

  defp normalize_total_token_count(total, _derived_total) when is_integer(total), do: total

  defp normalize_total_token_count(_total, derived_total) when derived_total > 0,
    do: derived_total

  defp normalize_total_token_count(_total, _derived_total), do: nil

  defp rate_limit_evidence(payload) do
    payload
    |> rate_limits_payload()
    |> case do
      %{} = rate_limits ->
        primary = map_value(rate_limits, :primary) || %{}

        %{
          rate_limits_present?: true,
          rate_limit_id: map_value(rate_limits, :limit_id) || map_value(rate_limits, :limit_name),
          rate_limit_primary_remaining: parse_integer(map_value(primary, :remaining)),
          rate_limit_primary_limit: parse_integer(map_value(primary, :limit))
        }
        |> compact_map()

      _missing ->
        nil
    end
  end

  defp rate_limits_payload(payload) when is_map(payload) do
    map_at_path(payload, [:params, :rateLimits]) ||
      map_at_path(payload, [:params, :rate_limits]) ||
      map_value(payload, :rateLimits) ||
      map_value(payload, :rate_limits)
  end

  defp rate_limits_payload(_payload), do: nil

  defp map_at_path(payload, path) when is_map(payload) and is_list(path) do
    Enum.reduce_while(path, payload, fn key, acc ->
      case map_value(acc, key) do
        nil -> {:halt, nil}
        value -> {:cont, value}
      end
    end)
  end

  defp map_at_path(_payload, _path), do: nil

  defp token_integer(payload, keys) do
    Enum.find_value(keys, fn key -> parse_integer(map_value(payload, key)) end)
  end

  defp parse_integer(value) when is_integer(value), do: value

  defp parse_integer(value) when is_binary(value) do
    case Integer.parse(String.trim(value)) do
      {parsed, ""} -> parsed
      _other -> nil
    end
  end

  defp parse_integer(_value), do: nil

  defp string_key_map(nil), do: nil

  defp string_key_map(%{} = map) do
    map
    |> Enum.map(fn {key, value} -> {string_key(key), value} end)
    |> Map.new()
  end

  defp string_key_map(value), do: value

  defp string_key(key) when is_atom(key), do: Atom.to_string(key)
  defp string_key(key), do: key

  defp event_token(nil), do: nil
  defp event_token(value) when is_atom(value), do: Atom.to_string(value)
  defp event_token(value) when is_binary(value), do: value
  defp event_token(value), do: to_string(value)

  defp app_server_protocol_evidence(
         input,
         output,
         session_start,
         lower_request_ref,
         lower_receipt_ref
       ) do
    provider_session_id = provider_session_id(output)

    if not is_nil(session_start) and app_server_requested?(input) and
         present_binary?(lower_receipt_ref) do
      %{
        confirmed?: true,
        transport: "app_server",
        jsonrpc_methods: @app_server_protocol_methods,
        initialization_confirmed?: true,
        thread_start_confirmed?: true,
        turn_start_confirmed?: true,
        cwd_validation_confirmed?: present_binary?(map_value(input, :cwd)),
        command_launch_owner: "lower_runtime",
        timeout_policy_owner: "lower_runtime",
        provider_session_id: provider_session_id,
        provider_turn_id: provider_turn_id(output),
        runtime_control_session_ref: session_start && session_start.runtime_control_session_ref,
        lower_request_ref: lower_request_ref,
        lower_receipt_ref: lower_receipt_ref
      }
    end
  end

  defp app_server_requested?(input) do
    input
    |> map_value(:provider_metadata)
    |> map_value(:app_server)
    |> truthy?()
  end

  defp provider_session_id(output),
    do: map_value(output, :provider_session_id) || map_value(output, :session_id)

  defp provider_turn_id(output),
    do:
      map_value(output, :provider_turn_id) ||
        output
        |> map_value(:metadata)
        |> map_value(:provider_turn_id)

  defp app_server_protocol_extensions(nil), do: %{}

  defp app_server_protocol_extensions(evidence) do
    %{
      "codex_app_server_protocol" =>
        %{
          "confirmed?" => evidence.confirmed?,
          "transport" => evidence.transport,
          "jsonrpc_methods" => evidence.jsonrpc_methods,
          "initialization_confirmed?" => evidence.initialization_confirmed?,
          "thread_start_confirmed?" => evidence.thread_start_confirmed?,
          "turn_start_confirmed?" => evidence.turn_start_confirmed?,
          "cwd_validation_confirmed?" => evidence.cwd_validation_confirmed?,
          "command_launch_owner" => evidence.command_launch_owner,
          "timeout_policy_owner" => evidence.timeout_policy_owner,
          "provider_session_id" => evidence.provider_session_id,
          "provider_turn_id" => evidence.provider_turn_id,
          "runtime_control_session_ref" => evidence.runtime_control_session_ref,
          "lower_request_ref" => evidence.lower_request_ref,
          "lower_receipt_ref" => evidence.lower_receipt_ref
        }
        |> compact_map()
    }
  end

  defp app_server_protocol_turn_fields(nil), do: %{}

  defp app_server_protocol_turn_fields(evidence) do
    %{
      app_server_protocol_confirmed?: evidence.confirmed?,
      app_server_transport: evidence.transport,
      app_server_jsonrpc_methods: evidence.jsonrpc_methods,
      app_server_initialization_confirmed?: evidence.initialization_confirmed?,
      app_server_thread_start_confirmed?: evidence.thread_start_confirmed?,
      app_server_turn_start_confirmed?: evidence.turn_start_confirmed?,
      app_server_cwd_validation_confirmed?: evidence.cwd_validation_confirmed?,
      app_server_lower_request_ref: evidence.lower_request_ref,
      app_server_lower_receipt_ref: evidence.lower_receipt_ref,
      provider_session_id: evidence.provider_session_id,
      provider_turn_id: evidence.provider_turn_id
    }
    |> compact_map()
  end

  defp app_server_protocol_runtime_events(
         nil,
         _attrs,
         _workflow_ref,
         _turn_ref,
         _observed_at,
         _event_seq
       ),
       do: []

  defp app_server_protocol_runtime_events(
         evidence,
         attrs,
         workflow_ref,
         turn_ref,
         observed_at,
         event_seq
       ) do
    [
      %{
        event_ref:
          "event://codex-agent-runtime/#{ref_suffix(map_value(attrs, :run_ref))}/app-server-protocol",
        event_seq: event_seq,
        event_kind: "codex.app_server.protocol.confirmed",
        observed_at: observed_at,
        tenant_ref: map_value(attrs, :tenant_id) || map_value(attrs, :tenant_ref),
        subject_ref: map_value(attrs, :subject_ref),
        run_ref: map_value(attrs, :run_ref),
        workflow_ref: workflow_ref,
        session_ref: evidence.runtime_control_session_ref,
        turn_ref: turn_ref,
        level: "info",
        message_summary: "codex app-server protocol confirmed",
        extensions: %{
          jsonrpc_methods: evidence.jsonrpc_methods,
          transport: evidence.transport,
          lower_request_ref: evidence.lower_request_ref,
          lower_receipt_ref: evidence.lower_receipt_ref,
          provider_session_id: evidence.provider_session_id,
          provider_turn_id: evidence.provider_turn_id,
          cwd_validation_confirmed?: evidence.cwd_validation_confirmed?
        }
      }
    ]
  end

  defp hook_events(_attrs, _workflow_ref, _turn_ref, _observed_at, _stage, _seq, []), do: []

  defp hook_events(attrs, workflow_ref, turn_ref, observed_at, stage, seq, receipts) do
    run_ref = map_value(attrs, :run_ref)
    stage_name = Atom.to_string(stage)
    event_slug = String.replace(stage_name, "_", "-")

    [
      %{
        event_ref: "event://codex-agent-runtime/#{ref_suffix(run_ref)}/#{event_slug}-hook",
        event_seq: seq,
        event_kind: "workspace.hook.#{stage_name}",
        observed_at: observed_at,
        tenant_ref: map_value(attrs, :tenant_id) || map_value(attrs, :tenant_ref),
        subject_ref: map_value(attrs, :subject_ref),
        run_ref: run_ref,
        workflow_ref: workflow_ref,
        turn_ref: turn_ref,
        level: "info",
        message_summary: "#{stage_name} hook completed",
        extensions: %{
          hook_receipts: receipts,
          hook_receipt: List.first(receipts),
          path_redacted?: true
        }
      }
    ]
  end

  defp lower_receipt_ref(attempt, lower_request_ref) do
    case map_value(attempt, :attempt_id) do
      value when is_binary(value) and value != "" ->
        "lower-receipt://#{value}/#{@capability_id}/succeeded"

      _missing ->
        "#{lower_request_ref}/succeeded"
    end
  end

  defp codex_stall_decision(
         attrs,
         turn_attempts,
         event_stream,
         base_status,
         observed_at,
         workflow_ref
       ) do
    timeout_ms = stall_timeout_ms(attrs)

    cond do
      not active_runtime_status?(base_status) ->
        nil

      not is_integer(timeout_ms) or timeout_ms <= 0 ->
        nil

      true ->
        codex_stall_decision_for_timeout(
          attrs,
          turn_attempts,
          event_stream,
          timeout_ms,
          observed_at,
          workflow_ref
        )
    end
  end

  defp active_runtime_status?(status) when status in ["running", "active", "pending"],
    do: true

  defp active_runtime_status?(status) when status in ["in_progress", "started", "queued"],
    do: true

  defp active_runtime_status?(_status), do: false

  defp codex_stall_decision_for_timeout(
         attrs,
         turn_attempts,
         event_stream,
         timeout_ms,
         observed_at,
         workflow_ref
       ) do
    case runtime_last_activity(attrs, turn_attempts, event_stream) do
      {%DateTime{} = last_activity_at, activity_source} ->
        elapsed_ms = DateTime.diff(observed_at, last_activity_at, :millisecond)

        if elapsed_ms >= timeout_ms do
          build_codex_stall_decision(
            attrs,
            turn_attempts,
            timeout_ms,
            observed_at,
            workflow_ref,
            last_activity_at,
            activity_source,
            elapsed_ms
          )
        end

      _missing ->
        nil
    end
  end

  defp runtime_last_activity(attrs, turn_attempts, event_stream) do
    case latest_runtime_event_observed_at(event_stream) do
      %DateTime{} = observed_at ->
        {observed_at, "last_runtime_event_at"}

      nil ->
        case run_started_at(attrs, List.first(turn_attempts)) do
          %DateTime{} = started_at -> {started_at, "started_at"}
          nil -> nil
        end
    end
  end

  defp build_codex_stall_decision(
         attrs,
         turn_attempts,
         timeout_ms,
         observed_at,
         workflow_ref,
         last_activity_at,
         activity_source,
         elapsed_ms
       ) do
    run_ref = map_value(attrs, :run_ref)
    final_turn_attempt = List.last(turn_attempts)
    result = turn_attempt_result(final_turn_attempt)
    output = map_value(result, :output) || %{}
    current_attempt_ref = current_attempt_ref(attrs, final_turn_attempt, run_ref)
    retry_delay_ms = stall_retry_delay_ms(attrs)
    retry_due_at = DateTime.add(observed_at, retry_delay_ms, :millisecond)
    safe_action = "terminate_lower_and_schedule_retry"

    %{
      state: "stalled",
      runtime_state: "stalled",
      status_reason: "stall_timeout",
      event_ref: "event://codex-agent-runtime/#{ref_suffix(run_ref)}/runtime-stalled",
      observed_at: observed_at,
      elapsed_ms: elapsed_ms,
      stall_timeout_ms: timeout_ms,
      last_activity_at: last_activity_at,
      activity_source: activity_source,
      safe_action: safe_action,
      workflow_signal: "operator.cancel",
      cancel_lower_run?: true,
      cleanup_workspace?: false,
      run_ref: run_ref,
      workflow_ref: workflow_ref,
      attempt_ref: current_attempt_ref,
      session_ref: session_ref(output, run_ref),
      worker_ref: map_value(attrs, :worker_ref),
      workspace_ref: map_value(attrs, :workspace_ref),
      retry: %{
        retry_ref: "retry://codex-agent-runtime/#{ref_suffix(run_ref)}/stall-timeout-1",
        next_attempt_ref: "attempt://codex-agent-runtime/#{ref_suffix(run_ref)}/retry-1",
        status: "scheduled",
        reason: "stall_timeout",
        scheduled_at: observed_at,
        due_at: retry_due_at,
        delay_ms: retry_delay_ms,
        delay_type: "failure_backoff",
        metadata: %{safe_action: safe_action}
      },
      diagnostic: %{
        code: "runtime_stall_timeout",
        severity: "warning",
        message: "runtime activity exceeded stall timeout",
        observed_at: observed_at,
        stale_after_ms: timeout_ms
      }
    }
    |> compact_map()
  end

  defp stalled_runtime_state(nil), do: nil
  defp stalled_runtime_state(stall_decision), do: map_value(stall_decision, :runtime_state)

  defp latest_runtime_event_observed_at(nil), do: nil

  defp latest_runtime_event_observed_at(event_stream) do
    event_stream
    |> map_value(:events)
    |> List.wrap()
    |> Enum.map(&map_value(&1, :observed_at))
    |> latest_datetime()
  end

  defp run_started_at(attrs, first_turn_attempt) do
    result = turn_attempt_result(first_turn_attempt)
    run = map_value(result, :run) || %{}
    attempt = map_value(result, :attempt) || %{}

    [
      map_value(attrs, :run_started_at),
      map_value(attrs, :started_at),
      map_value(run, :started_at),
      map_value(run, :inserted_at),
      map_value(attempt, :started_at),
      map_value(attempt, :inserted_at)
    ]
    |> first_datetime()
  end

  defp lower_event_observed_at(event) do
    payload = map_value(event, :payload) || %{}

    [
      map_value(event, :observed_at),
      map_value(event, :timestamp),
      map_value(event, :inserted_at),
      map_value(event, :created_at),
      map_value(payload, :observed_at),
      map_value(payload, :timestamp)
    ]
    |> first_datetime()
  end

  defp current_attempt_ref(attrs, turn_attempt, run_ref) do
    attempt = turn_attempt_attempt(turn_attempt)

    map_value(attrs, :attempt_ref) ||
      map_value(turn_attempt, :attempt_ref) ||
      map_value(attempt, :attempt_ref) ||
      case map_value(attempt, :attempt_id) do
        value when is_binary(value) and value != "" ->
          "attempt://codex-agent-runtime/#{ref_suffix(value)}"

        _missing ->
          "attempt://codex-agent-runtime/#{ref_suffix(run_ref)}/1"
      end
  end

  defp stall_timeout_ms(attrs) do
    policy = map_value(attrs, :timeout_policy) || %{}

    [
      map_value(policy, :stall_timeout_ms),
      map_value(policy, :codex_stall_timeout_ms),
      map_value(attrs, :stall_timeout_ms),
      map_value(attrs, :codex_stall_timeout_ms)
    ]
    |> Enum.find_value(&integer_value/1)
  end

  defp stall_retry_delay_ms(attrs) do
    policy = map_value(attrs, :retry_policy) || map_value(attrs, :timeout_policy) || %{}

    [
      map_value(policy, :stall_retry_delay_ms),
      map_value(policy, :retry_delay_ms),
      map_value(attrs, :stall_retry_delay_ms)
    ]
    |> Enum.find_value(&positive_integer_value/1)
    |> case do
      nil -> 10_000
      value -> value
    end
  end

  defp observed_now(opts) do
    opts
    |> Keyword.get(:now)
    |> normalize_datetime()
    |> case do
      nil -> DateTime.utc_now()
      now -> now
    end
  end

  defp first_datetime(values), do: Enum.find_value(values, &normalize_datetime/1)

  defp latest_datetime(values) do
    values
    |> Enum.map(&normalize_datetime/1)
    |> Enum.reject(&is_nil/1)
    |> Enum.reduce(nil, fn
      value, nil ->
        value

      value, acc ->
        if DateTime.compare(value, acc) == :gt, do: value, else: acc
    end)
  end

  defp normalize_datetime(%DateTime{} = value), do: value

  defp normalize_datetime(%NaiveDateTime{} = value),
    do: DateTime.from_naive!(value, "Etc/UTC")

  defp normalize_datetime(value) when is_binary(value) do
    case DateTime.from_iso8601(value) do
      {:ok, datetime, _offset} -> datetime
      _error -> nil
    end
  end

  defp normalize_datetime(_value), do: nil

  defp integer_value(value) when is_integer(value), do: value

  defp integer_value(value) when is_binary(value) do
    case Integer.parse(value) do
      {integer, ""} -> integer
      _other -> nil
    end
  end

  defp integer_value(_value), do: nil

  defp positive_integer_value(value) do
    case integer_value(value) do
      integer when is_integer(integer) and integer > 0 -> integer
      _other -> nil
    end
  end

  defp output_status(output) do
    case map_value(output, :status) do
      value when value in [:completed, "completed", :ok, "ok", :succeeded, "succeeded"] ->
        "completed"

      value when is_atom(value) ->
        Atom.to_string(value)

      value when is_binary(value) and value != "" ->
        value

      _missing ->
        "completed"
    end
  end

  defp terminal_status(output, event_stream) do
    case output_status(output) do
      "completed" -> map_value(event_stream, :terminal_status) || "completed"
      status -> status
    end
  end

  defp turn_status(output, event_stream, turn_index) do
    case output_status(output) do
      "completed" -> turn_event_terminal_status(event_stream, turn_index) || "completed"
      status -> status
    end
  end

  defp turn_event_terminal_status(nil, _turn_index), do: nil

  defp turn_event_terminal_status(event_stream, turn_index) do
    event_stream
    |> map_value(:events)
    |> List.wrap()
    |> Enum.filter(&(map_value(&1, :turn_index) == turn_index))
    |> event_terminal_status()
  end

  defp event_terminal_status(events) when is_list(events) do
    events
    |> Enum.filter(&(map_value(&1, :category) in terminal_event_categories()))
    |> List.last()
    |> case do
      nil -> nil
      event -> map_value(event, :status)
    end
  end

  defp terminal_event_categories,
    do: [:turn_completed, :turn_failed, :turn_cancelled, :turn_timeout]

  defp action_receipt_status("completed"), do: :succeeded
  defp action_receipt_status("stopped"), do: :succeeded
  defp action_receipt_status("cancelled"), do: :cancelled
  defp action_receipt_status("canceled"), do: :cancelled
  defp action_receipt_status("timeout"), do: :timed_out
  defp action_receipt_status(_status), do: :failed

  defp terminal_level("completed"), do: "info"
  defp terminal_level("stalled"), do: "warning"
  defp terminal_level("cancelled"), do: "warning"
  defp terminal_level("canceled"), do: "warning"
  defp terminal_level("timeout"), do: "warning"
  defp terminal_level(_status), do: "error"

  defp terminal_message_summary("completed"), do: "codex session turn completed"
  defp terminal_message_summary("stalled"), do: "codex session turn stalled"
  defp terminal_message_summary("cancelled"), do: "codex session turn cancelled"
  defp terminal_message_summary("canceled"), do: "codex session turn cancelled"
  defp terminal_message_summary("timeout"), do: "codex session turn timed out"
  defp terminal_message_summary(_status), do: "codex session turn failed"

  defp session_ref(output, run_ref) do
    provider_session_id =
      map_value(output, :provider_session_id) || map_value(output, :session_id)

    "session://codex/#{ref_suffix(provider_session_id || run_ref)}"
  end

  defp output_artifact_refs(result) do
    result
    |> map_value(:artifact_refs)
    |> List.wrap()
    |> Enum.filter(&present_binary?/1)
  end

  defp codex_operation_receipt(
         attrs,
         capability_id,
         lower_request_ref,
         lower_receipt_ref,
         status,
         artifact_refs,
         authority_handoff
       ) do
    authority_fields = ProviderAuthorityAdmission.result_fields(authority_handoff)
    run_ref = map_value(attrs, :run_ref)

    %{
      operation_receipt_ref: lower_receipt_ref,
      lower_receipt_ref: lower_receipt_ref,
      lower_request_ref: lower_request_ref,
      lower_runtime_kind: "codex_session",
      status: action_receipt_status_token(status),
      capability_id: capability_id,
      action_id: capability_id,
      effect_request_ref: lower_request_ref,
      connector_ref: "jido/connectors/#{@connector_id}",
      connector_manifest_ref: codex_connector_manifest_ref(),
      connector_binding_ref: Map.get(authority_fields, :connector_binding_ref),
      credential_lease_ref: Map.get(authority_fields, :credential_lease_ref),
      capability_negotiation_ref: capability_negotiation_ref(lower_request_ref),
      authority_ref: Map.get(authority_fields, :authority_packet_ref),
      authority_handoff_ref: Map.get(authority_fields, :authority_handoff_ref),
      trace_id: map_value(attrs, :trace_id),
      tenant_ref: map_value(attrs, :tenant_ref) || map_value(attrs, :tenant_id),
      subject_ref: map_value(attrs, :subject_ref),
      run_ref: run_ref,
      evidence_profile_ref: codex_evidence_profile_ref(run_ref),
      artifact_refs: artifact_refs
    }
    |> compact_map()
  end

  defp codex_connector_manifest_ref, do: "manifest://jido/connectors/#{@connector_id}@local"

  defp capability_negotiation_ref(lower_request_ref) when is_binary(lower_request_ref),
    do: "cap-neg://#{lower_request_ref}"

  defp capability_negotiation_ref(_lower_request_ref), do: nil

  defp codex_evidence_profile_ref(run_ref),
    do: "evidence://codex-agent-runtime/#{ref_suffix(run_ref)}"

  defp action_receipt_status_token("completed"), do: "succeeded"
  defp action_receipt_status_token("stopped"), do: "succeeded"
  defp action_receipt_status_token(status) when is_binary(status), do: status
  defp action_receipt_status_token(status) when is_atom(status), do: Atom.to_string(status)
  defp action_receipt_status_token(_status), do: "failed"

  defp digest(value) do
    value
    |> :erlang.term_to_binary()
    |> then(&:crypto.hash(:sha256, &1))
    |> Base.encode16(case: :lower)
  end

  defp prompt_hash(prompt) when is_binary(prompt) do
    digest = :crypto.hash(:sha256, prompt)
    "sha256:" <> Base.encode16(digest, case: :lower)
  end

  defp normalize(%_{} = struct), do: struct |> Map.from_struct() |> normalize()
  defp normalize(attrs) when is_map(attrs), do: attrs
  defp normalize(attrs) when is_list(attrs), do: Map.new(attrs)

  defp actor_id(attrs),
    do: map_value(attrs, :source_ref) || "actor://mezzanine/codex-agent-runtime"

  defp put_present(keyword, _key, nil), do: keyword
  defp put_present(keyword, key, value), do: Keyword.put(keyword, key, value)

  defp put_map_present(map, _key, nil), do: map
  defp put_map_present(map, key, value), do: Map.put(map, key, value)

  defp compact_map(map) do
    map
    |> Enum.reject(fn {_key, value} -> value in [nil, "", [], %{}] end)
    |> Map.new()
  end

  defp compact_list(list), do: Enum.reject(list, &is_nil/1)

  defp map_value(%_{} = struct, key), do: struct |> Map.from_struct() |> map_value(key)

  defp map_value(%{} = map, key) when is_atom(key) do
    case Map.fetch(map, key) do
      {:ok, nil} -> Map.get(map, Atom.to_string(key))
      {:ok, value} -> value
      :error -> Map.get(map, Atom.to_string(key))
    end
  end

  defp map_value(%{} = map, key) when is_binary(key), do: Map.get(map, key)
  defp map_value(_value, _key), do: nil

  defp present_binary?(value), do: is_binary(value) and String.trim(value) != ""

  defp non_empty(value) when is_binary(value) do
    if String.trim(value) == "", do: nil, else: value
  end

  defp non_empty(_value), do: nil

  defp truthy?(value), do: value in [true, "true", "1", 1, true]

  defp ref_suffix(ref) when is_binary(ref) do
    ref
    |> :binary.bin_to_list()
    |> Enum.reduce({[], false}, &ascii_alnum_dash_byte/2)
    |> elem(0)
    |> Enum.reverse()
    |> List.to_string()
    |> String.trim("-")
  end

  defp ref_suffix(ref), do: ref |> to_string() |> ref_suffix()

  defp ascii_alnum_dash_byte(byte, {chars, _previous_dash?}) when byte in ?A..?Z,
    do: {[byte | chars], false}

  defp ascii_alnum_dash_byte(byte, {chars, _previous_dash?}) when byte in ?a..?z,
    do: {[byte | chars], false}

  defp ascii_alnum_dash_byte(byte, {chars, _previous_dash?}) when byte in ?0..?9,
    do: {[byte | chars], false}

  defp ascii_alnum_dash_byte(_byte, {chars, true}), do: {chars, true}
  defp ascii_alnum_dash_byte(_byte, {chars, false}), do: {[?- | chars], true}
end
