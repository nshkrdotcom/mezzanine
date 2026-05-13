defmodule Mezzanine.IntegrationBridge.CodexAgentRuntime do
  @moduledoc """
  Lower-owned Codex agent runtime for AppKit AgentIntake.

  The runtime translates an AppKit/Mezzanine agent-run spec into a governed Jido
  `codex.session.turn` invocation and returns an M2 projection shape that AppKit
  can expose through its headless readback DTOs.
  """

  alias Jido.Integration.V2
  alias Jido.Integration.V2.Connectors.CodexCli
  alias Jido.Integration.V2.RuntimeRouter
  alias Mezzanine.WorkspaceEngine.{Hooks, LocalCommandRunner, WorkspaceRecord}

  @capability_id "codex.session.turn"
  @session_start_capability_id "codex.session.start"
  @connector_id "codex_cli"
  @codex_workspace_root "/tmp/jido_codex_cli_workspace"
  @scopes ["session:execute", "session:control", "session:tools"]

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
         {:ok, result} <-
           invoke_fun.(@capability_id, codex_input(attrs, opts, workspace_root), invoke_opts) do
      {:ok, result, invoke_opts, before_run_receipts}
    end
  end

  defp complete_codex_attempt(attempt_result, attrs, workspace, opts) do
    after_run_receipts = run_after_run_hooks(workspace, opts)

    case {attempt_result, after_run_receipts} do
      {{:ok, result, invoke_opts, before_run_receipts}, receipts} ->
        {:ok, projection(attrs, result, invoke_opts, before_run_receipts, receipts, opts)}

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
      allowed_operations: [@capability_id],
      sandbox:
        Keyword.get(opts, :sandbox, %{
          level: :strict,
          egress: :restricted,
          approvals: :manual,
          file_scope: workspace_root,
          allowed_tools: [@capability_id]
        })
    ]
    |> put_present(:runtime_auth_mode, Keyword.get(opts, :runtime_auth_mode))
    |> put_present(:runtime_auth_scope, Keyword.get(opts, :runtime_auth_scope))
  end

  defp codex_input(attrs, opts, workspace_root) do
    %{
      prompt:
        Keyword.get(
          opts,
          :prompt,
          "Return one concise sentence confirming the governed Codex runtime path is operational. Do not modify files."
        ),
      cwd: workspace_root,
      provider_metadata: %{"app_server" => true, "skip_git_repo_check" => true},
      authority_metadata: authority_metadata(attrs),
      host_tools: []
    }
    |> put_present(:dynamic_tool_manifest, Keyword.get(opts, :dynamic_tool_manifest))
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
    %{
      "authority_context_ref" => map_value(attrs, :authority_context_ref),
      "capability_id" => @capability_id,
      "idempotency_key" => map_value(attrs, :idempotency_key),
      "trace_id" => map_value(attrs, :trace_id)
    }
    |> compact_map()
  end

  defp projection(attrs, result, invoke_opts, before_run_receipts, after_run_receipts, opts) do
    run = map_value(result, :run) || %{}
    attempt = map_value(result, :attempt) || %{}
    output = map_value(result, :output) || %{}
    run_ref = map_value(attrs, :run_ref)
    run_id = map_value(run, :run_id) || ref_suffix(run_ref)
    workflow_ref = "workflow://codex-agent-runtime/#{ref_suffix(run_ref)}"
    turn_ref = "turn://codex-agent-runtime/#{ref_suffix(run_ref)}/1"
    lower_request_ref = lower_request_ref(run_id, @capability_id)

    lower_receipt_ref = lower_receipt_ref(attempt, lower_request_ref)
    session_start = result |> lower_runtime_events(opts) |> session_start_evidence(run_id)
    status = output_status(output)
    observed_at = DateTime.utc_now()
    terminal_event_seq = if session_start, do: 2, else: 1
    after_run_event_seq = if session_start, do: 3, else: 2

    %{
      run_ref: run_ref,
      subject_ref: map_value(attrs, :subject_ref),
      workflow_ref: workflow_ref,
      status: status,
      terminal_state: status,
      turn_states: [
        %{
          turn_ref: turn_ref,
          state: status,
          status: status,
          session_ref: session_ref(output, run_ref),
          operation: @capability_id,
          credential_redeemed?: true,
          provider_request_sent?: true,
          provider_response_received?: true,
          lower_request_ref: lower_request_ref,
          lower_receipt_ref: lower_receipt_ref
        }
        |> Map.merge(session_start_turn_fields(session_start))
      ],
      extensions: session_start_extensions(session_start),
      action_receipts:
        session_start_action_receipts(session_start) ++
          [
            %{
              status: :succeeded,
              lower_receipt_ref: lower_receipt_ref,
              output_artifact_refs: output_artifact_refs(result)
            }
          ],
      runtime_events:
        hook_events(
          attrs,
          workflow_ref,
          turn_ref,
          observed_at,
          :before_run,
          0,
          before_run_receipts
        ) ++
          session_start_runtime_events(session_start, attrs, workflow_ref, turn_ref, observed_at) ++
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
              turn_ref: turn_ref,
              level: "info",
              message_summary: "codex session turn completed"
            }
          ] ++
          hook_events(
            attrs,
            workflow_ref,
            turn_ref,
            observed_at,
            :after_run,
            after_run_event_seq,
            after_run_receipts
          ),
      budget_state: %{"turns_remaining" => 0},
      candidate_fact_refs: ["candidate-fact://codex-agent-runtime/#{ref_suffix(run_ref)}/1"],
      memory_proof_refs: [],
      receipt_ref_set: %{
        lower_request_refs:
          compact_list([session_start_lower_request_ref(session_start), lower_request_ref]),
        lower_receipt_refs:
          compact_list([session_start_lower_receipt_ref(session_start), lower_receipt_ref]),
        workspace_hook_refs: Enum.map(before_run_receipts ++ after_run_receipts, & &1.hook_ref)
      }
    }
  end

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

  defp session_start_action_receipts(nil), do: []

  defp session_start_action_receipts(evidence) do
    [
      %{
        operation: evidence.operation,
        status: :succeeded,
        lower_request_ref: evidence.lower_request_ref,
        lower_receipt_ref: evidence.lower_receipt_ref,
        runtime_control_session_ref: evidence.runtime_control_session_ref
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

  defp digest(value) do
    value
    |> :erlang.term_to_binary()
    |> then(&:crypto.hash(:sha256, &1))
    |> Base.encode16(case: :lower)
  end

  defp normalize(%_{} = struct), do: struct |> Map.from_struct() |> normalize()
  defp normalize(attrs) when is_map(attrs), do: attrs
  defp normalize(attrs) when is_list(attrs), do: Map.new(attrs)

  defp actor_id(attrs),
    do: map_value(attrs, :source_ref) || "actor://mezzanine/codex-agent-runtime"

  defp put_present(keyword, _key, nil), do: keyword
  defp put_present(keyword, key, value), do: Keyword.put(keyword, key, value)

  defp compact_map(map) do
    map
    |> Enum.reject(fn {_key, value} -> value in [nil, "", [], %{}] end)
    |> Map.new()
  end

  defp compact_list(list), do: Enum.reject(list, &is_nil/1)

  defp map_value(%_{} = struct, key), do: struct |> Map.from_struct() |> map_value(key)
  defp map_value(%{} = map, key), do: Map.get(map, key) || Map.get(map, Atom.to_string(key))
  defp map_value(_value, _key), do: nil

  defp present_binary?(value), do: is_binary(value) and String.trim(value) != ""

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
