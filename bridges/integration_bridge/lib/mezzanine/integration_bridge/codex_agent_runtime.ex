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

  @capability_id "codex.session.turn"
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
         :ok <- prepare_workspace(workspace_root, opts),
         invoke_opts <- codex_invoke_opts(attrs, connection_id, opts, workspace_root),
         {:ok, result} <-
           invoke_fun.(@capability_id, codex_input(attrs, opts, workspace_root), invoke_opts) do
      {:ok, projection(attrs, result, invoke_opts)}
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

  defp projection(attrs, result, invoke_opts) do
    run = map_value(result, :run) || %{}
    attempt = map_value(result, :attempt) || %{}
    output = map_value(result, :output) || %{}
    run_ref = map_value(attrs, :run_ref)
    workflow_ref = "workflow://codex-agent-runtime/#{ref_suffix(run_ref)}"
    turn_ref = "turn://codex-agent-runtime/#{ref_suffix(run_ref)}/1"

    lower_request_ref =
      "lower-request://#{map_value(run, :run_id) || ref_suffix(run_ref)}/#{@capability_id}"

    lower_receipt_ref = lower_receipt_ref(attempt, lower_request_ref)
    status = output_status(output)
    observed_at = DateTime.utc_now()

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
      ],
      action_receipts: [
        %{
          status: :succeeded,
          lower_receipt_ref: lower_receipt_ref,
          output_artifact_refs: output_artifact_refs(result)
        }
      ],
      runtime_events: [
        %{
          event_ref: "event://codex-agent-runtime/#{ref_suffix(run_ref)}/terminal",
          event_seq: 1,
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
      ],
      budget_state: %{"turns_remaining" => 0},
      candidate_fact_refs: ["candidate-fact://codex-agent-runtime/#{ref_suffix(run_ref)}/1"],
      memory_proof_refs: [],
      receipt_ref_set: %{
        lower_request_refs: [lower_request_ref],
        lower_receipt_refs: [lower_receipt_ref]
      }
    }
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
