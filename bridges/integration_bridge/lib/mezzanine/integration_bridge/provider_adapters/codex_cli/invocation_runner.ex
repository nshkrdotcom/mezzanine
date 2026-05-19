defmodule Mezzanine.IntegrationBridge.ProviderAdapters.CodexCli.InvocationRunner do
  @moduledoc false

  alias Mezzanine.IntegrationBridge.ProviderAdapters.CodexCli.Support
  alias Mezzanine.IntegrationBridge.ProviderAdapters.CodexCli.WorkspacePreparer
  alias Mezzanine.IntegrationBridge.ProviderAuthorityAdmission

  import Support, except: [requested_max_turns: 1]

  @capability_id "codex.session.turn"

  def run(attrs, opts, invoke_fun, workspace_root, connection_id, workspace) do
    with {:ok, before_run_receipts} <- WorkspacePreparer.run_hooks(workspace, :before_run, opts),
         invoke_opts <- codex_invoke_opts(attrs, connection_id, opts, workspace_root),
         {:ok, turn_attempts} <-
           run_codex_turns(attrs, opts, invoke_fun, workspace_root, invoke_opts) do
      {:ok, turn_attempts, invoke_opts, before_run_receipts}
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
end
