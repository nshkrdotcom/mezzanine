defmodule Mezzanine.IntegrationBridge.ProviderAdapters.CodexCli.AgentRuntime do
  @moduledoc """
  Lower-owned Codex agent runtime for AppKit AgentIntake.

  The runtime translates an AppKit/Mezzanine agent-run spec into a governed Jido
  `codex.session.turn` invocation and returns an M2 projection shape that AppKit
  can expose through its headless readback DTOs.
  """

  alias Jido.Integration.V2
  alias Mezzanine.IntegrationBridge.ProviderAdapters.CodexCli.ConnectionInstaller
  alias Mezzanine.IntegrationBridge.ProviderAdapters.CodexCli.InvocationRunner
  alias Mezzanine.IntegrationBridge.ProviderAdapters.CodexCli.ReadbackProjector
  alias Mezzanine.IntegrationBridge.ProviderAdapters.CodexCli.Support
  alias Mezzanine.IntegrationBridge.ProviderAdapters.CodexCli.WorkspacePreparer

  import Support

  @spec run(map() | keyword()) :: {:ok, map()} | {:error, term()}
  def run(attrs), do: run(attrs, [])

  @spec run(map() | keyword(), keyword()) :: {:ok, map()} | {:error, term()}
  def run(attrs, opts) when is_list(opts) do
    attrs = normalize(attrs)
    invoke_fun = Keyword.get(opts, :invoke_fun, &V2.invoke/3)
    workspace_root = codex_workspace_root(opts)

    with :ok <- ConnectionInstaller.maybe_start_runtime_router(opts),
         :ok <- ConnectionInstaller.maybe_register_connector(opts),
         {:ok, connection_id} <- ConnectionInstaller.connection_id(attrs, opts),
         :ok <- WorkspacePreparer.prepare(workspace_root, opts) do
      workspace = WorkspacePreparer.for_hooks(attrs, opts, workspace_root)

      attrs
      |> InvocationRunner.run(opts, invoke_fun, workspace_root, connection_id, workspace)
      |> maybe_stop_codex_session_result(attrs, opts, invoke_fun)
      |> complete_codex_attempt(attrs, workspace, opts)
    end
  end

  def run(_attrs, _opts), do: {:error, :invalid_codex_agent_runtime_opts}

  defp complete_codex_attempt(attempt_result, attrs, workspace, opts) do
    after_run_receipts = WorkspacePreparer.run_after_run_hooks(workspace, opts)

    case {attempt_result, after_run_receipts} do
      {{:ok, turn_attempts, invoke_opts, before_run_receipts, session_stop}, receipts} ->
        {:ok,
         ReadbackProjector.project(
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

  defp maybe_stop_codex_session_result(
         {:ok, turn_attempts, invoke_opts, before_run_receipts},
         attrs,
         opts,
         invoke_fun
       ) do
    case ReadbackProjector.maybe_stop_session(attrs, opts, invoke_fun, invoke_opts, turn_attempts) do
      {:ok, session_stop} -> {:ok, turn_attempts, invoke_opts, before_run_receipts, session_stop}
      {:error, reason} -> {:error, reason}
    end
  end

  defp maybe_stop_codex_session_result({:error, _reason} = error, _attrs, _opts, _invoke_fun),
    do: error
end
