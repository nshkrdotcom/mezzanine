defmodule Mezzanine.WorkspaceEngine.Cleanup do
  @moduledoc """
  Redacted local workspace cleanup receipts.

  Cleanup revalidates path safety immediately before deletion, runs
  `before_remove` hooks through the workspace hook contract, and never includes
  concrete filesystem paths in the returned receipt.
  """

  alias Mezzanine.WorkspaceEngine.{Hooks, PathSafety, WorkspaceRecord}

  @remove_policies [:on_terminal, :terminal, :delete, "on_terminal", "terminal", "delete"]

  @spec remove(WorkspaceRecord.t(), keyword()) ::
          {:ok, map()}
          | {:error,
             {:cleanup_denied | :cleanup_failed | :cleanup_hook_failed | :cleanup_hook_timeout,
              map()}}
  def remove(%WorkspaceRecord{} = workspace, opts \\ []) when is_list(opts) do
    if cleanup_policy_removes?(workspace.cleanup_policy) do
      remove_workspace(workspace, opts)
    else
      {:ok, receipt(workspace, :skipped, false, :cleanup_policy_never, [])}
    end
  end

  defp remove_workspace(workspace, opts) do
    with :ok <- PathSafety.validate(workspace.concrete_root, workspace.concrete_path),
         {:ok, hook_receipts} <- Hooks.run(workspace, :before_remove, opts),
         {:ok, removed?} <- remove_path(workspace.concrete_path) do
      {:ok, receipt(workspace, :removed, removed?, nil, hook_receipts)}
    else
      {:error, {:hook_failed, hook_receipt}} ->
        {:error,
         {:cleanup_hook_failed,
          receipt(workspace, :failed, false, hook_receipt.reason, [hook_receipt])}}

      {:error, {:hook_timeout, hook_receipt}} ->
        {:error,
         {:cleanup_hook_timeout,
          receipt(workspace, :failed, false, hook_receipt.reason, [hook_receipt])}}

      {:error, {:remove_failed, reason}} ->
        {:error, {:cleanup_failed, receipt(workspace, :failed, false, reason, [])}}

      {:error, reason} when is_atom(reason) ->
        {:error, {:cleanup_denied, receipt(workspace, :denied, false, reason, [])}}
    end
  end

  defp remove_path(path) do
    existed? = File.exists?(path)

    case File.rm_rf(path) do
      {:ok, _removed_paths} -> {:ok, existed?}
      {:error, _path, reason} -> {:error, {:remove_failed, reason}}
    end
  end

  defp cleanup_policy_removes?(policy), do: policy in @remove_policies

  defp receipt(workspace, status, removed?, reason, hook_receipts) do
    %{
      receipt_ref: receipt_ref(workspace, status),
      workspace_ref: "workspace://#{workspace.workspace_id}",
      workspace_id: workspace.workspace_id,
      cleanup_policy: workspace.cleanup_policy,
      status: status,
      removed?: removed?,
      reason: reason,
      hook_receipts: hook_receipts,
      safety_hash: workspace.safety_hash,
      path_redacted?: true,
      observed_at: DateTime.utc_now() |> DateTime.to_iso8601()
    }
  end

  defp receipt_ref(workspace, status) do
    digest =
      [workspace.workspace_id, workspace.safety_hash, status]
      |> :erlang.term_to_binary()
      |> then(&:crypto.hash(:sha256, &1))
      |> Base.encode16(case: :lower)
      |> binary_part(0, 16)

    "cleanup-receipt://#{workspace.workspace_id}/#{status}/#{digest}"
  end
end
