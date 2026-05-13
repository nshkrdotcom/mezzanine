defmodule Mezzanine.WorkspaceEngine.Cleanup do
  @moduledoc """
  Redacted local workspace cleanup receipts.

  Cleanup revalidates path safety immediately before deletion, runs
  `before_remove` hooks through the workspace hook contract without letting hook
  failures block deletion, and never includes concrete filesystem paths in the
  returned receipt.
  """

  alias Mezzanine.WorkspaceEngine.{Hooks, PathSafety, WorkspaceRecord}

  @remove_policies [:on_terminal, :terminal, :delete, "on_terminal", "terminal", "delete"]

  @spec remove(WorkspaceRecord.t(), keyword()) ::
          {:ok, map()}
          | {:error, {:cleanup_denied | :cleanup_failed, map()}}
  def remove(%WorkspaceRecord{} = workspace, opts \\ []) when is_list(opts) do
    if cleanup_policy_removes?(workspace.cleanup_policy) do
      remove_workspace(workspace, opts)
    else
      {:ok, receipt(workspace, :skipped, false, :cleanup_policy_never, [])}
    end
  end

  defp remove_workspace(workspace, opts) do
    case PathSafety.validate(workspace.concrete_root, workspace.concrete_path) do
      :ok ->
        hook_receipts = before_remove_hook_receipts(workspace, opts)

        case remove_path(workspace.concrete_path) do
          {:ok, removed?} ->
            {:ok, receipt(workspace, :removed, removed?, nil, hook_receipts)}

          {:error, {:remove_failed, reason}} ->
            {:error, {:cleanup_failed, receipt(workspace, :failed, false, reason, hook_receipts)}}
        end

      {:error, reason} when is_atom(reason) ->
        {:error, {:cleanup_denied, receipt(workspace, :denied, false, reason, [])}}
    end
  end

  defp before_remove_hook_receipts(workspace, opts) do
    case Hooks.run(workspace, :before_remove, opts) do
      {:ok, hook_receipts} -> hook_receipts
      {:error, {_kind, hook_receipt}} -> [hook_receipt]
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
