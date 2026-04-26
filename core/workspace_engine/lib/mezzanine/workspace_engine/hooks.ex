defmodule Mezzanine.WorkspaceEngine.Hooks do
  @moduledoc """
  Fail-closed workspace hook execution contracts.

  The workspace engine owns hook selection, timeout posture, and receipts. The
  concrete hook implementation is supplied by the caller so lower process and
  remote placement mechanics stay outside this neutral package.
  """

  alias Mezzanine.WorkspaceEngine.WorkspaceRecord

  @default_timeout_ms 30_000

  @spec run(WorkspaceRecord.t(), atom() | String.t(), keyword()) ::
          {:ok, [map()]} | {:error, {:hook_failed | :hook_timeout, map()}}
  def run(%WorkspaceRecord{} = workspace, stage, opts \\ []) when is_list(opts) do
    runner = Keyword.get(opts, :runner, &default_runner/2)

    workspace.hook_specs
    |> Enum.map(&normalize_hook/1)
    |> Enum.filter(&(normalize_stage(&1.stage) == normalize_stage(stage)))
    |> Enum.reduce_while({:ok, []}, fn hook, {:ok, receipts} ->
      case execute_hook(hook, workspace, runner) do
        {:ok, receipt} -> {:cont, {:ok, receipts ++ [receipt]}}
        {:error, reason} -> {:halt, {:error, reason}}
      end
    end)
  end

  defp execute_hook(hook, workspace, runner) do
    context = context(workspace, hook)
    timeout_ms = timeout_ms(hook)

    task = Task.async(fn -> runner.(hook, context) end)

    case Task.yield(task, timeout_ms) || Task.shutdown(task, :brutal_kill) do
      {:ok, {:ok, result}} ->
        {:ok, receipt(hook, :succeeded, result, nil)}

      {:ok, :ok} ->
        {:ok, receipt(hook, :succeeded, %{}, nil)}

      {:ok, {:error, reason}} ->
        {:error, {:hook_failed, receipt(hook, :failed, %{}, reason)}}

      {:ok, other} ->
        {:error, {:hook_failed, receipt(hook, :failed, %{}, {:invalid_hook_result, other})}}

      nil ->
        {:error, {:hook_timeout, receipt(hook, :timed_out, %{}, :timeout)}}
    end
  end

  defp default_runner(_hook, _context), do: :ok

  defp context(%WorkspaceRecord{} = workspace, hook) do
    %{
      workspace_id: workspace.workspace_id,
      installation_id: workspace.installation_id,
      subject_id: workspace.subject_id,
      concrete_path: workspace.concrete_path,
      stage: hook.stage
    }
  end

  defp receipt(hook, status, result, reason) do
    %{
      hook_ref: hook.hook_ref,
      stage: hook.stage,
      status: status,
      timeout_ms: hook.timeout_ms,
      result: result,
      reason: reason
    }
  end

  defp normalize_hook(hook) when is_map(hook) do
    %{
      hook_ref: value(hook, :hook_ref) || value(hook, :ref) || value(hook, :name),
      stage: value(hook, :stage),
      timeout_ms: timeout_ms(hook),
      attrs: value(hook, :attrs) || %{}
    }
  end

  defp normalize_hook(hook) when is_atom(hook) do
    %{hook_ref: Atom.to_string(hook), stage: hook, timeout_ms: @default_timeout_ms, attrs: %{}}
  end

  defp timeout_ms(hook) do
    case value(hook, :timeout_ms) do
      timeout when is_integer(timeout) and timeout > 0 -> timeout
      _other -> @default_timeout_ms
    end
  end

  defp normalize_stage(stage) when is_atom(stage), do: Atom.to_string(stage)
  defp normalize_stage(stage) when is_binary(stage), do: stage
  defp normalize_stage(stage), do: to_string(stage)

  defp value(attrs, key), do: Map.get(attrs, key) || Map.get(attrs, Atom.to_string(key))
end
