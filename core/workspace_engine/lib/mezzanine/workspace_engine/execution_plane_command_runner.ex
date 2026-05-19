defmodule Mezzanine.WorkspaceEngine.ExecutionPlaneCommandRunner do
  @moduledoc """
  Execution Plane-backed command runner for workspace hooks.

  Workspace hooks may request command execution, but the workspace engine does
  not spawn shells directly. This runner translates a hook command into a
  governed process intent and delegates execution to Execution Plane.
  """

  @type hook_result :: :ok | {:ok, map()} | {:error, map() | atom()}

  @spec runner(keyword()) :: (map(), map() -> hook_result())
  def runner(opts \\ []) when is_list(opts) do
    fn hook, context -> run(hook, context, opts) end
  end

  @spec run(map(), map(), keyword()) :: hook_result()
  def run(hook, context, opts \\ []) when is_map(hook) and is_map(context) and is_list(opts) do
    with {:ok, command} <- hook_command(hook),
         {:ok, cwd} <- hook_cwd(context) do
      hook
      |> invocation(command, cwd, opts)
      |> execute(opts)
      |> normalize_result()
    end
  end

  defp execute(invocation, opts) do
    process_fun = Keyword.get(opts, :process_fun, &ExecutionPlane.Process.run/2)
    process_fun.(invocation, process_opts(opts))
  end

  defp invocation(hook, command, cwd, opts) do
    %{
      command: Keyword.get(opts, :shell_command, "/bin/sh"),
      argv: Keyword.get(opts, :shell_argv, ["-lc", command]),
      cwd: cwd,
      env: explicit_env(opts),
      clear_env: Keyword.get(opts, :clear_env, true),
      stderr_mode: Keyword.get(opts, :stderr_mode, "stdout"),
      timeout_ms: timeout_ms(hook),
      execution_surface:
        Keyword.get(opts, :execution_surface, %{
          surface_kind: "local_subprocess",
          target_id: "workspace-hook-local-process"
        })
    }
  end

  defp process_opts(opts) do
    opts
    |> Keyword.get(:process_opts, [])
    |> Keyword.put_new(
      :lineage,
      Keyword.get(opts, :lineage, %{idempotency_key: "workspace-hook"})
    )
  end

  defp normalize_result({:ok, result}), do: {:ok, hook_result(result)}
  defp normalize_result({:error, result}), do: {:error, hook_result(result)}
  defp normalize_result(other), do: {:error, {:invalid_execution_plane_result, other}}

  defp hook_result(result) do
    raw_payload =
      result
      |> value(:outcome, %{})
      |> value(:raw_payload, %{})

    %{
      status: exit_status(raw_payload),
      stdout: output(raw_payload),
      stderr: value(raw_payload, :stderr, ""),
      execution_plane: %{
        route_id: result |> value(:outcome, %{}) |> value(:route_id),
        status: result |> value(:outcome, %{}) |> value(:status)
      }
    }
  end

  defp output(raw_payload) do
    value(raw_payload, :output) || value(raw_payload, :stdout, "")
  end

  defp exit_status(raw_payload) do
    raw_payload
    |> value(:exit, %{})
    |> value(:code, nil)
    |> case do
      status when is_integer(status) -> status
      _missing -> if value(raw_payload, :status) in [:success, "success"], do: 0, else: 1
    end
  end

  defp hook_command(hook) do
    attrs = value(hook, :attrs, %{}) || %{}

    case value(attrs, :command) || value(hook, :command) do
      command when is_binary(command) and command != "" -> {:ok, command}
      _missing -> {:error, :missing_hook_command}
    end
  end

  defp hook_cwd(context) do
    case value(context, :cwd) || value(context, :concrete_path) do
      cwd when is_binary(cwd) and cwd != "" -> {:ok, cwd}
      _missing -> {:error, :missing_hook_cwd}
    end
  end

  defp timeout_ms(hook) do
    case value(hook, :timeout_ms) do
      timeout_ms when is_integer(timeout_ms) and timeout_ms > 0 -> timeout_ms
      _other -> nil
    end
  end

  defp explicit_env(opts) do
    opts
    |> Keyword.get(:env, %{})
    |> Enum.flat_map(fn
      {key, value} when is_atom(key) and is_binary(value) -> [{Atom.to_string(key), value}]
      {key, value} when is_binary(key) and is_binary(value) -> [{key, value}]
      _other -> []
    end)
    |> Map.new()
  end

  defp value(attrs, key, default \\ nil)

  defp value(%_{} = struct, key, default), do: struct |> Map.from_struct() |> value(key, default)

  defp value(attrs, key, default) when is_atom(key) do
    case Map.fetch(attrs, key) do
      {:ok, value} ->
        value

      :error ->
        case Map.fetch(attrs, Atom.to_string(key)) do
          {:ok, value} -> value
          :error -> default
        end
    end
  end
end
