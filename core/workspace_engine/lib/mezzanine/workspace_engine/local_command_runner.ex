defmodule Mezzanine.WorkspaceEngine.LocalCommandRunner do
  @moduledoc """
  Local shell-command runner for workspace hooks.

  The caller supplies any environment values explicitly. This module never reads
  process environment variables.
  """

  @spec runner(keyword()) :: (map(), map() -> :ok | {:ok, map()} | {:error, map() | atom()})
  def runner(opts \\ []) when is_list(opts) do
    fn hook, context -> run(hook, context, opts) end
  end

  @spec run(map(), map(), keyword()) :: :ok | {:ok, map()} | {:error, map() | atom()}
  def run(hook, context, opts \\ []) when is_map(hook) and is_map(context) and is_list(opts) do
    with {:ok, command} <- hook_command(hook),
         {:ok, cwd} <- hook_cwd(context) do
      command_opts = [
        cd: cwd,
        stderr_to_stdout: true,
        env: explicit_env(opts)
      ]

      case System.cmd("sh", ["-lc", command], command_opts) do
        {output, 0} -> {:ok, %{status: 0, stdout: output, stderr: ""}}
        {output, status} -> {:error, %{status: status, stdout: output, stderr: ""}}
      end
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

  defp explicit_env(opts) do
    opts
    |> Keyword.get(:env, %{})
    |> Enum.flat_map(fn
      {key, value} when is_atom(key) and is_binary(value) -> [{Atom.to_string(key), value}]
      {key, value} when is_binary(key) and is_binary(value) -> [{key, value}]
      _other -> []
    end)
  end

  defp value(attrs, key, default \\ nil) when is_atom(key) do
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
