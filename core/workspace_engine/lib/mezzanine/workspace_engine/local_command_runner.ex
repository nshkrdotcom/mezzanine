defmodule Mezzanine.WorkspaceEngine.LocalCommandRunner do
  @moduledoc """
  Simulation-profile command runner for workspace hooks.

  Production hook command execution must use Execution Plane. This module keeps
  the legacy local-runner name available only for explicit simulation/test
  profiles and delegates to the Execution Plane-backed runner.
  """

  alias Mezzanine.WorkspaceEngine.ExecutionPlaneCommandRunner

  @spec runner(keyword()) :: (map(), map() -> :ok | {:ok, map()} | {:error, map() | atom()})
  def runner(opts \\ []) when is_list(opts) do
    fn hook, context -> run(hook, context, opts) end
  end

  @spec run(map(), map(), keyword()) :: :ok | {:ok, map()} | {:error, map() | atom()}
  def run(hook, context, opts \\ []) when is_map(hook) and is_map(context) and is_list(opts) do
    if simulation_profile?(opts) do
      ExecutionPlaneCommandRunner.run(hook, context, opts)
    else
      {:error, :local_command_runner_requires_simulation_profile}
    end
  end

  defp simulation_profile?(opts),
    do: Keyword.get(opts, :profile) in [:simulation, :test, "simulation", "test"]
end
