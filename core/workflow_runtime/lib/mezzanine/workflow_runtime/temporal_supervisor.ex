defmodule Mezzanine.WorkflowRuntime.TemporalSupervisor do
  @moduledoc """
  Mezzanine-owned Temporalex worker supervision contract.

  The module builds explicit `Temporalex` child specs from the workflow/activity
  registry. Runtime environments opt in with configuration; disabled
  environments keep the application inert and do not silently depend on a local
  Temporal daemon.
  """

  alias Mezzanine.WorkflowRuntime.DurableOrchestrationDecision

  @default_address "127.0.0.1:7233"
  @default_namespace "default"
  @default_instance_base Mezzanine.WorkflowRuntime.Temporal

  @type runtime_config :: keyword()

  @doc "Returns normalized Temporal runtime config."
  @spec runtime_config(keyword()) :: runtime_config()
  def runtime_config(overrides \\ []) do
    :mezzanine_workflow_runtime
    |> Application.get_env(:temporal, [])
    |> Keyword.merge(overrides)
    |> Keyword.put_new(:enabled?, false)
    |> Keyword.put_new(:address, @default_address)
    |> Keyword.put_new(:namespace, @default_namespace)
    |> Keyword.put_new(:instance_base, @default_instance_base)
    |> Keyword.put_new(:max_concurrent_workflow_tasks, 5)
    |> Keyword.put_new(:max_concurrent_activity_tasks, 5)
    |> Keyword.put_new(:headers, [])
  end

  @doc "Returns Temporalex child specs, or an empty list when Temporal is disabled."
  @spec child_specs(keyword()) :: [Supervisor.child_spec()]
  def child_specs(overrides \\ []) do
    config = runtime_config(overrides)

    if Keyword.fetch!(config, :enabled?) do
      config
      |> task_queue_specs()
      |> Enum.map(& &1.child_spec)
    else
      []
    end
  end

  @doc "Returns the per-task-queue worker specs without starting them."
  @spec task_queue_specs(runtime_config()) :: [map()]
  def task_queue_specs(config \\ runtime_config()) do
    config = runtime_config(config)

    DurableOrchestrationDecision.task_queues()
    |> Enum.map(&task_queue_spec(&1, config))
  end

  @doc "Returns the configured Temporalex instance module for a task queue."
  @spec instance_name(String.t(), runtime_config()) :: module()
  def instance_name(task_queue, config \\ runtime_config()) do
    Module.concat(Keyword.fetch!(config, :instance_base), task_queue_suffix(task_queue))
  end

  @doc "Returns the Temporalex connection name for a task queue."
  @spec connection_name(String.t(), runtime_config()) :: module()
  def connection_name(task_queue, config \\ runtime_config()) do
    task_queue
    |> instance_name(config)
    |> Temporalex.connection_name()
  end

  defp task_queue_spec(task_queue, config) do
    name = instance_name(task_queue, config)

    opts =
      config
      |> Keyword.take([
        :namespace,
        :max_concurrent_workflow_tasks,
        :max_concurrent_activity_tasks
      ])
      |> Keyword.put(:name, name)
      |> Keyword.put(:address, temporalex_address(Keyword.fetch!(config, :address)))
      |> Keyword.put(:task_queue, task_queue)
      |> Keyword.put(:workflows, workflows_for(task_queue))
      |> Keyword.put(:activities, activities_for(task_queue))
      |> maybe_put(:api_key, Keyword.get(config, :api_key))
      |> maybe_put(:headers, Keyword.get(config, :headers))

    %{
      task_queue: task_queue,
      name: name,
      connection: Temporalex.connection_name(name),
      workflows: Keyword.fetch!(opts, :workflows),
      activities: Keyword.fetch!(opts, :activities),
      child_spec: %{
        id: name,
        start: {Temporalex, :start_link, [opts]},
        type: :supervisor
      }
    }
  end

  defp workflows_for(task_queue) do
    DurableOrchestrationDecision.workflow_types()
    |> Enum.filter(&(&1.task_queue == task_queue))
    |> Enum.map(& &1.module)
  end

  defp activities_for(task_queue) do
    DurableOrchestrationDecision.activity_registrations()
    |> Enum.filter(&(&1.task_queue == task_queue))
    |> Enum.map(& &1.module)
  end

  defp temporalex_address("http://" <> _rest = address), do: address
  defp temporalex_address("https://" <> _rest = address), do: address
  defp temporalex_address(address), do: "http://" <> address

  defp task_queue_suffix(task_queue) do
    task_queue
    |> String.replace(~r/[^A-Za-z0-9]+/, "_")
    |> String.trim("_")
    |> Macro.camelize()
  end

  defp maybe_put(opts, _key, nil), do: opts
  defp maybe_put(opts, _key, []), do: opts
  defp maybe_put(opts, key, value), do: Keyword.put(opts, key, value)
end
