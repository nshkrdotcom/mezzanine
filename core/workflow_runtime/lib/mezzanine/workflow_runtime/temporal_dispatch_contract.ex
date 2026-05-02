defmodule Mezzanine.WorkflowRuntime.TemporalDispatchContract do
  @moduledoc """
  Phase 6 Temporal dispatch evidence contract.

  The contract joins the Mezzanine-owned Temporal worker registration, the
  ExecutionAttempt workflow, compact describe/query references, and retained
  workflow-start outbox persistence into one service-mode evidence surface.
  """

  alias Mezzanine.WorkflowRuntime.DurableOrchestrationDecision
  alias Mezzanine.WorkflowRuntime.ExecutionLifecycleWorkflow
  alias Mezzanine.WorkflowRuntime.TemporalSupervisor
  alias Mezzanine.WorkflowRuntime.WorkflowStarterOutbox

  @contract_id "TemporalDispatchContract.v1"
  @owner :mezzanine
  @primary_repos [:mezzanine, :stack_lab]
  @execution_attempt_task_queue "mezzanine.hazmat"
  @execution_attempt_workflow Mezzanine.Workflows.ExecutionAttempt
  @p5p_residual_ref "P5P-007"

  @required_fields [
    :temporal_namespace,
    :task_queue_refs,
    :worker_identity_refs,
    :worker_health_refs,
    :workflow_type_refs,
    :execution_attempt_workflow_refs,
    :compact_describe_query_refs,
    :outbox_gc_only_posture,
    :active_workflow_state_before_restart_ref,
    :restart_procedure_ref,
    :replay_or_continuation_evidence_ref,
    :persisted_outcome_state_ref
  ]
  @key_lookup Map.new(
                @required_fields ++ [:dispatch_state, :workflow_run_id],
                &{Atom.to_string(&1), &1}
              )

  @forbidden [
    :raw_workflow_history_in_evidence,
    :destructive_temporal_reset_without_user_approval,
    :ad_hoc_temporal_server_start_dev_outside_just_dev_up,
    :lower_runtime_smoke_claimed_as_service_mode_evidence
  ]

  @local_substrate_command_refs [
    "just dev-up",
    "just dev-status",
    "just temporal-restart",
    "just dev-status"
  ]

  @doc "Static Phase 6 contract metadata."
  @spec contract() :: map()
  def contract do
    %{
      id: @contract_id,
      owner: @owner,
      primary_repos: @primary_repos,
      required_fields: @required_fields,
      forbidden: @forbidden,
      workflow_runtime_boundary: Mezzanine.WorkflowRuntime,
      temporal_adapter: DurableOrchestrationDecision.runtime_adapter(),
      temporal_supervisor: TemporalSupervisor,
      execution_attempt_workflow: @execution_attempt_workflow,
      execution_attempt_task_queue: @execution_attempt_task_queue,
      outbox_contract: WorkflowStarterOutbox.schema_contract(),
      p5p_residual_ref: @p5p_residual_ref
    }
  end

  @doc """
  Builds compact restart/replay evidence for the ExecutionAttempt service-mode path.

  This function does not export raw Temporal history. Runtime callers that have a
  local outbox store can pass `:outbox_persistence` to prove the start outcome was
  recorded before acknowledging the dispatcher.
  """
  @spec restart_replay_evidence(map() | keyword(), keyword()) :: {:ok, map()} | {:error, term()}
  def restart_replay_evidence(lifecycle_attrs, opts \\ []) do
    lifecycle_attrs = normalize(lifecycle_attrs)
    runtime_config = runtime_config(opts)
    worker_specs = Keyword.get_lazy(opts, :worker_specs, fn -> worker_specs(runtime_config) end)
    outbox_row = Keyword.fetch!(opts, :outbox_row)

    namespace = Keyword.fetch!(runtime_config, :namespace)

    with {:ok, worker_health_refs} <- worker_health_refs(worker_specs, namespace),
         {:ok, start_request} <- WorkflowStarterOutbox.start_request(outbox_row),
         :ok <- validate_execution_attempt_start(start_request),
         {:ok, recovery} <- ExecutionLifecycleWorkflow.worker_failover_recovery(lifecycle_attrs),
         {:ok, persisted_outcome_state_ref} <-
           persisted_outcome_state_ref(
             outbox_row,
             Keyword.get_lazy(opts, :outcome_row, fn ->
               default_outcome_row(outbox_row, lifecycle_attrs)
             end),
             outbox_persistence: Keyword.get(opts, :outbox_persistence)
           ) do
      workflow_id = fetch!(lifecycle_attrs, :workflow_id)
      workflow_run_id = Map.get(lifecycle_attrs, :workflow_run_id)

      {:ok,
       %{
         contract_id: @contract_id,
         owner: @owner,
         primary_repos: @primary_repos,
         p5p_residual_ref: @p5p_residual_ref,
         temporal_namespace: namespace,
         task_queue_refs: task_queue_refs(namespace, worker_specs),
         worker_identity_refs: worker_identity_refs(namespace, worker_specs),
         worker_health_refs: worker_health_refs,
         workflow_runtime_boundary: Mezzanine.WorkflowRuntime,
         temporal_adapter: DurableOrchestrationDecision.runtime_adapter(),
         workflow_type_refs: workflow_type_refs(),
         execution_attempt_workflow_refs:
           execution_attempt_workflow_refs(lifecycle_attrs, start_request),
         compact_describe_query_refs: compact_describe_query_refs(workflow_id, workflow_run_id),
         outbox_gc_only_posture: "gc_allowed_only_after_temporal_outcome_persisted",
         active_workflow_state_before_restart_ref:
           active_workflow_state_before_restart_ref(workflow_id),
         restart_procedure_ref: "mezzanine-just://temporal-restart",
         replay_or_continuation_evidence_ref: replay_or_continuation_evidence_ref(recovery),
         persisted_outcome_state_ref: persisted_outcome_state_ref,
         local_substrate_command_refs: @local_substrate_command_refs,
         raw_workflow_history_included?: false
       }}
    end
  end

  @doc """
  Records or formats the persisted workflow-start outcome reference.

  Passing `:outbox_persistence` delegates to the configured store. A failed store
  write returns an error so callers do not treat an unpersisted Temporal outcome
  as acknowledged evidence.
  """
  @spec persisted_outcome_state_ref(map() | struct(), map() | struct(), keyword()) ::
          {:ok, String.t()} | {:error, term()}
  def persisted_outcome_state_ref(original_row, outcome_row, opts \\ []) do
    original_row = normalize(original_row)
    outcome_row = normalize(outcome_row)

    with :ok <-
           maybe_record_outcome(original_row, outcome_row, Keyword.get(opts, :outbox_persistence)) do
      {:ok,
       "workflow-start-outbox://#{fetch!(original_row, :outbox_id)}/#{fetch!(outcome_row, :dispatch_state)}/#{fetch!(outcome_row, :workflow_run_id)}"}
    end
  end

  defp runtime_config(opts) do
    opts
    |> Keyword.get(:runtime_config, [])
    |> TemporalSupervisor.runtime_config()
    |> Keyword.put(:enabled?, true)
  end

  defp worker_specs(runtime_config), do: TemporalSupervisor.task_queue_specs(runtime_config)

  defp worker_health_refs(worker_specs, namespace) do
    case Enum.find(worker_specs, &(&1.task_queue == @execution_attempt_task_queue)) do
      nil ->
        {:error, {:missing_worker, @execution_attempt_task_queue}}

      %{workflows: workflows} = hazmat ->
        if @execution_attempt_workflow in workflows do
          {:ok, Enum.map(worker_specs, &worker_health_ref(namespace, &1))}
        else
          {:error, {:missing_worker, Map.fetch!(hazmat, :task_queue)}}
        end
    end
  end

  defp worker_health_ref(namespace, spec) do
    %{
      task_queue: spec.task_queue,
      worker_identity_ref: worker_identity_ref(namespace, spec),
      workflows: spec.workflows,
      activities: spec.activities,
      execution_attempt_registered?: @execution_attempt_workflow in spec.workflows,
      status: "configured"
    }
  end

  defp validate_execution_attempt_start(%{task_queue: task_queue})
       when task_queue != @execution_attempt_task_queue do
    {:error, {:wrong_task_queue, %{expected: @execution_attempt_task_queue, got: task_queue}}}
  end

  defp validate_execution_attempt_start(%{workflow_module: @execution_attempt_workflow}), do: :ok

  defp validate_execution_attempt_start(%{workflow_module: workflow_module}) do
    {:error,
     {:wrong_workflow_module, %{expected: @execution_attempt_workflow, got: workflow_module}}}
  end

  defp task_queue_refs(namespace, worker_specs) do
    worker_specs
    |> Enum.map(& &1.task_queue)
    |> Enum.map(&"temporal-task-queue://#{namespace}/#{&1}")
  end

  defp worker_identity_refs(namespace, worker_specs) do
    Enum.map(worker_specs, &worker_identity_ref(namespace, &1))
  end

  defp worker_identity_ref(namespace, spec) do
    "temporal-worker://#{namespace}/#{inspect(spec.name)}"
  end

  defp workflow_type_refs do
    DurableOrchestrationDecision.workflow_types()
    |> Enum.map(&"workflow://#{inspect(&1.module)}")
  end

  defp execution_attempt_workflow_refs(lifecycle_attrs, start_request) do
    %{
      workflow_module: @execution_attempt_workflow,
      task_queue: @execution_attempt_task_queue,
      workflow_id: fetch!(lifecycle_attrs, :workflow_id),
      workflow_run_id: Map.get(lifecycle_attrs, :workflow_run_id),
      start_request_task_queue: start_request.task_queue,
      start_request_workflow_module: start_request.workflow_module,
      boundary: Mezzanine.WorkflowRuntime
    }
  end

  defp compact_describe_query_refs(workflow_id, nil) do
    [
      "temporal-describe://#{workflow_id}",
      "temporal-query://#{workflow_id}/operator_state.v1"
    ]
  end

  defp compact_describe_query_refs(workflow_id, workflow_run_id) do
    [
      "temporal-describe://#{workflow_id}/#{workflow_run_id}",
      "temporal-query://#{workflow_id}/operator_state.v1"
    ]
  end

  defp active_workflow_state_before_restart_ref(workflow_id) do
    "temporal-query://#{workflow_id}/operator_state.v1#accepted_active"
  end

  defp replay_or_continuation_evidence_ref(recovery) do
    "temporal-replay://#{fetch!(recovery, :workflow_id)}/idempotency/#{fetch!(recovery, :idempotency_key)}"
  end

  defp default_outcome_row(outbox_row, lifecycle_attrs) do
    outbox_row
    |> normalize()
    |> Map.merge(%{
      dispatch_state: "started",
      workflow_run_id: Map.get(lifecycle_attrs, :workflow_run_id)
    })
  end

  defp maybe_record_outcome(_original_row, _outcome_row, nil), do: :ok

  defp maybe_record_outcome(original_row, outcome_row, store) do
    case store.record_start_outcome(original_row, outcome_row) do
      :ok -> :ok
      {:error, reason} -> {:error, {:outbox_outcome_not_persisted, reason}}
    end
  end

  defp normalize(attrs) when is_list(attrs), do: attrs |> Map.new() |> normalize_keys()
  defp normalize(%_{} = struct), do: struct |> Map.from_struct() |> normalize_keys()
  defp normalize(map) when is_map(map), do: normalize_keys(map)

  defp normalize_keys(map) do
    map
    |> Enum.map(fn {key, value} -> {normalize_key(key), value} end)
    |> Map.new()
  end

  defp normalize_key(key) when is_atom(key), do: key

  defp normalize_key(key) when is_binary(key) do
    Map.get(@key_lookup, key, key)
  end

  defp fetch!(map, key) do
    case Map.fetch(map, key) do
      {:ok, value} when value not in [nil, ""] -> value
      _missing -> raise ArgumentError, "missing required Temporal dispatch field #{inspect(key)}"
    end
  end
end
