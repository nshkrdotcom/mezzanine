defmodule Mezzanine.Execution.OperatorActionClassification do
  @moduledoc """
  Source-owned classifier for operator effects.

  Every operator effect must be either a Temporal workflow signal intent or a
  declared local mutation owned by a bounded context. Legacy string refs,
  callbacks, raw row writes, and retired Oban/lower-worker effects fail closed.
  """

  @release_manifest_ref "phase5-v7-m02ah-operator-actions-signal-or-local-mutation"
  @signal_contract "Mezzanine.OperatorWorkflowSignal.v1"
  @signal_boundary "Mezzanine.WorkflowRuntime.signal_workflow/1"
  @allowed_kinds [:workflow_signal, :declared_local_mutation]
  @forbidden_kinds [
    :raw_sql_write,
    :oban_saga_job,
    :lower_cancel_worker,
    :unclassified_ref,
    :callback,
    :anonymous_callback
  ]

  @signal_registry %{
    cancel: %{signal_name: "operator.cancel", signal_version: "operator-cancel.v1"},
    pause: %{signal_name: "operator.pause", signal_version: "operator-pause.v1"},
    resume: %{signal_name: "operator.resume", signal_version: "operator-resume.v1"},
    retry: %{signal_name: "operator.retry", signal_version: "operator-retry.v1"},
    replan: %{signal_name: "operator.replan", signal_version: "operator-replan.v1"}
  }

  @local_owners %{
    audit_event: %{owner: :audit_evidence, owner_module: "Mezzanine.Audit.WorkAudit"},
    control_session: %{owner: :control_session, owner_module: "Mezzanine.Control.ControlSession"},
    execution_cancel: %{
      owner: :execution_ledger,
      owner_module: "Mezzanine.Execution.ExecutionRecord"
    },
    lease_invalidation: %{owner: :leasing, owner_module: "Mezzanine.Leasing"},
    operator_intervention: %{
      owner: :control_session,
      owner_module: "Mezzanine.Control.OperatorIntervention"
    },
    run_state: %{owner: :run_ledger, owner_module: "Mezzanine.Runs.Run"},
    run_series: %{owner: :run_ledger, owner_module: "Mezzanine.Runs.RunSeries"},
    subject_status: %{owner: :object_lifecycle, owner_module: "Mezzanine.Objects.SubjectRecord"},
    work_object: %{owner: :work_ledger, owner_module: "Mezzanine.Work.WorkObject"},
    work_plan: %{owner: :work_ledger, owner_module: "Mezzanine.Work.WorkPlan"}
  }

  @context_key_map %{
    "action" => :action,
    "actor_ref" => :actor_ref,
    "causation_id" => :causation_id,
    "metadata" => :metadata,
    "reason" => :reason,
    "trace_id" => :trace_id
  }

  @workflow_required_fields [
    :kind,
    :action,
    :target_ref,
    :execution_id,
    :subject_id,
    :workflow_id,
    :signal_name,
    :signal_version,
    :signal_contract,
    :boundary,
    :idempotency_key,
    :trace_id,
    :causation_id,
    :actor_ref,
    :release_manifest_ref
  ]

  @local_required_fields [
    :kind,
    :action,
    :target_ref,
    :owner,
    :owner_module,
    :owner_action,
    :trace_id,
    :causation_id,
    :actor_ref,
    :release_manifest_ref
  ]

  @spec profile() :: map()
  def profile do
    %{
      release_manifest_ref: @release_manifest_ref,
      allowed_action_kinds: @allowed_kinds,
      forbidden_action_kinds: @forbidden_kinds,
      signal_contract: @signal_contract,
      signal_boundary: @signal_boundary,
      signal_registry: @signal_registry,
      local_mutation_owners: @local_owners,
      workflow_signal_required_fields: @workflow_required_fields,
      declared_local_mutation_required_fields: @local_required_fields,
      policy:
        "operator effects must be workflow_signal or declared_local_mutation; unclassified refs fail closed"
    }
  end

  @spec signal_registry() :: map()
  def signal_registry, do: @signal_registry

  @spec local_owners() :: map()
  def local_owners, do: @local_owners

  @spec workflow_signal(atom(), map(), map() | keyword()) :: {:ok, map()} | {:error, term()}
  def workflow_signal(action, execution, context) when is_atom(action) and is_map(execution) do
    case Map.fetch(@signal_registry, action) do
      {:ok, signal} ->
        context = normalize_context(context)
        execution_id = required_value(execution, :id)
        subject_id = required_value(execution, :subject_id)

        %{
          kind: :workflow_signal,
          action: action,
          target_ref: "workflow-signal://#{signal.signal_name}/#{execution_id}",
          execution_id: execution_id,
          subject_id: subject_id,
          workflow_id: workflow_id(execution),
          workflow_run_id: workflow_run_id(execution),
          dispatch_state: dispatch_state(execution),
          signal_name: signal.signal_name,
          signal_version: signal.signal_version,
          signal_contract: @signal_contract,
          boundary: @signal_boundary,
          idempotency_key: "operator-signal:#{action}:#{execution_id}",
          trace_id: Map.get(context, :trace_id),
          causation_id: Map.get(context, :causation_id),
          actor_ref: Map.get(context, :actor_ref),
          reason: Map.get(context, :reason),
          release_manifest_ref: @release_manifest_ref
        }
        |> validate()

      :error ->
        {:error, {:unsupported_operator_signal_action, action}}
    end
  end

  @spec declared_local_mutation(atom(), atom(), String.t(), map() | keyword()) ::
          {:ok, map()} | {:error, term()}
  def declared_local_mutation(owner_key, owner_action, target_ref, context)
      when is_atom(owner_key) and is_atom(owner_action) and is_binary(target_ref) do
    case Map.fetch(@local_owners, owner_key) do
      {:ok, owner} ->
        context = normalize_context(context)

        context
        |> Map.take([:trace_id, :causation_id, :actor_ref, :reason])
        |> Map.merge(%{
          kind: :declared_local_mutation,
          action: Map.get(context, :action, owner_action),
          target_ref: target_ref,
          owner: owner.owner,
          owner_module: owner.owner_module,
          owner_action: owner_action,
          release_manifest_ref: @release_manifest_ref
        })
        |> Map.merge(Map.get(context, :metadata, %{}))
        |> validate()

      :error ->
        {:error, {:undeclared_local_mutation_owner, owner_key}}
    end
  end

  @spec validate(map() | term()) :: {:ok, map()} | {:error, term()}
  def validate(%{kind: kind} = action) when kind in @allowed_kinds do
    case {kind, missing_required(action, required_fields(kind))} do
      {_kind, []} -> {:ok, action}
      {_kind, missing} -> {:error, {:missing_operator_action_fields, kind, missing}}
    end
  end

  def validate(%{kind: kind}) when kind in @forbidden_kinds,
    do: {:error, {:forbidden_operator_action_kind, kind}}

  def validate(%{kind: kind}), do: {:error, {:unsupported_operator_action_kind, kind}}
  def validate(ref) when is_binary(ref), do: {:error, {:unclassified_operator_action_ref, ref}}
  def validate(_other), do: {:error, :unclassified_operator_action}

  @spec action_ref(map()) :: String.t()
  def action_ref(%{target_ref: target_ref}) when is_binary(target_ref), do: target_ref

  @spec allowed_kind?(term()) :: boolean()
  def allowed_kind?(kind), do: kind in @allowed_kinds

  defp required_fields(:workflow_signal), do: @workflow_required_fields
  defp required_fields(:declared_local_mutation), do: @local_required_fields

  defp missing_required(action, fields) do
    Enum.reject(fields, fn field -> present?(Map.get(action, field)) end)
  end

  defp present?(value) when is_binary(value), do: String.trim(value) != ""
  defp present?(value) when is_map(value), do: map_size(value) > 0
  defp present?(value), do: not is_nil(value)

  defp workflow_id(execution) do
    Map.get(submission_ref(execution), "workflow_id") ||
      Map.get(lower_receipt(execution), "workflow_id") ||
      "tenant:#{required_value(execution, :tenant_id)}:execution:#{required_value(execution, :id)}:attempt:#{attempt(execution)}"
  end

  defp workflow_run_id(execution) do
    Map.get(submission_ref(execution), "workflow_run_id") ||
      Map.get(lower_receipt(execution), "workflow_run_id") ||
      Map.get(lower_receipt(execution), "run_id")
  end

  defp submission_ref(execution), do: map_field(execution, :submission_ref)
  defp lower_receipt(execution), do: map_field(execution, :lower_receipt)

  defp map_field(map, field) do
    case Map.get(map, field) do
      value when is_map(value) -> stringify_keys(value)
      _other -> %{}
    end
  end

  defp stringify_keys(map) do
    Map.new(map, fn {key, value} -> {to_string(key), value} end)
  end

  defp attempt(execution) do
    execution
    |> Map.get(:dispatch_attempt_count, 0)
    |> max(0)
    |> Kernel.+(1)
  end

  defp dispatch_state(execution) do
    execution
    |> Map.get(:dispatch_state)
    |> case do
      nil -> nil
      state -> to_string(state)
    end
  end

  defp required_value(map, field), do: Map.get(map, field) || Map.fetch!(map, field)

  defp normalize_context(context) when is_list(context),
    do: context |> Map.new() |> normalize_context()

  defp normalize_context(context) when is_map(context) do
    context
    |> Enum.map(fn {key, value} -> {normalize_key(key), value} end)
    |> Map.new()
    |> Map.put_new(:metadata, %{})
  end

  defp normalize_key(key) when is_atom(key), do: key
  defp normalize_key(key) when is_binary(key), do: Map.get(@context_key_map, key, key)
end
