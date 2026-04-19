defmodule Mezzanine.WorkflowRuntime.WorkflowFanoutFanin do
  @moduledoc """
  Child-workflow fan-out/fan-in contract for Phase 4 M30.

  The parent workflow owns branch identity, duplicate completion suppression,
  cancellation propagation, and operator-safe branch queries. Branch artifacts
  stay behind refs in fact stores or claim checks; this module carries only the
  compact routing and scope facts required for deterministic workflow decisions.
  """

  @contract_name "Mezzanine.WorkflowFanoutFanin.v1"
  @release_manifest_ref "phase4-v6-milestone30-workflow-fanout-fanin"
  @join_signal %{name: "child.completed", version: "child-completed.v1"}
  @branch_query %{name: "fanout.branch_state", version: "fanout-branch-state.v1"}
  @cancel_signal %{name: "operator.cancel.child", version: "operator-cancel-child.v1"}
  @terminal_statuses MapSet.new([:completed, :failed, :cancelled])

  @required_branch_fields [
    :tenant_ref,
    :resource_ref,
    :trace_id,
    :parent_workflow_ref,
    :child_workflow_ref,
    :idempotency_scope,
    :authority_context,
    :release_manifest_ref
  ]

  @required_parent_fields [
    :tenant_ref,
    :resource_ref,
    :trace_id,
    :parent_workflow_ref,
    :fanout_group_ref,
    :idempotency_scope,
    :authority_context,
    :release_manifest_ref,
    :branches
  ]

  @doc "M30 parent/child workflow fan-out/fan-in contract."
  @spec contract() :: map()
  def contract do
    %{
      contract_name: @contract_name,
      contract_version: "1.0.0",
      owner_repo: :mezzanine,
      boundary_owner: :parent_child_workflow_fanout_fanin,
      topology: :child_workflows,
      parent_workflow: Mezzanine.Workflows.JoinBarrier,
      join_signal: @join_signal,
      query: @branch_query,
      cancellation_signal: @cancel_signal,
      child_workflow_selection_rule: child_workflow_selection_rule(),
      required_branch_fields: @required_branch_fields,
      release_manifest_ref: @release_manifest_ref,
      history_policy: :refs_hashes_routing_facts_and_operator_projection_only
    }
  end

  @doc "Branching uses child workflows when each branch owns an independent durable lifecycle."
  @spec child_workflow_selection_rule() :: :independent_durable_branch_lifecycle
  def child_workflow_selection_rule, do: :independent_durable_branch_lifecycle

  @doc "Fields required on every child branch before fan-out is valid."
  @spec required_branch_fields() :: [atom()]
  def required_branch_fields, do: @required_branch_fields

  @doc "Build deterministic parent workflow fan-out state."
  @spec new_parent_state!(map() | keyword()) :: map()
  def new_parent_state!(attrs) do
    attrs = normalize(attrs)
    require_fields!(attrs, @required_parent_fields, :parent)

    branches =
      attrs.branches
      |> Enum.map(&normalize_branch!/1)
      |> Map.new(fn branch -> {branch.branch_ref, branch} end)

    if map_size(branches) != length(attrs.branches) do
      raise ArgumentError, "duplicate branch_ref values are not allowed"
    end

    %{
      contract_name: @contract_name,
      status: :waiting,
      parent_workflow_ref: attrs.parent_workflow_ref,
      fanout_group_ref: attrs.fanout_group_ref,
      tenant_ref: attrs.tenant_ref,
      resource_ref: attrs.resource_ref,
      trace_id: attrs.trace_id,
      idempotency_scope: attrs.idempotency_scope,
      authority_context: attrs.authority_context,
      release_manifest_ref: Map.get(attrs, :release_manifest_ref, @release_manifest_ref),
      branches: branches,
      completion_keys: MapSet.new(),
      close_count: 0,
      close_event_ref: nil,
      duplicate_completion_count: 0,
      failure_classes: MapSet.new()
    }
  end

  @doc "Apply a child completion signal exactly once."
  @spec apply_completion(map(), map() | keyword()) :: {:ok, map(), [map()]} | {:error, term()}
  def apply_completion(state, completion_attrs) when is_map(state) do
    completion = normalize(completion_attrs)

    with {:ok, branch} <- fetch_branch(state, completion),
         :ok <- require_completion_fields(completion) do
      apply_known_completion(state, branch, completion)
    end
  end

  @doc "Public-safe query projection for operator and incident surfaces."
  @spec operator_query(map()) :: map()
  def operator_query(state) when is_map(state) do
    %{
      contract_name: @contract_name,
      status: state.status,
      parent_workflow_ref: state.parent_workflow_ref,
      fanout_group_ref: state.fanout_group_ref,
      tenant_ref: state.tenant_ref,
      resource_ref: state.resource_ref,
      trace_id: state.trace_id,
      close_event_ref: state.close_event_ref,
      branches:
        state.branches
        |> Enum.map(fn {branch_ref, branch} -> {branch_ref, branch_projection(branch)} end)
        |> Map.new(),
      failure_summary: failure_summary(state),
      raw_payload?: false
    }
  end

  @doc "Build child cancellation signals for unfinished branches."
  @spec cancellation_propagation(map(), map() | keyword()) :: [map()]
  def cancellation_propagation(state, attrs) when is_map(state) do
    attrs = normalize(attrs)

    state.branches
    |> Enum.filter(fn {_branch_ref, branch} -> not terminal?(branch.status) end)
    |> Enum.map(fn {branch_ref, branch} ->
      %{
        signal_name: @cancel_signal.name,
        signal_version: @cancel_signal.version,
        branch_ref: branch_ref,
        child_workflow_ref: branch.child_workflow_ref,
        parent_workflow_ref: state.parent_workflow_ref,
        tenant_ref: branch.tenant_ref,
        resource_ref: branch.resource_ref,
        trace_id: branch.trace_id,
        authority_context: Map.get(branch, :authority_context, state.authority_context),
        reason: Map.get(attrs, :reason, "unspecified"),
        idempotency_key: "#{branch.idempotency_scope}:cancel"
      }
    end)
  end

  @doc "Operator-explainable failure aggregation by branch and failure class."
  @spec failure_summary(map()) :: map()
  def failure_summary(state) when is_map(state) do
    failed =
      state.branches
      |> Enum.filter(fn {_branch_ref, branch} -> branch.status == :failed end)
      |> Enum.sort_by(fn {branch_ref, _branch} -> branch_ref end)

    %{
      failed_count: length(failed),
      failed_branches: Enum.map(failed, fn {branch_ref, _branch} -> branch_ref end),
      failure_classes:
        failed
        |> Enum.map(fn {_branch_ref, branch} -> branch.failure_class end)
        |> Enum.reject(&is_nil/1)
        |> Enum.uniq()
    }
  end

  @doc "Run the join-barrier workflow contract against supplied completion signals."
  @spec run_join_barrier(map() | keyword()) :: {:ok, map()} | {:error, term()}
  def run_join_barrier(attrs) do
    state = new_parent_state!(attrs)

    with {:ok, state} <- apply_completions(state, Map.get(normalize(attrs), :completions, [])) do
      {:ok,
       state
       |> operator_query()
       |> Map.merge(%{
         close_count: state.close_count,
         duplicate_completion_count: state.duplicate_completion_count,
         close_event_ref: state.close_event_ref
       })}
    end
  rescue
    exception in ArgumentError ->
      {:error, {:invalid_fanout_fanin_input, Exception.message(exception)}}
  end

  defp apply_completions(state, completions) do
    Enum.reduce_while(completions, {:ok, state}, fn completion, {:ok, current_state} ->
      case apply_completion(current_state, completion) do
        {:ok, next_state, _events} -> {:cont, {:ok, next_state}}
        {:error, reason} -> {:halt, {:error, reason}}
      end
    end)
  end

  defp apply_known_completion(state, branch, completion) do
    completion_key = completion_key(completion)

    cond do
      MapSet.member?(state.completion_keys, completion_key) ->
        {:ok, increment_duplicate_count(state), [duplicate_suppressed_event(branch, completion)]}

      terminal?(branch.status) ->
        {:ok, increment_duplicate_count(state), [duplicate_suppressed_event(branch, completion)]}

      true ->
        state
        |> put_completed_branch(branch, completion, completion_key)
        |> maybe_close_fan_in(branch, completion)
    end
  end

  defp put_completed_branch(state, branch, completion, completion_key) do
    status = Map.get(completion, :status, :completed)

    completed_branch =
      branch
      |> Map.merge(%{
        status: status,
        completion_ref: completion.completion_ref,
        completion_idempotency_key: completion_key,
        result_ref: Map.get(completion, :result_ref),
        failure_class: Map.get(completion, :failure_class)
      })

    %{
      state
      | branches: Map.put(state.branches, branch.branch_ref, completed_branch),
        completion_keys: MapSet.put(state.completion_keys, completion_key),
        failure_classes: put_failure_class(state.failure_classes, completed_branch.failure_class)
    }
  end

  defp maybe_close_fan_in(state, branch, completion) do
    branch_event = %{
      event_type: :branch_completed,
      branch_ref: branch.branch_ref,
      completion_ref: completion.completion_ref
    }

    if all_branches_terminal?(state) and state.close_count == 0 do
      {:ok, close_state(state), [branch_event, fan_in_closed_event(state)]}
    else
      {:ok, state, [branch_event]}
    end
  end

  defp close_state(state) do
    %{state | status: :closed, close_count: 1, close_event_ref: close_event_ref(state)}
  end

  defp fan_in_closed_event(state) do
    %{
      event_type: :fan_in_closed,
      close_event_ref: close_event_ref(state),
      fanout_group_ref: state.fanout_group_ref
    }
  end

  defp duplicate_suppressed_event(branch, completion) do
    %{
      event_type: :duplicate_completion_suppressed,
      branch_ref: branch.branch_ref,
      completion_ref: Map.get(completion, :completion_ref)
    }
  end

  defp increment_duplicate_count(state) do
    %{state | duplicate_completion_count: state.duplicate_completion_count + 1}
  end

  defp close_event_ref(state), do: "fanout-group:#{state.fanout_group_ref}:close:1"

  defp all_branches_terminal?(state) do
    Enum.all?(state.branches, fn {_branch_ref, branch} -> terminal?(branch.status) end)
  end

  defp terminal?(status), do: MapSet.member?(@terminal_statuses, status)

  defp branch_projection(branch) do
    %{
      status: branch.status,
      child_workflow_ref: branch.child_workflow_ref,
      completion_ref: Map.get(branch, :completion_ref),
      failure_class: Map.get(branch, :failure_class),
      idempotency_scope: branch.idempotency_scope,
      trace_id: branch.trace_id
    }
  end

  defp normalize_branch!(attrs) do
    attrs = normalize(attrs)
    require_fields!(attrs, [:branch_ref | @required_branch_fields], :branch)

    Map.merge(attrs, %{
      status: Map.get(attrs, :status, :pending),
      completion_ref: Map.get(attrs, :completion_ref),
      completion_idempotency_key: Map.get(attrs, :completion_idempotency_key),
      result_ref: Map.get(attrs, :result_ref),
      failure_class: Map.get(attrs, :failure_class)
    })
  end

  defp fetch_branch(state, completion) do
    case Map.fetch(state.branches, Map.get(completion, :branch_ref)) do
      {:ok, branch} -> {:ok, branch}
      :error -> {:error, {:unknown_branch_ref, Map.get(completion, :branch_ref)}}
    end
  end

  defp require_completion_fields(completion) do
    case missing(completion, [:branch_ref, :completion_ref, :completion_idempotency_key]) do
      [] -> :ok
      fields -> {:error, {:missing_completion_fields, fields}}
    end
  end

  defp require_fields!(attrs, fields, scope) do
    case missing(attrs, fields) do
      [] ->
        :ok

      missing_fields ->
        raise ArgumentError, "#{scope} missing required fields: #{inspect(missing_fields)}"
    end
  end

  defp missing(attrs, fields) do
    Enum.reject(fields, fn field ->
      Map.has_key?(attrs, field) and not is_nil(Map.get(attrs, field))
    end)
  end

  defp completion_key(completion) do
    Map.get(completion, :completion_idempotency_key, Map.fetch!(completion, :completion_ref))
  end

  defp put_failure_class(classes, nil), do: classes
  defp put_failure_class(classes, failure_class), do: MapSet.put(classes, failure_class)

  defp normalize(attrs) when is_map(attrs), do: attrs
  defp normalize(attrs) when is_list(attrs), do: Map.new(attrs)
end
