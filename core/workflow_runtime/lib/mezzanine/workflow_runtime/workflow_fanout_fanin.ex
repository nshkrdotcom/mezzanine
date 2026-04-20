defmodule Mezzanine.WorkflowRuntime.WorkflowFanoutFanin do
  @moduledoc """
  Child-workflow fan-out/fan-in contract for Phase 4 M30.

  The parent workflow owns branch identity, duplicate completion suppression,
  cancellation propagation, close-policy evaluation, and operator-safe branch
  queries. Branch artifacts stay behind refs in fact stores or claim checks;
  this module carries only the compact routing and scope facts required for
  deterministic workflow decisions.
  """

  @contract_name "Mezzanine.WorkflowFanoutFanin.v1"
  @release_manifest_ref "phase4-v6-milestone30-workflow-fanout-fanin"
  @join_signal %{name: "child.completed", version: "child-completed.v1"}
  @branch_query %{name: "fanout.branch_state", version: "fanout-branch-state.v1"}
  @cancel_signal %{name: "operator.cancel.child", version: "operator-cancel-child.v1"}
  @terminal_statuses MapSet.new([:completed, :failed, :cancelled])
  @join_policies [
    :all_required,
    :k_of_n,
    :at_least_one,
    :best_effort_with_required,
    :fail_fast
  ]
  @policy_fields [
    :fanout_group_ref,
    :parent_workflow_ref,
    :workflow_version,
    :join_policy,
    :required_success_count,
    :required_branch_refs,
    :optional_branch_refs,
    :timeout_policy,
    :late_completion_policy,
    :heterogeneous_failure_actions,
    :close_decision,
    :quorum_result,
    :branch_counts,
    :failure_classes,
    :compensation_refs,
    :close_event_ref,
    :release_manifest_ref
  ]

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
      policy_fields: @policy_fields,
      supported_join_policies: @join_policies,
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

  @doc "Supported parent close policies for partial-failure fan-in."
  @spec supported_join_policies() :: [atom()]
  def supported_join_policies, do: @join_policies

  @doc "Fields every fan-out/fan-in policy profile must expose to release evidence."
  @spec policy_fields() :: [atom()]
  def policy_fields, do: @policy_fields

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

    policy = close_policy!(attrs, branches)

    %{
      contract_name: @contract_name,
      status: :waiting,
      parent_workflow_ref: attrs.parent_workflow_ref,
      fanout_group_ref: attrs.fanout_group_ref,
      workflow_version: workflow_version(attrs.parent_workflow_ref),
      tenant_ref: attrs.tenant_ref,
      resource_ref: attrs.resource_ref,
      trace_id: attrs.trace_id,
      idempotency_scope: attrs.idempotency_scope,
      authority_context: attrs.authority_context,
      release_manifest_ref: Map.get(attrs, :release_manifest_ref, @release_manifest_ref),
      join_policy: policy.join_policy,
      required_success_count: policy.required_success_count,
      required_branch_refs: policy.required_branch_refs,
      optional_branch_refs: policy.optional_branch_refs,
      timeout_policy: policy.timeout_policy,
      late_completion_policy: policy.late_completion_policy,
      heterogeneous_failure_actions: policy.heterogeneous_failure_actions,
      branches: branches,
      completion_keys: MapSet.new(),
      close_count: 0,
      close_decision: nil,
      close_event_ref: nil,
      quorum_result: nil,
      duplicate_completion_count: 0,
      late_completion_count: 0,
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
      close_decision: state.close_decision,
      quorum_result: state.quorum_result,
      join_policy: state.join_policy,
      required_success_count: state.required_success_count,
      required_branch_refs: state.required_branch_refs,
      optional_branch_refs: state.optional_branch_refs,
      branch_counts: branch_counts(state),
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
        |> Enum.uniq(),
      failure_class_counts: failure_class_counts(failed),
      failures:
        Enum.map(failed, fn {branch_ref, branch} ->
          %{
            branch_ref: branch_ref,
            failure_class: branch.failure_class,
            count: 1,
            safe_action: failure_safe_action(branch),
            compensation_ref: Map.get(branch, :compensation_ref)
          }
        end),
      compensation_refs:
        failed
        |> Enum.map(fn {_branch_ref, branch} -> Map.get(branch, :compensation_ref) end)
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
         late_completion_count: state.late_completion_count,
         close_decision: state.close_decision,
         quorum_result: state.quorum_result,
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

      closed?(state) ->
        {:ok, increment_late_count(state),
         [late_completion_evidence_event(state, branch, completion)]}

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
        failure_class: Map.get(completion, :failure_class),
        safe_action: Map.get(completion, :safe_action),
        compensation_ref: Map.get(completion, :compensation_ref)
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
      completion_ref: completion.completion_ref,
      status: Map.get(completion, :status, :completed),
      failure_class: Map.get(completion, :failure_class)
    }

    case close_decision(state) do
      {:close, decision, quorum_result} when state.close_count == 0 ->
        closed_state = close_state(state, decision, quorum_result)
        {:ok, closed_state, [branch_event, fan_in_closed_event(closed_state)]}

      _other ->
        {:ok, state, [branch_event]}
    end
  end

  defp close_decision(state) do
    counts = branch_counts(state)

    case state.join_policy do
      :all_required -> close_when_all_required(state, counts)
      :k_of_n -> close_when_k_of_n(state, counts)
      :at_least_one -> close_when_at_least_one(counts)
      :best_effort_with_required -> close_when_best_effort_with_required(state, counts)
      :fail_fast -> close_when_fail_fast(state, counts)
    end
  end

  defp close_when_all_required(state, counts) do
    if counts.terminal == counts.total do
      {:close, terminal_decision(counts), quorum_result(state, counts)}
    else
      :wait
    end
  end

  defp close_when_k_of_n(state, counts) do
    cond do
      counts.completed >= state.required_success_count ->
        {:close, :succeeded, quorum_result(state, counts)}

      counts.completed + counts.pending < state.required_success_count ->
        {:close, :failed, quorum_result(state, counts)}

      true ->
        :wait
    end
  end

  defp close_when_at_least_one(counts) do
    cond do
      counts.completed >= 1 ->
        {:close, :succeeded, %{mode: :at_least_one, met?: true, required_success_count: 1}}

      counts.pending == 0 ->
        {:close, :failed, %{mode: :at_least_one, met?: false, required_success_count: 1}}

      true ->
        :wait
    end
  end

  defp close_when_best_effort_with_required(state, counts) do
    required = required_branches(state)

    cond do
      Enum.any?(required, &(&1.status != :completed and terminal?(&1.status))) ->
        {:close, :failed, quorum_result(state, counts)}

      Enum.all?(required, &(&1.status == :completed)) ->
        decision = if counts.completed == counts.total, do: :succeeded, else: :partial_success
        {:close, decision, quorum_result(state, counts)}

      true ->
        :wait
    end
  end

  defp close_when_fail_fast(state, counts) do
    cond do
      counts.failed + counts.cancelled > 0 ->
        {:close, :failed, quorum_result(state, counts)}

      counts.terminal == counts.total ->
        {:close, :succeeded, quorum_result(state, counts)}

      true ->
        :wait
    end
  end

  defp terminal_decision(%{failed: failed, cancelled: cancelled}) when failed + cancelled > 0,
    do: :failed

  defp terminal_decision(_counts), do: :succeeded

  defp quorum_result(state, counts) do
    %{
      mode: state.join_policy,
      met?: counts.completed >= state.required_success_count,
      required_success_count: state.required_success_count,
      completed_success_count: counts.completed,
      total_branch_count: counts.total,
      required_branch_refs: state.required_branch_refs,
      optional_branch_refs: state.optional_branch_refs
    }
  end

  defp required_branches(state) do
    state.required_branch_refs
    |> Enum.map(&Map.fetch!(state.branches, &1))
  end

  defp close_state(state, decision, quorum_result) do
    %{
      state
      | status: :closed,
        close_count: 1,
        close_decision: decision,
        close_event_ref: close_event_ref(state),
        quorum_result: quorum_result
    }
  end

  defp fan_in_closed_event(state) do
    failure_summary = failure_summary(state)

    %{
      event_type: :fan_in_closed,
      close_event_ref: state.close_event_ref,
      fanout_group_ref: state.fanout_group_ref,
      close_decision: state.close_decision,
      quorum_result: state.quorum_result,
      branch_counts: branch_counts(state),
      failure_summary: failure_summary,
      compensation_refs: failure_summary.compensation_refs,
      release_manifest_ref: state.release_manifest_ref
    }
  end

  defp late_completion_evidence_event(state, branch, completion) do
    %{
      event_type: :late_completion_evidence,
      branch_ref: branch.branch_ref,
      completion_ref: Map.get(completion, :completion_ref),
      completion_idempotency_key: completion_key(completion),
      attempted_status: Map.get(completion, :status, :completed),
      failure_class: Map.get(completion, :failure_class),
      safe_action: Map.get(completion, :safe_action),
      compensation_ref: Map.get(completion, :compensation_ref),
      close_event_ref: state.close_event_ref,
      close_decision: state.close_decision,
      close_count: state.close_count,
      release_manifest_ref: state.release_manifest_ref
    }
  end

  defp duplicate_suppressed_event(branch, completion) do
    %{
      event_type: :duplicate_completion_suppressed,
      branch_ref: branch.branch_ref,
      completion_ref: Map.get(completion, :completion_ref),
      close_count_unchanged?: true
    }
  end

  defp increment_duplicate_count(state) do
    %{state | duplicate_completion_count: state.duplicate_completion_count + 1}
  end

  defp increment_late_count(state) do
    %{state | late_completion_count: state.late_completion_count + 1}
  end

  defp close_event_ref(state), do: "fanout-group:#{state.fanout_group_ref}:close:1"

  defp branch_counts(state) do
    statuses =
      state.branches
      |> Map.values()
      |> Enum.map(& &1.status)

    %{
      total: length(statuses),
      completed: Enum.count(statuses, &(&1 == :completed)),
      failed: Enum.count(statuses, &(&1 == :failed)),
      cancelled: Enum.count(statuses, &(&1 == :cancelled)),
      pending: Enum.count(statuses, &(&1 == :pending)),
      terminal: Enum.count(statuses, &terminal?/1)
    }
  end

  defp failure_class_counts(failed) do
    failed
    |> Enum.map(fn {_branch_ref, branch} -> branch.failure_class || :unclassified_failure end)
    |> Enum.frequencies()
  end

  defp failure_safe_action(branch) do
    Map.get(branch, :safe_action) ||
      Map.get(failure_action_defaults(), branch.failure_class) ||
      :operator_review_required
  end

  defp failure_action_defaults do
    %{
      child_workflow_failed: :operator_review_required,
      child_workflow_timeout: :retry_or_cancel_branch,
      cancelled: :respect_cancellation
    }
  end

  defp close_policy!(attrs, branches) do
    branch_refs = Map.keys(branches) |> Enum.sort()
    join_policy = normalize_join_policy(Map.get(attrs, :join_policy, :all_required))
    required_branch_refs = required_branch_refs!(attrs, branch_refs, join_policy)
    optional_branch_refs = branch_refs -- required_branch_refs

    required_success_count =
      required_success_count!(attrs, join_policy, branch_refs, required_branch_refs)

    %{
      join_policy: join_policy,
      required_success_count: required_success_count,
      required_branch_refs: required_branch_refs,
      optional_branch_refs: optional_branch_refs,
      timeout_policy: Map.get(attrs, :timeout_policy, %{mode: :parent_workflow_timer}),
      late_completion_policy: Map.get(attrs, :late_completion_policy, :evidence_only_after_close),
      heterogeneous_failure_actions: Map.get(attrs, :heterogeneous_failure_actions, %{})
    }
  end

  defp normalize_join_policy(policy) when policy in @join_policies, do: policy

  defp normalize_join_policy(policy) when is_binary(policy) do
    Enum.find(@join_policies, &(Atom.to_string(&1) == policy)) ||
      raise ArgumentError, "unsupported join_policy: #{inspect(policy)}"
  end

  defp normalize_join_policy(policy),
    do: raise(ArgumentError, "unsupported join_policy: #{inspect(policy)}")

  defp required_branch_refs!(attrs, branch_refs, :best_effort_with_required) do
    attrs
    |> Map.get(:required_branch_refs, branch_refs)
    |> normalize_branch_ref_list!(branch_refs, :required_branch_refs)
  end

  defp required_branch_refs!(_attrs, branch_refs, _join_policy), do: branch_refs

  defp required_success_count!(attrs, :k_of_n, branch_refs, _required_branch_refs) do
    attrs
    |> Map.get(:required_success_count)
    |> case do
      count when is_integer(count) and count > 0 and count <= length(branch_refs) ->
        count

      other ->
        raise ArgumentError,
              "k_of_n required_success_count must be between 1 and branch count, got: #{inspect(other)}"
    end
  end

  defp required_success_count!(_attrs, :at_least_one, _branch_refs, _required_branch_refs),
    do: 1

  defp required_success_count!(
         _attrs,
         :best_effort_with_required,
         _branch_refs,
         required_branch_refs
       ),
       do: length(required_branch_refs)

  defp required_success_count!(_attrs, _join_policy, branch_refs, _required_branch_refs),
    do: length(branch_refs)

  defp normalize_branch_ref_list!(refs, branch_refs, field) when is_list(refs) do
    refs = Enum.map(refs, &to_string/1)
    unknown = refs -- branch_refs

    cond do
      refs == [] ->
        raise ArgumentError, "#{field} cannot be empty"

      unknown != [] ->
        raise ArgumentError, "#{field} has unknown branch refs: #{inspect(unknown)}"

      true ->
        Enum.sort(refs)
    end
  end

  defp normalize_branch_ref_list!(refs, _branch_refs, field),
    do: raise(ArgumentError, "#{field} must be a list, got: #{inspect(refs)}")

  defp workflow_version(parent_workflow_ref) when is_map(parent_workflow_ref) do
    Map.get(parent_workflow_ref, :workflow_version) ||
      Map.get(parent_workflow_ref, "workflow_version")
  end

  defp workflow_version(_parent_workflow_ref), do: nil

  defp closed?(state), do: state.close_count > 0

  defp terminal?(status), do: MapSet.member?(@terminal_statuses, status)

  defp branch_projection(branch) do
    %{
      status: branch.status,
      child_workflow_ref: branch.child_workflow_ref,
      completion_ref: Map.get(branch, :completion_ref),
      failure_class: Map.get(branch, :failure_class),
      safe_action: Map.get(branch, :safe_action),
      compensation_ref: Map.get(branch, :compensation_ref),
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
      failure_class: Map.get(attrs, :failure_class),
      safe_action: Map.get(attrs, :safe_action),
      compensation_ref: Map.get(attrs, :compensation_ref)
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
