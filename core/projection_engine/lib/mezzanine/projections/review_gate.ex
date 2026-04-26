defmodule Mezzanine.Projections.ReviewGate do
  @moduledoc """
  Resolves operator review decisions into subject state and read projections.

  The gate consumes durable decision ids and policy carried by workflow/product
  state. It does not use process environment or static provider object ids.
  """

  alias Mezzanine.Audit.AuditAppend
  alias Mezzanine.DecisionCommands
  alias Mezzanine.Decisions.DecisionRecord
  alias Mezzanine.Objects.SubjectRecord
  alias Mezzanine.Projections.ProjectionRow

  @actions [:accept, :reject, :waive, :expire, :escalate]
  @runtime_projection "review_gate_runtime"

  @spec resolve(map() | keyword()) :: {:ok, map()} | {:error, term()}
  def resolve(attrs) when is_map(attrs) or is_list(attrs) do
    attrs = normalize_attrs(attrs)

    with {:ok, decision} <- fetch_decision(required!(attrs, :decision_id)),
         action <- normalize_action!(required!(attrs, :decision_action)),
         {:ok, resolved_decision} <- resolve_decision(decision, action, attrs),
         {:ok, subject} <- fetch_subject(resolved_decision.subject_id),
         {:ok, subject} <- apply_subject_outcome(subject, action, attrs),
         {:ok, projection} <- upsert_review_projection(subject, resolved_decision, action, attrs),
         {:ok, audit} <- append_review_audit(subject, resolved_decision, action, attrs) do
      {:ok,
       %{
         decision: resolved_decision,
         subject: subject,
         projection: projection,
         audit: audit
       }}
    end
  end

  defp resolve_decision(%DecisionRecord{} = decision, action, attrs) do
    case DecisionCommands.resolve_terminal(decision, action, decision_attrs(attrs, action)) do
      {:ok, %DecisionRecord{} = resolved_decision} ->
        {:ok, resolved_decision}

      {:error,
       {:decision_terminal_resolution_failed, {:decision_not_pending, _state},
        :duplicate_same_decision}} ->
        fetch_decision(decision.id)

      {:error, reason} ->
        {:error, reason}
    end
  end

  defp decision_attrs(attrs, action) do
    attrs
    |> Map.take([
      :reason,
      :trace_id,
      :causation_id,
      :actor_ref,
      :expected_row_version,
      :attempt_id,
      :idempotency_key,
      :attempted_at
    ])
    |> maybe_put_required_escalation_reason(attrs, action)
  end

  defp maybe_put_required_escalation_reason(decision_attrs, attrs, :escalate) do
    Map.put_new(decision_attrs, :reason, value(attrs, :reason) || "review escalated")
  end

  defp maybe_put_required_escalation_reason(decision_attrs, _attrs, _action), do: decision_attrs

  defp apply_subject_outcome(%SubjectRecord{} = subject, action, attrs) do
    policy = policy(attrs)

    case action do
      :accept ->
        advance_subject(
          subject,
          attrs,
          policy_value(policy, :accept_lifecycle_state, "completed")
        )

      :waive ->
        advance_subject(subject, attrs, policy_value(policy, :waive_lifecycle_state, "completed"))

      :reject ->
        subject
        |> block_subject(attrs, policy_value(policy, :rework_reason, "review_rejected"))
        |> then(fn
          {:ok, blocked_subject} ->
            advance_subject(
              blocked_subject,
              attrs,
              policy_value(policy, :rework_lifecycle_state, "rework_requested")
            )

          {:error, reason} ->
            {:error, reason}
        end)

      :expire ->
        subject
        |> block_subject(attrs, policy_value(policy, :expire_reason, "review_expired"))
        |> then(fn
          {:ok, blocked_subject} ->
            advance_subject(
              blocked_subject,
              attrs,
              policy_value(policy, :expire_lifecycle_state, "review_expired")
            )

          {:error, reason} ->
            {:error, reason}
        end)

      :escalate ->
        advance_subject(
          subject,
          attrs,
          policy_value(policy, :escalate_lifecycle_state, "awaiting_review")
        )
    end
  end

  defp block_subject(%SubjectRecord{} = subject, attrs, block_reason) do
    if subject.block_reason == block_reason do
      {:ok, subject}
    else
      SubjectRecord.block(subject, %{
        block_reason: block_reason,
        trace_id: required!(attrs, :trace_id),
        causation_id: required!(attrs, :causation_id),
        actor_ref: required!(attrs, :actor_ref)
      })
    end
  end

  defp advance_subject(%SubjectRecord{} = subject, attrs, lifecycle_state) do
    if subject.lifecycle_state == lifecycle_state do
      {:ok, subject}
    else
      SubjectRecord.advance_lifecycle(subject, %{
        lifecycle_state: lifecycle_state,
        trace_id: required!(attrs, :trace_id),
        causation_id: required!(attrs, :causation_id),
        actor_ref: required!(attrs, :actor_ref)
      })
    end
  end

  defp upsert_review_projection(subject, decision, action, attrs) do
    ProjectionRow.upsert(%{
      installation_id: decision.installation_id,
      projection_name: projection_name(action),
      row_key: projection_row_key(subject, decision, action),
      subject_id: subject.id,
      execution_id: decision.execution_id,
      projection_kind: projection_kind(action),
      sort_key: sort_key(action),
      trace_id: required!(attrs, :trace_id),
      causation_id: required!(attrs, :causation_id),
      payload: projection_payload(subject, decision, action, attrs),
      computed_at: DateTime.utc_now()
    })
  end

  defp projection_payload(subject, decision, action, attrs) do
    policy = policy(attrs)

    %{
      subject: %{
        subject_id: subject.id,
        lifecycle_state: subject.lifecycle_state,
        block_reason: subject.block_reason,
        status: subject.status
      },
      decision: %{
        decision_id: decision.id,
        decision_kind: decision.decision_kind,
        lifecycle_state: decision.lifecycle_state,
        decision_value: decision.decision_value,
        reason: decision.reason
      },
      decision_action: Atom.to_string(action),
      safe_action: safe_action(action),
      reason: action_reason(action, attrs, policy),
      rework_recipe_ref: policy_value(policy, :rework_recipe_ref, nil),
      escalation_owner_ref: policy_value(policy, :escalation_owner_ref, nil),
      policy_ref: policy_value(policy, :policy_ref, nil)
    }
    |> reject_nil_values()
  end

  defp append_review_audit(subject, decision, action, attrs) do
    AuditAppend.append_fact(%{
      installation_id: decision.installation_id,
      subject_id: subject.id,
      execution_id: decision.execution_id,
      decision_id: decision.id,
      trace_id: required!(attrs, :trace_id),
      causation_id: required!(attrs, :causation_id),
      fact_kind: :review_gate_resolved,
      actor_ref: required!(attrs, :actor_ref),
      payload: %{
        decision_action: Atom.to_string(action),
        decision_value: decision.decision_value,
        decision_lifecycle_state: decision.lifecycle_state,
        subject_lifecycle_state: subject.lifecycle_state,
        projection_name: projection_name(action),
        safe_action: safe_action(action)
      },
      occurred_at: DateTime.utc_now()
    })
  end

  defp projection_name(action) when action in [:accept, :waive], do: @runtime_projection
  defp projection_name(:reject), do: "review_rework_queue"
  defp projection_name(:expire), do: "review_expiry_queue"
  defp projection_name(:escalate), do: "review_escalation_queue"

  defp projection_row_key(_subject, decision, action) when action in [:accept, :waive],
    do: decision.id

  defp projection_row_key(subject, _decision, _action), do: subject.id

  defp projection_kind(action) when action in [:accept, :waive], do: "review_runtime"
  defp projection_kind(:reject), do: "review_rework"
  defp projection_kind(:expire), do: "review_expiry"
  defp projection_kind(:escalate), do: "review_escalation"

  defp safe_action(:accept), do: "complete_subject"
  defp safe_action(:waive), do: "complete_subject_by_waiver"
  defp safe_action(:reject), do: "operator.rework"
  defp safe_action(:expire), do: "review_expired"
  defp safe_action(:escalate), do: "route_to_escalation_owner"

  defp sort_key(:escalate), do: 0
  defp sort_key(:reject), do: 10
  defp sort_key(:expire), do: 20
  defp sort_key(:accept), do: 50
  defp sort_key(:waive), do: 60

  defp action_reason(:reject, attrs, policy),
    do: policy_value(policy, :rework_reason, value(attrs, :reason) || "review_rejected")

  defp action_reason(:expire, attrs, policy),
    do: policy_value(policy, :expire_reason, value(attrs, :reason) || "review_expired")

  defp action_reason(_action, attrs, _policy), do: value(attrs, :reason)

  defp normalize_action!(action) when is_atom(action) and action in @actions, do: action

  defp normalize_action!(action) when is_binary(action) do
    case String.downcase(String.trim(action)) do
      "accept" -> :accept
      "reject" -> :reject
      "waive" -> :waive
      "expire" -> :expire
      "expired" -> :expire
      "escalate" -> :escalate
      other -> raise ArgumentError, "unsupported review gate action #{inspect(other)}"
    end
  end

  defp normalize_action!(action),
    do: raise(ArgumentError, "unsupported review gate action #{inspect(action)}")

  defp fetch_decision(decision_id) do
    case Ash.get(DecisionRecord, decision_id) do
      {:ok, %DecisionRecord{} = decision} -> {:ok, decision}
      {:ok, nil} -> {:error, :decision_not_found}
      {:error, reason} -> {:error, reason}
    end
  end

  defp fetch_subject(subject_id) do
    case Ash.get(SubjectRecord, subject_id) do
      {:ok, %SubjectRecord{} = subject} -> {:ok, subject}
      {:ok, nil} -> {:error, :subject_not_found}
      {:error, reason} -> {:error, reason}
    end
  end

  defp policy(attrs) do
    case value(attrs, :review_policy) do
      %{} = policy -> normalize_attrs(policy)
      _other -> %{}
    end
  end

  defp policy_value(policy, key, default) do
    case value(policy, key) do
      nil -> default
      value -> value
    end
  end

  defp reject_nil_values(map), do: Map.reject(map, fn {_key, value} -> is_nil(value) end)

  defp normalize_attrs(attrs) when is_list(attrs), do: Map.new(attrs)
  defp normalize_attrs(%{} = attrs), do: Map.new(attrs)

  defp required!(attrs, key) do
    case value(attrs, key) do
      nil -> raise ArgumentError, "missing required review gate field #{inspect(key)}"
      value -> value
    end
  end

  defp value(attrs, key) when is_map(attrs) do
    case Map.fetch(attrs, key) do
      {:ok, value} -> value
      :error -> Map.get(attrs, Atom.to_string(key))
    end
  end
end
