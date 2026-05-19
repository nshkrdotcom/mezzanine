defmodule Mezzanine.Pack.Canonicalizer do
  @moduledoc false

  alias Mezzanine.Pack.LifecycleSpec

  @runtime_classes [:session, :workflow, :playbook, :scan, :inference]
  @failure_kinds [
    :transient_failure,
    :timeout,
    :infrastructure_error,
    :auth_error,
    :semantic_failure,
    :fatal_error
  ]

  @decision_values [:accept, :reject, :waive, :expired, :escalate]
  @context_usage_phases [:preprocess, :retrieval, :repair]
  @context_merge_strategies [:append, :ranked_append, :replace_slot]
  @source_publish_operations [
    :update_state,
    :create_comment,
    :update_comment,
    :add_label,
    :remove_label
  ]
  @source_publish_idempotency_scopes [:subject, :execution, :source_event]

  def runtime_class?(value), do: value in @runtime_classes
  def failure_kind?(value), do: value in @failure_kinds
  def decision_value?(value), do: value in @decision_values
  def context_usage_phase?(value), do: value in @context_usage_phases
  def context_merge_strategy?(value), do: value in @context_merge_strategies
  def source_publish_operation?(value), do: value in @source_publish_operations
  def source_publish_idempotency_scope?(value), do: value in @source_publish_idempotency_scopes

  def transition_field(transition, key) do
    Map.get(transition, key) || Map.get(transition, Atom.to_string(key))
  end

  def canonicalize_identifier(nil), do: {:error, "is required"}
  def canonicalize_identifier(value) when is_atom(value), do: {:ok, Atom.to_string(value)}

  def canonicalize_identifier(value) when is_binary(value) and byte_size(value) > 0,
    do: {:ok, value}

  def canonicalize_identifier(value) when is_binary(value), do: {:error, "must not be empty"}

  def canonicalize_identifier(value) do
    {:error, "must be an atom or string, got: #{inspect(value)}"}
  end

  def canonicalize_identifier!(value) do
    case canonicalize_identifier(value) do
      {:ok, identifier} -> identifier
      {:error, message} -> raise ArgumentError, message
    end
  end

  def canonicalize_trigger(:auto), do: {:ok, :auto}

  def canonicalize_trigger({:execution_requested, recipe_ref}) do
    with {:ok, recipe_ref} <- canonicalize_identifier(recipe_ref) do
      {:ok, {:execution_requested, recipe_ref}}
    end
  end

  def canonicalize_trigger({:execution_completed, recipe_ref}) do
    with {:ok, recipe_ref} <- canonicalize_identifier(recipe_ref) do
      {:ok, {:execution_completed, recipe_ref}}
    end
  end

  def canonicalize_trigger({:execution_failed, recipe_ref}) do
    with {:ok, recipe_ref} <- canonicalize_identifier(recipe_ref) do
      {:ok, {:execution_failed, recipe_ref}}
    end
  end

  def canonicalize_trigger({:execution_failed, recipe_ref, failure_kind}) do
    with {:ok, recipe_ref} <- canonicalize_identifier(recipe_ref) do
      {:ok, {:execution_failed, recipe_ref, failure_kind}}
    end
  end

  def canonicalize_trigger({:join_completed, join_step_ref}) do
    with {:ok, join_step_ref} <- canonicalize_identifier(join_step_ref) do
      {:ok, {:join_completed, join_step_ref}}
    end
  end

  def canonicalize_trigger({:decision_made, decision_kind, decision_value}) do
    with {:ok, decision_kind} <- canonicalize_identifier(decision_kind) do
      {:ok, {:decision_made, decision_kind, decision_value}}
    end
  end

  def canonicalize_trigger({:operator_action, action_kind}) do
    with {:ok, action_kind} <- canonicalize_identifier(action_kind) do
      {:ok, {:operator_action, action_kind}}
    end
  end

  def canonicalize_trigger({:subject_entered_state, state}) do
    with {:ok, state} <- canonicalize_identifier(state) do
      {:ok, {:subject_entered_state, state}}
    end
  end

  def canonicalize_trigger(other), do: {:error, "has unsupported trigger #{inspect(other)}"}

  def canonicalize_trigger!(trigger) do
    case canonicalize_trigger(trigger) do
      {:ok, canonical} -> canonical
      {:error, message} -> raise ArgumentError, message
    end
  end

  def canonicalize_decision_trigger({:after_execution_completed, recipe_ref}) do
    with {:ok, recipe_ref} <- canonicalize_identifier(recipe_ref) do
      {:ok, {:after_execution_completed, recipe_ref}}
    end
  end

  def canonicalize_decision_trigger({:after_decision, decision_kind, decision_value}) do
    with {:ok, decision_kind} <- canonicalize_identifier(decision_kind) do
      {:ok, {:after_decision, decision_kind, decision_value}}
    end
  end

  def canonicalize_decision_trigger({:on_subject_entered_state, state}) do
    with {:ok, state} <- canonicalize_identifier(state) do
      {:ok, {:on_subject_entered_state, state}}
    end
  end

  def canonicalize_decision_trigger(other),
    do: {:error, "has unsupported decision trigger #{inspect(other)}"}

  def canonicalize_decision_trigger!(trigger) do
    case canonicalize_decision_trigger(trigger) do
      {:ok, canonical} -> canonical
      {:error, message} -> raise ArgumentError, message
    end
  end

  def canonicalize_evidence_trigger({:execution_completed, recipe_ref}) do
    with {:ok, recipe_ref} <- canonicalize_identifier(recipe_ref) do
      {:ok, {:execution_completed, recipe_ref}}
    end
  end

  def canonicalize_evidence_trigger({:decision_created, decision_kind}) do
    with {:ok, decision_kind} <- canonicalize_identifier(decision_kind) do
      {:ok, {:decision_created, decision_kind}}
    end
  end

  def canonicalize_evidence_trigger({:subject_entered_state, state}) do
    with {:ok, state} <- canonicalize_identifier(state) do
      {:ok, {:subject_entered_state, state}}
    end
  end

  def canonicalize_evidence_trigger(other),
    do: {:error, "has unsupported evidence trigger #{inspect(other)}"}

  def canonicalize_evidence_trigger!(trigger) do
    case canonicalize_evidence_trigger(trigger) do
      {:ok, canonical} -> canonical
      {:error, message} -> raise ArgumentError, message
    end
  end

  def canonicalize_source_publish_trigger({:subject_entered_state, state}) do
    with {:ok, state} <- canonicalize_identifier(state) do
      {:ok, {:subject_entered_state, state}}
    end
  end

  def canonicalize_source_publish_trigger({:execution_completed, recipe_ref}) do
    with {:ok, recipe_ref} <- canonicalize_identifier(recipe_ref) do
      {:ok, {:execution_completed, recipe_ref}}
    end
  end

  def canonicalize_source_publish_trigger({:decision_made, decision_kind, decision_value}) do
    with {:ok, decision_kind} <- canonicalize_identifier(decision_kind) do
      {:ok, {:decision_made, decision_kind, decision_value}}
    end
  end

  def canonicalize_source_publish_trigger({:operator_action, action_kind}) do
    with {:ok, action_kind} <- canonicalize_identifier(action_kind) do
      {:ok, {:operator_action, action_kind}}
    end
  end

  def canonicalize_source_publish_trigger(other),
    do: {:error, "has unsupported source publish trigger #{inspect(other)}"}

  def canonicalize_source_publish_trigger!(trigger) do
    case canonicalize_source_publish_trigger(trigger) do
      {:ok, canonical} -> canonical
      {:error, message} -> raise ArgumentError, message
    end
  end

  def canonicalize_effect({:advance_lifecycle, state}) do
    with {:ok, state} <- canonicalize_identifier(state) do
      {:ok, {:advance_lifecycle, state}}
    end
  end

  def canonicalize_effect({:dispatch_effect, effect_kind}) do
    with {:ok, effect_kind} <- canonicalize_identifier(effect_kind) do
      {:ok, {:dispatch_effect, effect_kind}}
    end
  end

  def canonicalize_effect({:collect_evidence, evidence_kind}) do
    with {:ok, evidence_kind} <- canonicalize_identifier(evidence_kind) do
      {:ok, {:collect_evidence, evidence_kind}}
    end
  end

  def canonicalize_effect(effect)
      when effect in [
             :block_subject,
             :unblock_subject,
             :pause_execution,
             :resume_execution,
             :retry_execution,
             :cancel_active_execution
           ],
      do: {:ok, effect}

  def canonicalize_effect(other),
    do: {:error, "has unsupported operator effect #{inspect(other)}"}

  def canonicalize_effect!(effect) do
    case canonicalize_effect(effect) do
      {:ok, canonical} -> canonical
      {:error, message} -> raise ArgumentError, message
    end
  end

  def lifecycle_states(%LifecycleSpec{} = lifecycle) do
    values =
      [lifecycle.initial_state | lifecycle.terminal_states] ++
        Enum.flat_map(lifecycle.transitions, fn transition ->
          extra_state =
            case transition_field(transition, :trigger) do
              {:subject_entered_state, state} -> [state]
              _ -> []
            end

          [transition_field(transition, :from), transition_field(transition, :to) | extra_state]
        end)

    Enum.reduce(values, MapSet.new(), fn value, acc ->
      case canonicalize_identifier(value) do
        {:ok, identifier} -> MapSet.put(acc, identifier)
        {:error, _message} -> acc
      end
    end)
  end

  def state_lookup_key(subject_kind, lifecycle_state) do
    {canonicalize_identifier!(subject_kind), canonicalize_identifier!(lifecycle_state)}
  end

  def decision_event_key!({:after_execution_completed, recipe_ref}),
    do: {:execution_completed, canonicalize_identifier!(recipe_ref)}

  def decision_event_key!({:after_decision, decision_kind, decision_value}),
    do: {:decision_made, canonicalize_identifier!(decision_kind), decision_value}

  def decision_event_key!({:on_subject_entered_state, state}),
    do: {:subject_entered_state, canonicalize_identifier!(state)}

  def evidence_event_key!({:execution_completed, recipe_ref}),
    do: {:execution_completed, canonicalize_identifier!(recipe_ref)}

  def evidence_event_key!({:decision_created, decision_kind}),
    do: {:decision_created, canonicalize_identifier!(decision_kind)}

  def evidence_event_key!({:subject_entered_state, state}),
    do: {:subject_entered_state, canonicalize_identifier!(state)}

  def source_publish_event_key!({:subject_entered_state, state}),
    do: {:subject_entered_state, canonicalize_identifier!(state)}

  def source_publish_event_key!({:execution_completed, recipe_ref}),
    do: {:execution_completed, canonicalize_identifier!(recipe_ref)}

  def source_publish_event_key!({:decision_made, decision_kind, decision_value}),
    do: {:decision_made, canonicalize_identifier!(decision_kind), decision_value}

  def source_publish_event_key!({:operator_action, action_kind}),
    do: {:operator_action, canonicalize_identifier!(action_kind)}

  def transition_sort_key(transition) do
    {
      canonicalize_identifier!(transition_field(transition, :from)),
      inspect(canonicalize_trigger!(transition_field(transition, :trigger))),
      canonicalize_identifier!(transition_field(transition, :to))
    }
  end
end
