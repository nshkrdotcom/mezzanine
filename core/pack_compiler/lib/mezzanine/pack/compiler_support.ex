defmodule Mezzanine.Pack.Compiler.Helpers do
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

defmodule Mezzanine.Pack.Validator do
  @moduledoc false

  alias Mezzanine.Pack.{
    ContextSourceSpec,
    DecisionSpec,
    EvidenceSpec,
    ExecutionRecipeSpec,
    LifecycleSpec,
    Manifest,
    OperatorActionSpec,
    ProjectionSpec,
    SourceBindingSpec,
    SourceKindSpec,
    SourcePublishSpec,
    SubjectKindSpec,
    ValidationError
  }

  alias Mezzanine.Pack.Compiler.Helpers, as: H

  @spec diagnostics(Manifest.t()) :: [ValidationError.t()]
  def diagnostics(%Manifest{} = manifest) do
    validate_manifest(manifest) ++
      validate_profile_slots(manifest.profile_slots) ++
      validate_subject_kind_specs(manifest.subject_kind_specs) ++
      validate_source_kind_specs(manifest.source_kind_specs) ++
      validate_source_binding_specs(manifest.source_binding_specs) ++
      validate_source_publish_specs(manifest.source_publish_specs) ++
      validate_context_source_specs(manifest.context_source_specs) ++
      validate_lifecycle_specs(manifest.lifecycle_specs) ++
      validate_recipe_specs(manifest.execution_recipe_specs) ++
      validate_decision_specs(manifest.decision_specs) ++
      validate_evidence_specs(manifest.evidence_specs) ++
      validate_operator_action_specs(manifest.operator_action_specs) ++
      validate_projection_specs(manifest.projection_specs) ++
      validate_cross_references(manifest)
  end

  defp validate_manifest(%Manifest{} = manifest) do
    []
    |> append(identifier_issue(manifest.pack_slug, [:pack_slug], "pack slug"))
    |> append(version_issue(manifest.version))
    |> append(migration_strategy_issue(manifest.migration_strategy))
    |> append(max_supersession_depth_issue(manifest.max_supersession_depth))
  end

  @profile_slots [
    :source_profile_ref,
    :runtime_profile_ref,
    :tool_scope_ref,
    :evidence_profile_ref,
    :publication_profile_ref,
    :review_profile_ref,
    :memory_profile_ref,
    :projection_profile_ref
  ]

  defp validate_profile_slots(nil) do
    [
      ValidationError.error(
        [:profile_slots],
        "profile_slots must declare all eight S0 profile slot refs"
      )
    ]
  end

  defp validate_profile_slots(slots) when is_map(slots) do
    unknown_issues =
      slots
      |> Map.keys()
      |> Enum.reject(&(&1 in @profile_slots))
      |> Enum.map(fn key ->
        ValidationError.error([:profile_slots, key], "profile slot #{inspect(key)} is unknown")
      end)

    missing_issues =
      @profile_slots
      |> Enum.reject(&Map.has_key?(slots, &1))
      |> Enum.map(fn slot ->
        ValidationError.error([:profile_slots, slot], "profile slot #{slot} is required")
      end)

    value_issues =
      Enum.flat_map(@profile_slots, fn slot ->
        value = Map.get(slots, slot)

        if profile_slot_ref?(slot, value) do
          []
        else
          [
            ValidationError.error(
              [:profile_slots, slot],
              "profile slot #{slot} must be an atom or {:custom, ref}"
            )
          ]
        end
      end)

    unknown_issues ++ missing_issues ++ value_issues
  end

  defp validate_profile_slots(_slots) do
    [
      ValidationError.error(
        [:profile_slots],
        "profile_slots must be a map of explicit profile slot refs"
      )
    ]
  end

  defp profile_slot_ref?(:memory_profile_ref, :none), do: true
  defp profile_slot_ref?(:memory_profile_ref, :private_facts_v1), do: true
  defp profile_slot_ref?(_slot, value) when is_atom(value) and not is_nil(value), do: true

  defp profile_slot_ref?(_slot, {:custom, value}) when is_binary(value),
    do: String.trim(value) != ""

  defp profile_slot_ref?(_slot, _value), do: false

  defp validate_subject_kind_specs(specs) do
    duplicate_identifier_issues(specs, :name, [:subject_kind_specs], "subject kind") ++
      (Enum.with_index(specs)
       |> Enum.flat_map(fn {%SubjectKindSpec{} = spec, index} ->
         []
         |> append(
           identifier_issue(spec.name, [:subject_kind_specs, index, :name], "subject kind name")
         )
         |> append(
           optional_module_issue(
             spec.normalizer_mod,
             [:subject_kind_specs, index, :normalizer_mod],
             "normalizer_mod"
           )
         )
       end))
  end

  defp validate_source_kind_specs(specs) do
    duplicate_identifier_issues(specs, :name, [:source_kind_specs], "source kind") ++
      (Enum.with_index(specs)
       |> Enum.flat_map(fn {%SourceKindSpec{} = spec, index} ->
         []
         |> append(
           identifier_issue(spec.name, [:source_kind_specs, index, :name], "source kind name")
         )
         |> append(
           identifier_issue(
             spec.subject_kind,
             [:source_kind_specs, index, :subject_kind],
             "source subject kind"
           )
         )
         |> append(
           optional_module_issue(
             spec.adapter_mod,
             [:source_kind_specs, index, :adapter_mod],
             "adapter_mod"
           )
         )
       end))
  end

  defp validate_source_binding_specs(specs) do
    duplicate_identifier_issues(specs, :binding_ref, [:source_binding_specs], "source binding") ++
      (Enum.with_index(specs)
       |> Enum.flat_map(fn {%SourceBindingSpec{} = spec, index} ->
         []
         |> append(
           identifier_issue(
             spec.binding_ref,
             [:source_binding_specs, index, :binding_ref],
             "source binding ref"
           )
         )
         |> append(
           identifier_issue(
             spec.source_kind,
             [:source_binding_specs, index, :source_kind],
             "source binding source kind"
           )
         )
         |> append(
           identifier_issue(
             spec.subject_kind,
             [:source_binding_specs, index, :subject_kind],
             "source binding subject kind"
           )
         )
         |> append(
           identifier_issue(spec.provider, [:source_binding_specs, index, :provider], "provider")
         )
         |> append(
           identifier_issue(
             spec.connection_ref,
             [:source_binding_specs, index, :connection_ref],
             "connection_ref"
           )
         )
         |> append(
           source_state_mapping_issue(spec.state_mapping, [
             :source_binding_specs,
             index,
             :state_mapping
           ])
         )
         |> append(
           map_issue(spec.candidate_filters, [:source_binding_specs, index, :candidate_filters])
         )
         |> append(map_issue(spec.cursor_policy, [:source_binding_specs, index, :cursor_policy]))
         |> append(
           map_issue(spec.source_write_policy, [
             :source_binding_specs,
             index,
             :source_write_policy
           ])
         )
       end))
  end

  defp validate_source_publish_specs(specs) do
    duplicate_identifier_issues(specs, :publish_ref, [:source_publish_specs], "source publish") ++
      (Enum.with_index(specs)
       |> Enum.flat_map(fn {%SourcePublishSpec{} = spec, index} ->
         []
         |> append(
           identifier_issue(
             spec.publish_ref,
             [:source_publish_specs, index, :publish_ref],
             "source publish ref"
           )
         )
         |> append(
           identifier_issue(
             spec.source_binding_ref,
             [:source_publish_specs, index, :source_binding_ref],
             "source publish binding ref"
           )
         )
         |> append(
           source_publish_trigger_issue(spec.trigger, [:source_publish_specs, index, :trigger])
         )
         |> append(
           source_publish_operation_issue(spec.operation, [
             :source_publish_specs,
             index,
             :operation
           ])
         )
         |> append(
           optional_identifier_issue(
             spec.template_ref,
             [:source_publish_specs, index, :template_ref],
             "template_ref"
           )
         )
         |> append(
           source_publish_idempotency_scope_issue(spec.idempotency_scope, [
             :source_publish_specs,
             index,
             :idempotency_scope
           ])
         )
       end))
  end

  defp validate_context_source_specs(specs) do
    duplicate_identifier_issues(specs, :source_ref, [:context_source_specs], "context source") ++
      (Enum.with_index(specs)
       |> Enum.flat_map(fn {%ContextSourceSpec{} = spec, index} ->
         []
         |> append(
           identifier_issue(
             spec.source_ref,
             [:context_source_specs, index, :source_ref],
             "context source ref"
           )
         )
         |> append(
           identifier_issue(
             spec.binding_key,
             [:context_source_specs, index, :binding_key],
             "context binding key"
           )
         )
         |> append(
           context_usage_phase_issue(
             spec.usage_phase,
             [:context_source_specs, index, :usage_phase]
           )
         )
         |> append(
           boolean_issue(spec.required?, [:context_source_specs, index, :required?], "required?")
         )
         |> append(
           positive_integer_issue(
             spec.timeout_ms,
             [:context_source_specs, index, :timeout_ms],
             "timeout_ms"
           )
         )
         |> append(
           optional_identifier_issue(
             spec.schema_ref,
             [:context_source_specs, index, :schema_ref],
             "schema_ref"
           )
         )
         |> append(
           positive_integer_issue(
             spec.max_fragments,
             [:context_source_specs, index, :max_fragments],
             "max_fragments"
           )
         )
         |> append(
           context_merge_strategy_issue(
             spec.merge_strategy,
             [:context_source_specs, index, :merge_strategy]
           )
         )
       end))
  end

  defp validate_lifecycle_specs(specs) do
    duplicate_identifier_issues(
      specs,
      :subject_kind,
      [:lifecycle_specs],
      "lifecycle subject kind"
    ) ++
      (Enum.with_index(specs)
       |> Enum.flat_map(fn {%LifecycleSpec{} = spec, index} ->
         validate_lifecycle_spec(spec, [:lifecycle_specs, index])
       end))
  end

  defp validate_lifecycle_spec(%LifecycleSpec{} = spec, path) do
    from_states =
      spec.transitions
      |> Enum.map(&canonical_transition_field(&1, :from))
      |> Enum.reject(&is_nil/1)
      |> MapSet.new()

    to_states =
      spec.transitions
      |> Enum.map(&canonical_transition_field(&1, :to))
      |> Enum.reject(&is_nil/1)
      |> MapSet.new()

    terminal_states = identifier_set(spec.terminal_states)
    initial_state = canonical_identifier_or_nil(spec.initial_state)

    []
    |> append(
      identifier_issue(spec.subject_kind, path ++ [:subject_kind], "lifecycle subject kind")
    )
    |> append(identifier_issue(spec.initial_state, path ++ [:initial_state], "initial state"))
    |> append(
      identifier_list_issues(spec.terminal_states, path ++ [:terminal_states], "terminal state")
    )
    |> append(validate_transition_list(spec.transitions, path ++ [:transitions]))
    |> append(
      initial_state_issue(initial_state, from_states, terminal_states, path ++ [:initial_state])
    )
    |> append(
      terminal_state_origin_issues(terminal_states, from_states, path ++ [:terminal_states])
    )
    |> append(
      target_state_issues(to_states, from_states, terminal_states, path ++ [:transitions])
    )
  end

  defp validate_transition_list(transitions, path) do
    transition_issues =
      transitions
      |> Enum.with_index()
      |> Enum.flat_map(fn {transition, index} ->
        validate_transition(transition, path ++ [index])
      end)

    duplicate_issues =
      transitions
      |> Enum.with_index()
      |> Enum.reduce({MapSet.new(), []}, fn transition_and_index, acc ->
        duplicate_transition_issue_step(transition_and_index, acc, path)
      end)
      |> elem(1)
      |> Enum.reverse()

    transition_issues ++ duplicate_issues
  end

  defp validate_transition(transition, path) do
    []
    |> append(
      identifier_issue(
        H.transition_field(transition, :from),
        path ++ [:from],
        "transition source state"
      )
    )
    |> append(
      identifier_issue(
        H.transition_field(transition, :to),
        path ++ [:to],
        "transition target state"
      )
    )
    |> append(trigger_issue(H.transition_field(transition, :trigger), path ++ [:trigger]))
    |> append(
      guard_issue(Map.get(transition, :guard) || Map.get(transition, "guard"), path ++ [:guard])
    )
  end

  defp validate_recipe_specs(specs) do
    duplicate_identifier_issues(specs, :recipe_ref, [:execution_recipe_specs], "execution recipe") ++
      (Enum.with_index(specs)
       |> Enum.flat_map(fn {%ExecutionRecipeSpec{} = spec, index} ->
         retry_config = spec.retry_config || %{}
         workspace_policy = spec.workspace_policy || %{}

         []
         |> append(
           identifier_issue(
             spec.recipe_ref,
             [:execution_recipe_specs, index, :recipe_ref],
             "recipe ref"
           )
         )
         |> append(
           identifier_issue(
             spec.placement_ref,
             [:execution_recipe_specs, index, :placement_ref],
             "placement ref"
           )
         )
         |> append(
           identifier_list_issues(
             spec.required_lifecycle_hints,
             [:execution_recipe_specs, index, :required_lifecycle_hints],
             "required lifecycle hint"
           )
         )
         |> append(
           duplicate_identifier_list_issues(
             spec.required_lifecycle_hints,
             [:execution_recipe_specs, index, :required_lifecycle_hints],
             "required lifecycle hint"
           )
         )
         |> append(
           runtime_class_issue(spec.runtime_class, [
             :execution_recipe_specs,
             index,
             :runtime_class
           ])
         )
         |> append(
           identifier_list_issues(
             spec.applicable_to,
             [:execution_recipe_specs, index, :applicable_to],
             "applicable subject kind"
           )
         )
         |> append(
           retry_config_issue(retry_config, [:execution_recipe_specs, index, :retry_config])
         )
         |> append(
           workspace_policy_issue(workspace_policy, [
             :execution_recipe_specs,
             index,
             :workspace_policy
           ])
         )
         |> append(
           identifier_issue(
             spec.sandbox_policy_ref,
             [:execution_recipe_specs, index, :sandbox_policy_ref],
             "sandbox_policy_ref"
           )
         )
         |> append(
           non_empty_identifier_list_issues(
             spec.prompt_refs,
             [:execution_recipe_specs, index, :prompt_refs],
             "prompt_refs"
           )
         )
         |> append(
           map_issue(spec.dynamic_tool_manifest, [
             :execution_recipe_specs,
             index,
             :dynamic_tool_manifest
           ])
         )
         |> append(
           atom_list_issue(spec.hook_stages, [:execution_recipe_specs, index, :hook_stages])
         )
         |> append(
           optional_positive_integer_issue(
             spec.max_turns,
             [:execution_recipe_specs, index, :max_turns],
             "max_turns"
           )
         )
         |> append(
           optional_positive_integer_issue(
             spec.stall_timeout_ms,
             [:execution_recipe_specs, index, :stall_timeout_ms],
             "stall_timeout_ms"
           )
         )
       end))
  end

  defp validate_decision_specs(specs) do
    duplicate_identifier_issues(specs, :decision_kind, [:decision_specs], "decision kind") ++
      (Enum.with_index(specs)
       |> Enum.flat_map(fn {%DecisionSpec{} = spec, index} ->
         []
         |> append(
           identifier_issue(
             spec.decision_kind,
             [:decision_specs, index, :decision_kind],
             "decision kind"
           )
         )
         |> append(decision_trigger_issue(spec.trigger, [:decision_specs, index, :trigger]))
         |> append(
           identifier_list_issues(
             spec.required_evidence_kinds,
             [:decision_specs, index, :required_evidence_kinds],
             "required evidence kind"
           )
         )
         |> append(
           identifier_list_issues(
             spec.authorized_actors,
             [:decision_specs, index, :authorized_actors],
             "authorized actor"
           )
         )
         |> append(
           allowed_decisions_issue(spec.allowed_decisions, [
             :decision_specs,
             index,
             :allowed_decisions
           ])
         )
         |> append(
           required_within_hours_issue(spec.required_within_hours, [
             :decision_specs,
             index,
             :required_within_hours
           ])
         )
       end))
  end

  defp validate_evidence_specs(specs) do
    duplicate_identifier_issues(specs, :evidence_kind, [:evidence_specs], "evidence kind") ++
      (Enum.with_index(specs)
       |> Enum.flat_map(fn {%EvidenceSpec{} = spec, index} ->
         []
         |> append(
           identifier_issue(
             spec.evidence_kind,
             [:evidence_specs, index, :evidence_kind],
             "evidence kind"
           )
         )
         |> append(
           identifier_issue(
             spec.collector_ref,
             [:evidence_specs, index, :collector_ref],
             "collector ref"
           )
         )
         |> append(
           collection_strategy_issue(spec.collection_strategy, [
             :evidence_specs,
             index,
             :collection_strategy
           ])
         )
         |> append(
           evidence_trigger_issue(spec.collected_on, [:evidence_specs, index, :collected_on])
         )
       end))
  end

  defp validate_operator_action_specs(specs) do
    duplicate_identifier_issues(specs, :action_kind, [:operator_action_specs], "operator action") ++
      (Enum.with_index(specs)
       |> Enum.flat_map(fn {%OperatorActionSpec{} = spec, index} ->
         []
         |> append(
           identifier_issue(
             spec.action_kind,
             [:operator_action_specs, index, :action_kind],
             "action kind"
           )
         )
         |> append(
           identifier_list_issues(
             spec.applicable_states,
             [:operator_action_specs, index, :applicable_states],
             "applicable state"
           )
         )
         |> append(
           identifier_list_issues(
             spec.authorized_roles,
             [:operator_action_specs, index, :authorized_roles],
             "authorized role"
           )
         )
         |> append(effect_issue(spec.effect, [:operator_action_specs, index, :effect]))
       end))
  end

  defp validate_projection_specs(specs) do
    duplicate_identifier_issues(specs, :name, [:projection_specs], "projection name") ++
      (Enum.with_index(specs)
       |> Enum.flat_map(fn {%ProjectionSpec{} = spec, index} ->
         []
         |> append(
           identifier_issue(spec.name, [:projection_specs, index, :name], "projection name")
         )
         |> append(
           identifier_list_issues(
             spec.subject_kinds,
             [:projection_specs, index, :subject_kinds],
             "projection subject kind"
           )
         )
       end))
  end

  defp validate_cross_references(%Manifest{} = manifest) do
    subject_kinds = identifier_set(Enum.map(manifest.subject_kind_specs, & &1.name))
    source_kind_refs = identifier_set(Enum.map(manifest.source_kind_specs, & &1.name))

    source_binding_refs =
      identifier_set(Enum.map(manifest.source_binding_specs, & &1.binding_ref))

    recipe_refs = identifier_set(Enum.map(manifest.execution_recipe_specs, & &1.recipe_ref))
    decision_kinds = identifier_set(Enum.map(manifest.decision_specs, & &1.decision_kind))
    evidence_kinds = identifier_set(Enum.map(manifest.evidence_specs, & &1.evidence_kind))
    action_kinds = identifier_set(Enum.map(manifest.operator_action_specs, & &1.action_kind))
    source_kind_subjects = source_kind_subjects(manifest.source_kind_specs)

    all_states =
      manifest.lifecycle_specs
      |> Enum.flat_map(&MapSet.to_list(H.lifecycle_states(&1)))
      |> MapSet.new()

    states_by_subject_kind =
      Map.new(manifest.lifecycle_specs, fn %LifecycleSpec{} = spec ->
        {H.canonicalize_identifier!(spec.subject_kind), H.lifecycle_states(spec)}
      end)

    expired_decision_kinds = expired_decision_kinds(manifest.lifecycle_specs)

    source_issues =
      manifest.source_kind_specs
      |> Enum.with_index()
      |> Enum.flat_map(fn {%SourceKindSpec{} = spec, index} ->
        reference_issue(
          spec.subject_kind,
          subject_kinds,
          [:source_kind_specs, index, :subject_kind],
          "source subject kind"
        )
      end)

    source_binding_issues =
      manifest.source_binding_specs
      |> Enum.with_index()
      |> Enum.flat_map(fn {%SourceBindingSpec{} = spec, index} ->
        subject_kind = canonical_identifier_or_nil(spec.subject_kind)

        source_kind_subject =
          Map.get(source_kind_subjects, canonical_identifier_or_nil(spec.source_kind))

        lifecycle_states = Map.get(states_by_subject_kind, subject_kind, MapSet.new())

        []
        |> append(
          reference_issue(
            spec.source_kind,
            source_kind_refs,
            [:source_binding_specs, index, :source_kind],
            "source binding source kind"
          )
        )
        |> append(
          reference_issue(
            spec.subject_kind,
            subject_kinds,
            [:source_binding_specs, index, :subject_kind],
            "source binding subject kind"
          )
        )
        |> append(
          source_kind_subject_match_issue(
            subject_kind,
            source_kind_subject,
            [:source_binding_specs, index, :subject_kind]
          )
        )
        |> append(
          source_state_mapping_reference_issues(
            spec.state_mapping,
            lifecycle_states,
            [:source_binding_specs, index, :state_mapping]
          )
        )
      end)

    source_publish_issues =
      manifest.source_publish_specs
      |> Enum.with_index()
      |> Enum.flat_map(fn {%SourcePublishSpec{} = spec, index} ->
        []
        |> append(
          reference_issue(
            spec.source_binding_ref,
            source_binding_refs,
            [:source_publish_specs, index, :source_binding_ref],
            "source publish binding"
          )
        )
        |> append(
          source_publish_trigger_reference_issue(
            spec.trigger,
            all_states,
            recipe_refs,
            decision_kinds,
            action_kinds,
            [:source_publish_specs, index, :trigger]
          )
        )
      end)

    lifecycle_issues =
      manifest.lifecycle_specs
      |> Enum.with_index()
      |> Enum.flat_map(fn {%LifecycleSpec{} = spec, index} ->
        lifecycle_state_set = H.lifecycle_states(spec)

        []
        |> append(
          reference_issue(
            spec.subject_kind,
            subject_kinds,
            [:lifecycle_specs, index, :subject_kind],
            "lifecycle subject kind"
          )
        )
        |> append(
          spec.transitions
          |> Enum.with_index()
          |> Enum.flat_map(fn {transition, transition_index} ->
            transition_trigger_reference_issue(
              H.transition_field(transition, :trigger),
              lifecycle_state_set,
              recipe_refs,
              decision_kinds,
              action_kinds,
              [:lifecycle_specs, index, :transitions, transition_index, :trigger]
            )
          end)
        )
      end)

    recipe_issues =
      manifest.execution_recipe_specs
      |> Enum.with_index()
      |> Enum.flat_map(fn {%ExecutionRecipeSpec{} = spec, index} ->
        spec.applicable_to
        |> Enum.with_index()
        |> Enum.flat_map(fn {subject_kind, applicable_index} ->
          reference_issue(
            subject_kind,
            subject_kinds,
            [:execution_recipe_specs, index, :applicable_to, applicable_index],
            "recipe subject kind"
          )
        end)
      end)

    decision_issues =
      manifest.decision_specs
      |> Enum.with_index()
      |> Enum.flat_map(fn {%DecisionSpec{} = spec, index} ->
        []
        |> append(
          decision_trigger_reference_issue(spec.trigger, recipe_refs, decision_kinds, [
            :decision_specs,
            index,
            :trigger
          ])
        )
        |> append(
          spec.required_evidence_kinds
          |> Enum.with_index()
          |> Enum.flat_map(fn {evidence_kind, evidence_index} ->
            reference_issue(
              evidence_kind,
              evidence_kinds,
              [:decision_specs, index, :required_evidence_kinds, evidence_index],
              "required evidence kind"
            )
          end)
        )
        |> append(
          expired_transition_requirement_issue(spec, expired_decision_kinds, [
            :decision_specs,
            index
          ])
        )
      end)

    evidence_issues =
      manifest.evidence_specs
      |> Enum.with_index()
      |> Enum.flat_map(fn {%EvidenceSpec{} = spec, index} ->
        evidence_trigger_reference_issue(spec.collected_on, recipe_refs, decision_kinds, [
          :evidence_specs,
          index,
          :collected_on
        ])
      end)

    operator_action_issues =
      manifest.operator_action_specs
      |> Enum.with_index()
      |> Enum.flat_map(fn {%OperatorActionSpec{} = spec, index} ->
        []
        |> append(
          spec.applicable_states
          |> Enum.with_index()
          |> Enum.flat_map(fn {state, state_index} ->
            reference_issue(
              state,
              all_states,
              [:operator_action_specs, index, :applicable_states, state_index],
              "operator applicable state"
            )
          end)
        )
        |> append(
          operator_effect_reference_issue(spec.effect, all_states, evidence_kinds, [
            :operator_action_specs,
            index,
            :effect
          ])
        )
      end)

    projection_issues =
      manifest.projection_specs
      |> Enum.with_index()
      |> Enum.flat_map(fn {%ProjectionSpec{} = spec, index} ->
        spec.subject_kinds
        |> Enum.with_index()
        |> Enum.flat_map(fn {subject_kind, subject_index} ->
          reference_issue(
            subject_kind,
            subject_kinds,
            [:projection_specs, index, :subject_kinds, subject_index],
            "projection subject kind"
          )
        end)
      end)

    source_issues ++
      source_binding_issues ++
      source_publish_issues ++
      lifecycle_issues ++
      recipe_issues ++
      decision_issues ++ evidence_issues ++ operator_action_issues ++ projection_issues
  end

  defp transition_trigger_reference_issue(
         trigger,
         lifecycle_states,
         recipe_refs,
         decision_kinds,
         action_kinds,
         path
       ) do
    case H.canonicalize_trigger(trigger) do
      {:ok, canonical_trigger} ->
        transition_trigger_reference_issue_resolved(
          canonical_trigger,
          lifecycle_states,
          recipe_refs,
          decision_kinds,
          action_kinds,
          path
        )

      {:error, message} ->
        [ValidationError.error(path, message)]
    end
  end

  defp decision_trigger_reference_issue(trigger, recipe_refs, decision_kinds, path) do
    case H.canonicalize_decision_trigger(trigger) do
      {:ok, {:after_execution_completed, recipe_ref}} ->
        reference_issue(recipe_ref, recipe_refs, path, "decision trigger recipe")

      {:ok, {:after_decision, decision_kind, decision_value}} ->
        reference_issue(decision_kind, decision_kinds, path, "decision trigger prior decision") ++
          decision_value_issue(decision_value, path)

      {:ok, {:on_subject_entered_state, _state}} ->
        []

      {:error, message} ->
        [ValidationError.error(path, message)]
    end
  end

  defp evidence_trigger_reference_issue(trigger, recipe_refs, decision_kinds, path) do
    case H.canonicalize_evidence_trigger(trigger) do
      {:ok, {:execution_completed, recipe_ref}} ->
        reference_issue(recipe_ref, recipe_refs, path, "evidence trigger recipe")

      {:ok, {:decision_created, decision_kind}} ->
        reference_issue(decision_kind, decision_kinds, path, "evidence trigger decision")

      {:ok, {:subject_entered_state, _state}} ->
        []

      {:error, message} ->
        [ValidationError.error(path, message)]
    end
  end

  defp operator_effect_reference_issue(effect, all_states, evidence_kinds, path) do
    case H.canonicalize_effect(effect) do
      {:ok, {:advance_lifecycle, state}} ->
        if MapSet.member?(all_states, state) do
          []
        else
          [ValidationError.error(path, "references unknown lifecycle state #{inspect(state)}")]
        end

      {:ok, {:collect_evidence, evidence_kind}} ->
        reference_issue(evidence_kind, evidence_kinds, path, "operator evidence kind")

      {:ok, _effect} ->
        []

      {:error, message} ->
        [ValidationError.error(path, message)]
    end
  end

  defp source_publish_trigger_reference_issue(
         trigger,
         all_states,
         recipe_refs,
         decision_kinds,
         action_kinds,
         path
       ) do
    case H.canonicalize_source_publish_trigger(trigger) do
      {:ok, {:subject_entered_state, state}} ->
        if MapSet.member?(all_states, state) do
          []
        else
          [ValidationError.error(path, "references unknown lifecycle state #{inspect(state)}")]
        end

      {:ok, {:execution_completed, recipe_ref}} ->
        reference_issue(recipe_ref, recipe_refs, path, "source publish recipe")

      {:ok, {:decision_made, decision_kind, decision_value}} ->
        reference_issue(decision_kind, decision_kinds, path, "source publish decision") ++
          decision_value_issue(decision_value, path)

      {:ok, {:operator_action, action_kind}} ->
        reference_issue(action_kind, action_kinds, path, "source publish operator action")

      {:error, message} ->
        [ValidationError.error(path, message)]
    end
  end

  defp source_kind_subjects(source_kind_specs) do
    Map.new(source_kind_specs, fn %SourceKindSpec{} = spec ->
      {H.canonicalize_identifier!(spec.name), H.canonicalize_identifier!(spec.subject_kind)}
    end)
  end

  defp source_kind_subject_match_issue(nil, _source_kind_subject, _path), do: []
  defp source_kind_subject_match_issue(_subject_kind, nil, _path), do: []

  defp source_kind_subject_match_issue(subject_kind, source_kind_subject, path) do
    if subject_kind == source_kind_subject do
      []
    else
      [
        ValidationError.error(
          path,
          "source binding subject kind must match its source kind subject kind"
        )
      ]
    end
  end

  defp source_state_mapping_reference_issues(mapping, lifecycle_states, path)
       when is_map(mapping) do
    mapping
    |> Map.keys()
    |> Enum.flat_map(&source_state_mapping_reference_issue(&1, lifecycle_states, path))
  end

  defp source_state_mapping_reference_issues(_mapping, _lifecycle_states, _path), do: []

  defp source_state_mapping_reference_issue(state, lifecycle_states, path) do
    case H.canonicalize_identifier(state) do
      {:ok, lifecycle_state} ->
        unknown_lifecycle_state_issue(lifecycle_state, lifecycle_states, path)

      {:error, message} ->
        [ValidationError.error(path, message)]
    end
  end

  defp unknown_lifecycle_state_issue(lifecycle_state, lifecycle_states, path) do
    if MapSet.member?(lifecycle_states, lifecycle_state) do
      []
    else
      [
        ValidationError.error(
          path,
          "state_mapping references unknown lifecycle state #{inspect(lifecycle_state)}"
        )
      ]
    end
  end

  defp expired_decision_kinds(lifecycle_specs) do
    lifecycle_specs
    |> Enum.flat_map(&expired_decisions_for_lifecycle/1)
    |> MapSet.new()
  end

  defp expired_transition_requirement_issue(
         %DecisionSpec{required_within_hours: nil},
         _expired_decision_kinds,
         _path
       ),
       do: []

  defp expired_transition_requirement_issue(%DecisionSpec{} = spec, expired_decision_kinds, path) do
    case H.canonicalize_identifier(spec.decision_kind) do
      {:ok, decision_kind} ->
        if MapSet.member?(expired_decision_kinds, decision_kind) do
          []
        else
          [
            ValidationError.error(
              path ++ [:decision_kind],
              "requires a {:decision_made, kind, :expired} lifecycle transition"
            )
          ]
        end

      {:error, _message} ->
        []
    end
  end

  defp identifier_issue(value, path, label) do
    case H.canonicalize_identifier(value) do
      {:ok, _identifier} -> []
      {:error, message} -> [ValidationError.error(path, "#{label} #{message}")]
    end
  end

  defp identifier_list_issues(values, path, label) do
    values
    |> Enum.with_index()
    |> Enum.flat_map(fn {value, index} -> identifier_issue(value, path ++ [index], label) end)
  end

  defp non_empty_identifier_list_issues([], path, label),
    do: [ValidationError.error(path, "#{label} must not be empty")]

  defp non_empty_identifier_list_issues(values, path, label) when is_list(values) do
    identifier_list_issues(values, path, label)
  end

  defp non_empty_identifier_list_issues(_values, path, label),
    do: [ValidationError.error(path, "#{label} must be a list")]

  defp duplicate_identifier_list_issues(values, path_root, label) do
    values
    |> Enum.with_index()
    |> Enum.reduce({MapSet.new(), []}, fn {value, index}, {seen, issues} ->
      case H.canonicalize_identifier(value) do
        {:ok, identifier} ->
          duplicate_identifier_issue(identifier, seen, issues, path_root, index, label)

        {:error, _message} ->
          {seen, issues}
      end
    end)
    |> elem(1)
    |> Enum.reverse()
  end

  defp duplicate_identifier_issue(identifier, seen, issues, path_root, index, label) do
    if MapSet.member?(seen, identifier) do
      issue =
        ValidationError.error(
          path_root ++ [index],
          "#{label} #{inspect(identifier)} must be unique"
        )

      {seen, [issue | issues]}
    else
      {MapSet.put(seen, identifier), issues}
    end
  end

  defp optional_module_issue(nil, _path, _label), do: []
  defp optional_module_issue(value, _path, _label) when is_atom(value), do: []

  defp optional_module_issue(_value, path, label),
    do: [ValidationError.error(path, "#{label} must be a module when present")]

  defp optional_identifier_issue(nil, _path, _label), do: []
  defp optional_identifier_issue(value, path, label), do: identifier_issue(value, path, label)

  defp boolean_issue(value, _path, _label) when is_boolean(value), do: []

  defp boolean_issue(_value, path, label),
    do: [ValidationError.error(path, "#{label} must be a boolean")]

  defp positive_integer_issue(value, _path, _label) when is_integer(value) and value > 0, do: []

  defp positive_integer_issue(_value, path, label),
    do: [ValidationError.error(path, "#{label} must be a positive integer")]

  defp optional_positive_integer_issue(nil, _path, _label), do: []

  defp optional_positive_integer_issue(value, _path, _label) when is_integer(value) and value > 0,
    do: []

  defp optional_positive_integer_issue(_value, path, label),
    do: [ValidationError.error(path, "#{label} must be a positive integer when present")]

  defp map_issue(value, _path) when is_map(value), do: []
  defp map_issue(_value, path), do: [ValidationError.error(path, "must be a map")]

  defp atom_list_issue(values, path) when is_list(values) do
    values
    |> Enum.with_index()
    |> Enum.flat_map(fn
      {value, _index} when is_atom(value) -> []
      {_value, index} -> [ValidationError.error(path ++ [index], "hook stage must be an atom")]
    end)
  end

  defp atom_list_issue(_values, path),
    do: [ValidationError.error(path, "hook_stages must be a list")]

  defp trigger_issue(trigger, path) do
    case H.canonicalize_trigger(trigger) do
      {:ok, {:execution_failed, _recipe_ref, failure_kind}} ->
        failure_kind_issue(failure_kind, path)

      {:ok, {:decision_made, _decision_kind, decision_value}} ->
        decision_value_issue(decision_value, path)

      {:ok, _trigger} ->
        []

      {:error, message} ->
        [ValidationError.error(path, message)]
    end
  end

  defp guard_issue(nil, _path), do: []

  defp guard_issue(%{module: module, function: function}, path) do
    []
    |> append(optional_module_issue(module, path ++ [:module], "guard module"))
    |> append(function_issue(function, path ++ [:function]))
    |> append(guard_export_issue(module, function, path))
  end

  defp guard_issue(_other, path),
    do: [ValidationError.error(path, "guard must be nil or a %{module: ..., function: ...} map")]

  defp function_issue(value, _path) when is_atom(value), do: []

  defp function_issue(_value, path),
    do: [ValidationError.error(path, "guard function must be an atom")]

  defp guard_export_issue(module, function, path) when is_atom(module) and is_atom(function) do
    if Code.ensure_loaded?(module) and function_exported?(module, function, 1) do
      []
    else
      [ValidationError.error(path, "guard #{inspect(module)}.#{function}/1 is not exported")]
    end
  end

  defp guard_export_issue(_module, _function, _path), do: []

  defp runtime_class_issue(value, path) do
    if H.runtime_class?(value) do
      []
    else
      [
        ValidationError.error(path, "runtime_class must be one of the supported runtime classes")
      ]
    end
  end

  defp context_usage_phase_issue(value, path) do
    if H.context_usage_phase?(value) do
      []
    else
      [
        ValidationError.error(
          path,
          "usage_phase must be :preprocess, :retrieval, or :repair"
        )
      ]
    end
  end

  defp context_merge_strategy_issue(value, path) do
    if H.context_merge_strategy?(value) do
      []
    else
      [
        ValidationError.error(
          path,
          "merge_strategy must be :append, :ranked_append, or :replace_slot"
        )
      ]
    end
  end

  defp retry_config_issue(retry_config, path) when is_map(retry_config) do
    []
    |> append(max_attempts_issue(retry_config[:max_attempts], path ++ [:max_attempts]))
    |> append(backoff_issue(retry_config[:backoff], path ++ [:backoff]))
    |> append(retry_on_issue(retry_config[:retry_on] || [], path ++ [:retry_on]))
    |> append(rekey_on_issue(retry_config[:rekey_on] || [], path ++ [:rekey_on]))
  end

  defp retry_config_issue(_retry_config, path),
    do: [ValidationError.error(path, "retry_config must be a map")]

  defp max_attempts_issue(value, _path) when is_integer(value) and value > 0, do: []

  defp max_attempts_issue(_value, path),
    do: [ValidationError.error(path, "max_attempts must be a positive integer")]

  defp backoff_issue(value, _path) when value in [:linear, :exponential], do: []

  defp backoff_issue(_value, path),
    do: [ValidationError.error(path, "backoff must be :linear or :exponential")]

  defp retry_on_issue(values, path) do
    values
    |> Enum.with_index()
    |> Enum.flat_map(fn {value, index} -> failure_kind_issue(value, path ++ [index]) end)
  end

  defp rekey_on_issue(values, path), do: retry_on_issue(values, path)

  defp workspace_policy_issue(policy, path) when is_map(policy) do
    strategy = policy[:strategy] || policy["strategy"]
    root_ref = policy[:root_ref] || policy["root_ref"]

    strategy_issues =
      if strategy in [:per_subject, :per_execution, :shared, :none] do
        []
      else
        [
          ValidationError.error(
            path ++ [:strategy],
            "workspace strategy must be :per_subject, :per_execution, :shared, or :none"
          )
        ]
      end

    root_issues =
      if strategy == :none do
        []
      else
        case H.canonicalize_identifier(root_ref) do
          {:ok, _root_ref} ->
            []

          {:error, _message} ->
            [
              ValidationError.error(
                path ++ [:root_ref],
                "workspace root must be declared as root_ref when workspace strategy is not :none"
              )
            ]
        end
      end

    strategy_issues ++ root_issues
  end

  defp workspace_policy_issue(_policy, path),
    do: [ValidationError.error(path, "workspace_policy must be a map")]

  defp source_state_mapping_issue(mapping, path) when is_map(mapping) do
    base_issues =
      if map_size(mapping) == 0 do
        [ValidationError.error(path, "state_mapping must not be empty")]
      else
        []
      end

    mapping_issues =
      mapping
      |> Enum.flat_map(fn {state, provider_states} ->
        identifier_issue(state, path ++ [:state], "state_mapping lifecycle state") ++
          provider_state_list_issue(provider_states, path ++ [state])
      end)

    base_issues ++ mapping_issues
  end

  defp source_state_mapping_issue(_mapping, path),
    do: [ValidationError.error(path, "state_mapping must be a map")]

  defp provider_state_list_issue(values, path) when is_list(values) and values != [] do
    values
    |> Enum.with_index()
    |> Enum.flat_map(fn
      {value, _index} when is_binary(value) and value != "" ->
        []

      {_value, index} ->
        ValidationError.error(
          path ++ [index],
          "provider state must be a non-empty string"
        )
        |> List.wrap()
    end)
  end

  defp provider_state_list_issue(_values, path),
    do: [ValidationError.error(path, "provider state list must not be empty")]

  defp source_publish_trigger_issue(trigger, path) do
    case H.canonicalize_source_publish_trigger(trigger) do
      {:ok, {:decision_made, _decision_kind, decision_value}} ->
        decision_value_issue(decision_value, path)

      {:ok, _trigger} ->
        []

      {:error, message} ->
        [ValidationError.error(path, message)]
    end
  end

  defp source_publish_operation_issue(value, path) do
    if H.source_publish_operation?(value) do
      []
    else
      [
        ValidationError.error(
          path,
          "source publish operation must be :update_state, :create_comment, :update_comment, :add_label, or :remove_label"
        )
      ]
    end
  end

  defp source_publish_idempotency_scope_issue(value, path) do
    if H.source_publish_idempotency_scope?(value) do
      []
    else
      [
        ValidationError.error(
          path,
          "idempotency_scope must be :subject, :execution, or :source_event"
        )
      ]
    end
  end

  defp decision_trigger_issue(trigger, path) do
    case H.canonicalize_decision_trigger(trigger) do
      {:ok, {:after_decision, _decision_kind, decision_value}} ->
        decision_value_issue(decision_value, path)

      {:ok, _trigger} ->
        []

      {:error, message} ->
        [ValidationError.error(path, message)]
    end
  end

  defp allowed_decisions_issue([], path),
    do: [ValidationError.error(path, "allowed_decisions must not be empty")]

  defp allowed_decisions_issue(values, path) do
    values
    |> Enum.with_index()
    |> Enum.flat_map(fn {value, index} -> decision_value_issue(value, path ++ [index]) end)
  end

  defp decision_value_issue(value, path) do
    if H.decision_value?(value) do
      []
    else
      [
        ValidationError.error(
          path,
          "decision value must be :accept, :reject, :waive, :expired, or :escalate"
        )
      ]
    end
  end

  defp required_within_hours_issue(nil, _path), do: []
  defp required_within_hours_issue(value, _path) when is_integer(value) and value > 0, do: []

  defp required_within_hours_issue(_value, path),
    do: [
      ValidationError.error(path, "required_within_hours must be a positive integer when present")
    ]

  defp collection_strategy_issue(value, _path) when value in [:automatic, :manual, :on_demand],
    do: []

  defp collection_strategy_issue(_value, path),
    do: [
      ValidationError.error(
        path,
        "collection_strategy must be :automatic, :manual, or :on_demand"
      )
    ]

  defp evidence_trigger_issue(trigger, path) do
    case H.canonicalize_evidence_trigger(trigger) do
      {:ok, _trigger} -> []
      {:error, message} -> [ValidationError.error(path, message)]
    end
  end

  defp effect_issue(effect, path) do
    case H.canonicalize_effect(effect) do
      {:ok, _effect} -> []
      {:error, message} -> [ValidationError.error(path, message)]
    end
  end

  defp version_issue(value) when is_binary(value) and byte_size(value) > 0, do: []

  defp version_issue(_value),
    do: [ValidationError.error([:version], "version must be a non-empty string")]

  defp migration_strategy_issue(value) when value in [:additive, :force], do: []

  defp migration_strategy_issue(_value),
    do: [
      ValidationError.error(
        [:migration_strategy],
        "migration_strategy must be :additive or :force"
      )
    ]

  defp max_supersession_depth_issue(value)
       when is_integer(value) and value > 0 and value <= 32,
       do: []

  defp max_supersession_depth_issue(_value) do
    [
      ValidationError.error(
        [:max_supersession_depth],
        "max_supersession_depth must be a positive integer not greater than 32"
      )
    ]
  end

  defp failure_kind_issue(value, path) do
    if H.failure_kind?(value) do
      []
    else
      [ValidationError.error(path, "execution failure kind is outside the canonical taxonomy")]
    end
  end

  defp duplicate_identifier_issues(specs, field, path_root, label) do
    specs
    |> Enum.with_index()
    |> Enum.reduce({MapSet.new(), []}, fn spec_and_index, acc ->
      duplicate_identifier_issue_step(spec_and_index, acc, field, path_root, label)
    end)
    |> elem(1)
    |> Enum.reverse()
  end

  defp reference_issue(value, known_values, path, label) do
    case H.canonicalize_identifier(value) do
      {:ok, identifier} ->
        if MapSet.member?(known_values, identifier) do
          []
        else
          [
            ValidationError.error(
              path,
              "#{label} #{inspect(identifier)} is not declared in the pack"
            )
          ]
        end

      {:error, message} ->
        [ValidationError.error(path, message)]
    end
  end

  defp identifier_set(values) do
    values
    |> Enum.reduce(MapSet.new(), fn value, acc ->
      case H.canonicalize_identifier(value) do
        {:ok, identifier} -> MapSet.put(acc, identifier)
        {:error, _message} -> acc
      end
    end)
  end

  defp canonical_transition_field(transition, field) do
    case H.canonicalize_identifier(H.transition_field(transition, field)) do
      {:ok, identifier} -> identifier
      {:error, _message} -> nil
    end
  end

  defp canonical_identifier_or_nil(value) do
    case H.canonicalize_identifier(value) do
      {:ok, identifier} -> identifier
      {:error, _message} -> nil
    end
  end

  defp initial_state_issue(nil, _from_states, _terminal_states, _path), do: []

  defp initial_state_issue(initial_state, from_states, terminal_states, path) do
    if MapSet.member?(from_states, initial_state) or
         MapSet.member?(terminal_states, initial_state) do
      []
    else
      [
        ValidationError.error(
          path,
          "initial_state must appear as a transition source or be terminal"
        )
      ]
    end
  end

  defp terminal_state_origin_issues(terminal_states, from_states, path) do
    terminal_states
    |> Enum.flat_map(fn state ->
      if MapSet.member?(from_states, state) do
        [
          ValidationError.error(
            path,
            "terminal state #{inspect(state)} cannot appear as a transition source"
          )
        ]
      else
        []
      end
    end)
  end

  defp target_state_issues(to_states, from_states, terminal_states, path) do
    to_states
    |> Enum.flat_map(fn state ->
      if MapSet.member?(from_states, state) or MapSet.member?(terminal_states, state) do
        []
      else
        [
          ValidationError.error(
            path,
            "target state #{inspect(state)} must continue from another transition or be terminal"
          )
        ]
      end
    end)
  end

  defp append(list, []), do: list
  defp append(list, other), do: list ++ other

  defp duplicate_transition_issue_step({transition, index}, {seen, issues}, path) do
    case {H.canonicalize_identifier(H.transition_field(transition, :from)),
          H.canonicalize_trigger(H.transition_field(transition, :trigger))} do
      {{:ok, from}, {:ok, trigger}} ->
        pair = {from, trigger}

        if MapSet.member?(seen, pair) do
          issue =
            ValidationError.error(
              path ++ [index],
              "duplicate lifecycle transition for the same {from, trigger} pair"
            )

          {seen, [issue | issues]}
        else
          {MapSet.put(seen, pair), issues}
        end

      _ ->
        {seen, issues}
    end
  end

  defp transition_trigger_reference_issue_resolved(
         {:execution_requested, recipe_ref},
         _lifecycle_states,
         recipe_refs,
         _decision_kinds,
         _action_kinds,
         path
       ),
       do: reference_issue(recipe_ref, recipe_refs, path, "execution recipe")

  defp transition_trigger_reference_issue_resolved(
         {:execution_completed, recipe_ref},
         _lifecycle_states,
         recipe_refs,
         _decision_kinds,
         _action_kinds,
         path
       ),
       do: reference_issue(recipe_ref, recipe_refs, path, "execution recipe")

  defp transition_trigger_reference_issue_resolved(
         {:execution_failed, recipe_ref},
         _lifecycle_states,
         recipe_refs,
         _decision_kinds,
         _action_kinds,
         path
       ),
       do: reference_issue(recipe_ref, recipe_refs, path, "execution recipe")

  defp transition_trigger_reference_issue_resolved(
         {:execution_failed, recipe_ref, failure_kind},
         _lifecycle_states,
         recipe_refs,
         _decision_kinds,
         _action_kinds,
         path
       ),
       do:
         reference_issue(recipe_ref, recipe_refs, path, "execution recipe") ++
           failure_kind_issue(failure_kind, path)

  defp transition_trigger_reference_issue_resolved(
         {:join_completed, _join_step_ref},
         _lifecycle_states,
         _recipe_refs,
         _decision_kinds,
         _action_kinds,
         _path
       ),
       do: []

  defp transition_trigger_reference_issue_resolved(
         {:decision_made, decision_kind, decision_value},
         _lifecycle_states,
         _recipe_refs,
         decision_kinds,
         _action_kinds,
         path
       ),
       do:
         reference_issue(decision_kind, decision_kinds, path, "decision kind") ++
           decision_value_issue(decision_value, path)

  defp transition_trigger_reference_issue_resolved(
         {:operator_action, action_kind},
         _lifecycle_states,
         _recipe_refs,
         _decision_kinds,
         action_kinds,
         path
       ),
       do: reference_issue(action_kind, action_kinds, path, "operator action")

  defp transition_trigger_reference_issue_resolved(
         {:subject_entered_state, state},
         lifecycle_states,
         _recipe_refs,
         _decision_kinds,
         _action_kinds,
         path
       ) do
    if MapSet.member?(lifecycle_states, state) do
      []
    else
      [ValidationError.error(path, "references unknown lifecycle state #{inspect(state)}")]
    end
  end

  defp transition_trigger_reference_issue_resolved(
         :auto,
         _lifecycle_states,
         _recipe_refs,
         _decision_kinds,
         _action_kinds,
         _path
       ),
       do: []

  defp expired_decisions_for_lifecycle(%LifecycleSpec{} = spec) do
    spec.transitions
    |> Enum.flat_map(fn transition ->
      case H.canonicalize_trigger(H.transition_field(transition, :trigger)) do
        {:ok, {:decision_made, decision_kind, :expired}} -> [decision_kind]
        _ -> []
      end
    end)
  end

  defp duplicate_identifier_issue_step({spec, index}, {seen, issues}, field, path_root, label) do
    case H.canonicalize_identifier(Map.get(spec, field)) do
      {:ok, identifier} ->
        add_duplicate_identifier_issue(
          seen,
          issues,
          identifier,
          path_root ++ [index, field],
          label
        )

      {:error, _message} ->
        {seen, issues}
    end
  end

  defp add_duplicate_identifier_issue(seen, issues, identifier, path, label) do
    if MapSet.member?(seen, identifier) do
      issue =
        ValidationError.error(
          path,
          "#{label} #{inspect(identifier)} must be unique within the pack"
        )

      {seen, [issue | issues]}
    else
      {MapSet.put(seen, identifier), issues}
    end
  end
end

defmodule Mezzanine.Pack.Normalizer do
  @moduledoc false

  alias Mezzanine.Pack.{
    ContextSourceSpec,
    DecisionSpec,
    EvidenceSpec,
    ExecutionRecipeSpec,
    LifecycleSpec,
    Manifest,
    OperatorActionSpec,
    ProjectionSpec,
    SourceBindingSpec,
    SourceKindSpec,
    SourcePublishSpec,
    SubjectKindSpec
  }

  alias Mezzanine.Pack.Compiler.Helpers, as: H

  @spec normalize(struct()) :: struct()
  def normalize(%Manifest{} = manifest) do
    %Manifest{
      pack_slug: H.canonicalize_identifier!(manifest.pack_slug),
      version: manifest.version,
      description: manifest.description,
      migration_strategy: manifest.migration_strategy,
      max_supersession_depth: manifest.max_supersession_depth,
      profile_slots: normalize_profile_slots(manifest.profile_slots),
      subject_kind_specs:
        manifest.subject_kind_specs
        |> Enum.map(&normalize_subject_kind/1)
        |> Enum.sort_by(& &1.name),
      source_kind_specs:
        manifest.source_kind_specs
        |> Enum.map(&normalize_source_kind/1)
        |> Enum.sort_by(& &1.name),
      source_binding_specs:
        manifest.source_binding_specs
        |> Enum.map(&normalize_source_binding/1)
        |> Enum.sort_by(& &1.binding_ref),
      source_publish_specs:
        manifest.source_publish_specs
        |> Enum.map(&normalize_source_publish/1)
        |> Enum.sort_by(& &1.publish_ref),
      context_source_specs:
        manifest.context_source_specs
        |> Enum.map(&normalize_context_source/1)
        |> Enum.sort_by(& &1.source_ref),
      lifecycle_specs:
        manifest.lifecycle_specs
        |> Enum.map(&normalize_lifecycle/1)
        |> Enum.sort_by(& &1.subject_kind),
      execution_recipe_specs:
        manifest.execution_recipe_specs
        |> Enum.map(&normalize_recipe/1)
        |> Enum.sort_by(& &1.recipe_ref),
      decision_specs:
        manifest.decision_specs
        |> Enum.map(&normalize_decision/1)
        |> Enum.sort_by(& &1.decision_kind),
      evidence_specs:
        manifest.evidence_specs
        |> Enum.map(&normalize_evidence/1)
        |> Enum.sort_by(& &1.evidence_kind),
      operator_action_specs:
        manifest.operator_action_specs
        |> Enum.map(&normalize_operator_action/1)
        |> Enum.sort_by(& &1.action_kind),
      projection_specs:
        manifest.projection_specs |> Enum.map(&normalize_projection/1) |> Enum.sort_by(& &1.name)
    }
  end

  @spec normalize_profile_slots(map() | nil) :: map() | nil
  defp normalize_profile_slots(slots) when is_map(slots) do
    Map.new(
      [
        :source_profile_ref,
        :runtime_profile_ref,
        :tool_scope_ref,
        :evidence_profile_ref,
        :publication_profile_ref,
        :review_profile_ref,
        :memory_profile_ref,
        :projection_profile_ref
      ],
      fn slot -> {slot, normalize_profile_slot_ref(Map.fetch!(slots, slot))} end
    )
  end

  defp normalize_profile_slots(nil), do: nil

  @spec normalize_profile_slot_ref(atom() | {:custom, String.t()}) ::
          atom() | {:custom, String.t()}
  defp normalize_profile_slot_ref({:custom, ref}), do: {:custom, ref}
  defp normalize_profile_slot_ref(ref), do: ref

  @spec normalize_subject_kind(SubjectKindSpec.t()) :: SubjectKindSpec.t()
  defp normalize_subject_kind(%SubjectKindSpec{} = spec) do
    %SubjectKindSpec{spec | name: H.canonicalize_identifier!(spec.name)}
  end

  @spec normalize_source_kind(SourceKindSpec.t()) :: SourceKindSpec.t()
  defp normalize_source_kind(%SourceKindSpec{} = spec) do
    %SourceKindSpec{
      spec
      | name: H.canonicalize_identifier!(spec.name),
        subject_kind: H.canonicalize_identifier!(spec.subject_kind)
    }
  end

  @spec normalize_source_binding(SourceBindingSpec.t()) :: SourceBindingSpec.t()
  defp normalize_source_binding(%SourceBindingSpec{} = spec) do
    %SourceBindingSpec{
      spec
      | binding_ref: H.canonicalize_identifier!(spec.binding_ref),
        source_kind: H.canonicalize_identifier!(spec.source_kind),
        subject_kind: H.canonicalize_identifier!(spec.subject_kind),
        provider: H.canonicalize_identifier!(spec.provider),
        connection_ref: H.canonicalize_identifier!(spec.connection_ref),
        state_mapping: normalize_state_mapping(spec.state_mapping)
    }
  end

  @spec normalize_source_publish(SourcePublishSpec.t()) :: SourcePublishSpec.t()
  defp normalize_source_publish(%SourcePublishSpec{} = spec) do
    %SourcePublishSpec{
      spec
      | publish_ref: H.canonicalize_identifier!(spec.publish_ref),
        source_binding_ref: H.canonicalize_identifier!(spec.source_binding_ref),
        trigger: H.canonicalize_source_publish_trigger!(spec.trigger),
        template_ref: normalize_optional_identifier(spec.template_ref)
    }
  end

  @spec normalize_context_source(ContextSourceSpec.t()) :: ContextSourceSpec.t()
  defp normalize_context_source(%ContextSourceSpec{} = spec) do
    %ContextSourceSpec{
      spec
      | source_ref: H.canonicalize_identifier!(spec.source_ref),
        binding_key: H.canonicalize_identifier!(spec.binding_key)
    }
  end

  @spec normalize_lifecycle(LifecycleSpec.t()) :: LifecycleSpec.t()
  defp normalize_lifecycle(%LifecycleSpec{} = spec) do
    %LifecycleSpec{
      spec
      | subject_kind: H.canonicalize_identifier!(spec.subject_kind),
        initial_state: H.canonicalize_identifier!(spec.initial_state),
        terminal_states:
          spec.terminal_states
          |> Enum.map(&H.canonicalize_identifier!/1)
          |> Enum.uniq()
          |> Enum.sort(),
        transitions:
          spec.transitions
          |> Enum.map(&normalize_transition/1)
          |> Enum.sort_by(&H.transition_sort_key/1)
    }
  end

  @spec normalize_transition(LifecycleSpec.transition()) :: LifecycleSpec.transition()
  defp normalize_transition(transition) do
    base = %{
      from: H.canonicalize_identifier!(H.transition_field(transition, :from)),
      to: H.canonicalize_identifier!(H.transition_field(transition, :to)),
      trigger: H.canonicalize_trigger!(H.transition_field(transition, :trigger))
    }

    case Map.get(transition, :guard) || Map.get(transition, "guard") do
      nil -> base
      guard -> Map.put(base, :guard, guard)
    end
  end

  @spec normalize_recipe(ExecutionRecipeSpec.t()) :: ExecutionRecipeSpec.t()
  defp normalize_recipe(%ExecutionRecipeSpec{} = spec) do
    retry_on = spec.retry_config[:retry_on] || []
    rekey_on = spec.retry_config[:rekey_on] || []

    %ExecutionRecipeSpec{
      spec
      | recipe_ref: H.canonicalize_identifier!(spec.recipe_ref),
        placement_ref: H.canonicalize_identifier!(spec.placement_ref),
        required_lifecycle_hints:
          spec.required_lifecycle_hints
          |> Enum.map(&H.canonicalize_identifier!/1)
          |> Enum.uniq()
          |> Enum.sort(),
        applicable_to:
          spec.applicable_to
          |> Enum.map(&H.canonicalize_identifier!/1)
          |> Enum.uniq()
          |> Enum.sort(),
        retry_config:
          spec.retry_config
          |> Map.put(:retry_on, Enum.uniq(retry_on))
          |> Map.put(:rekey_on, Enum.uniq(rekey_on)),
        workspace_policy: normalize_workspace_policy(spec.workspace_policy),
        sandbox_policy_ref: H.canonicalize_identifier!(spec.sandbox_policy_ref),
        prompt_refs:
          spec.prompt_refs
          |> Enum.map(&H.canonicalize_identifier!/1)
          |> Enum.uniq()
          |> Enum.sort(),
        hook_stages: Enum.uniq(spec.hook_stages)
    }
  end

  @spec normalize_decision(DecisionSpec.t()) :: DecisionSpec.t()
  defp normalize_decision(%DecisionSpec{} = spec) do
    %DecisionSpec{
      spec
      | decision_kind: H.canonicalize_identifier!(spec.decision_kind),
        trigger: H.canonicalize_decision_trigger!(spec.trigger),
        required_evidence_kinds:
          spec.required_evidence_kinds
          |> Enum.map(&H.canonicalize_identifier!/1)
          |> Enum.uniq()
          |> Enum.sort(),
        authorized_actors:
          spec.authorized_actors
          |> Enum.map(&H.canonicalize_identifier!/1)
          |> Enum.uniq()
          |> Enum.sort(),
        allowed_decisions: spec.allowed_decisions |> Enum.uniq() |> Enum.sort()
    }
  end

  @spec normalize_evidence(EvidenceSpec.t()) :: EvidenceSpec.t()
  defp normalize_evidence(%EvidenceSpec{} = spec) do
    %EvidenceSpec{
      spec
      | evidence_kind: H.canonicalize_identifier!(spec.evidence_kind),
        collector_ref: H.canonicalize_identifier!(spec.collector_ref),
        collected_on: H.canonicalize_evidence_trigger!(spec.collected_on)
    }
  end

  @spec normalize_operator_action(OperatorActionSpec.t()) :: OperatorActionSpec.t()
  defp normalize_operator_action(%OperatorActionSpec{} = spec) do
    %OperatorActionSpec{
      spec
      | action_kind: H.canonicalize_identifier!(spec.action_kind),
        applicable_states:
          spec.applicable_states
          |> Enum.map(&H.canonicalize_identifier!/1)
          |> Enum.uniq()
          |> Enum.sort(),
        authorized_roles:
          spec.authorized_roles
          |> Enum.map(&H.canonicalize_identifier!/1)
          |> Enum.uniq()
          |> Enum.sort(),
        effect: H.canonicalize_effect!(spec.effect)
    }
  end

  @spec normalize_projection(ProjectionSpec.t()) :: ProjectionSpec.t()
  defp normalize_projection(%ProjectionSpec{} = spec) do
    %ProjectionSpec{
      spec
      | name: H.canonicalize_identifier!(spec.name),
        subject_kinds:
          spec.subject_kinds
          |> Enum.map(&H.canonicalize_identifier!/1)
          |> Enum.uniq()
          |> Enum.sort(),
        sort: Enum.sort_by(spec.sort, fn {field, direction} -> {field, direction} end)
    }
  end

  defp normalize_state_mapping(mapping) when is_map(mapping) do
    Map.new(mapping, fn {state, provider_states} ->
      {H.canonicalize_identifier!(state),
       Enum.map(provider_states, &H.canonicalize_identifier!/1)}
    end)
  end

  defp normalize_workspace_policy(policy) when is_map(policy) do
    case Map.get(policy, :root_ref) || Map.get(policy, "root_ref") do
      nil ->
        policy

      root_ref ->
        policy
        |> Map.drop(["root_ref"])
        |> Map.put(:root_ref, H.canonicalize_identifier!(root_ref))
    end
  end

  defp normalize_optional_identifier(nil), do: nil
  defp normalize_optional_identifier(value), do: H.canonicalize_identifier!(value)
end

defmodule Mezzanine.Pack.Builder do
  @moduledoc false

  alias Mezzanine.Pack.{
    CompiledPack,
    ContextSourceSpec,
    DecisionSpec,
    EvidenceSpec,
    ExecutionRecipeSpec,
    LifecycleSpec,
    Manifest
  }

  alias Mezzanine.Pack.Compiler.Helpers, as: H

  @spec build(struct()) :: CompiledPack.t()
  def build(%Manifest{} = manifest) do
    subject_kinds = Map.new(manifest.subject_kind_specs, &{&1.name, &1})

    %CompiledPack{
      pack_slug: manifest.pack_slug,
      version: manifest.version,
      manifest: manifest,
      subject_kinds: subject_kinds,
      source_kinds: Map.new(manifest.source_kind_specs, &{&1.name, &1}),
      source_bindings_by_ref: Map.new(manifest.source_binding_specs, &{&1.binding_ref, &1}),
      source_publishers_by_ref: Map.new(manifest.source_publish_specs, &{&1.publish_ref, &1}),
      context_sources_by_ref:
        Map.new(manifest.context_source_specs, fn %ContextSourceSpec{} = spec ->
          {spec.source_ref, spec}
        end),
      lifecycle_by_kind: Map.new(manifest.lifecycle_specs, &{&1.subject_kind, &1}),
      transitions_by_state: build_transition_index(manifest.lifecycle_specs),
      terminal_states_by_kind:
        Map.new(manifest.lifecycle_specs, fn %LifecycleSpec{} = lifecycle ->
          {lifecycle.subject_kind, MapSet.new(lifecycle.terminal_states)}
        end),
      recipes_by_ref: Map.new(manifest.execution_recipe_specs, &{&1.recipe_ref, &1}),
      recipes_by_subject_kind:
        build_recipe_subject_index(manifest.execution_recipe_specs, Map.keys(subject_kinds)),
      decision_specs_by_kind: Map.new(manifest.decision_specs, &{&1.decision_kind, &1}),
      evidence_specs_by_kind: Map.new(manifest.evidence_specs, &{&1.evidence_kind, &1}),
      operator_actions_by_kind: Map.new(manifest.operator_action_specs, &{&1.action_kind, &1}),
      projections_by_name: Map.new(manifest.projection_specs, &{&1.name, &1}),
      decision_triggers_by_event: build_decision_event_index(manifest.decision_specs),
      evidence_triggers_by_event: build_evidence_event_index(manifest.evidence_specs)
    }
  end

  @spec build_transition_index([LifecycleSpec.t()]) ::
          %{
            CompiledPack.state_key() => %{
              CompiledPack.trigger_key() => LifecycleSpec.transition()
            }
          }
  defp build_transition_index(lifecycle_specs) do
    Enum.reduce(lifecycle_specs, %{}, &put_lifecycle_transitions/2)
  end

  @spec build_recipe_subject_index([ExecutionRecipeSpec.t()], [String.t()]) ::
          %{String.t() => [ExecutionRecipeSpec.t()]}
  defp build_recipe_subject_index(recipes, subject_kinds) do
    initial = Map.new(subject_kinds, &{&1, []})

    recipes
    |> Enum.reduce(initial, fn %ExecutionRecipeSpec{} = recipe, acc ->
      applicable_subject_kinds =
        if recipe.applicable_to == [], do: subject_kinds, else: recipe.applicable_to

      Enum.reduce(applicable_subject_kinds, acc, fn subject_kind, inner_acc ->
        Map.update!(inner_acc, subject_kind, &[recipe | &1])
      end)
    end)
    |> Map.new(fn {subject_kind, subject_recipes} ->
      {subject_kind, Enum.sort_by(subject_recipes, & &1.recipe_ref)}
    end)
  end

  @spec build_decision_event_index([DecisionSpec.t()]) ::
          %{CompiledPack.trigger_key() => [DecisionSpec.t()]}
  defp build_decision_event_index(decision_specs) do
    Enum.reduce(decision_specs, %{}, fn %DecisionSpec{} = spec, acc ->
      Map.update(acc, H.decision_event_key!(spec.trigger), [spec], &[spec | &1])
    end)
  end

  @spec build_evidence_event_index([EvidenceSpec.t()]) ::
          %{CompiledPack.trigger_key() => [EvidenceSpec.t()]}
  defp build_evidence_event_index(evidence_specs) do
    Enum.reduce(evidence_specs, %{}, fn %EvidenceSpec{} = spec, acc ->
      Map.update(acc, H.evidence_event_key!(spec.collected_on), [spec], &[spec | &1])
    end)
  end

  @spec put_lifecycle_transitions(
          LifecycleSpec.t(),
          %{
            CompiledPack.state_key() => %{
              CompiledPack.trigger_key() => LifecycleSpec.transition()
            }
          }
        ) ::
          %{
            CompiledPack.state_key() => %{
              CompiledPack.trigger_key() => LifecycleSpec.transition()
            }
          }
  defp put_lifecycle_transitions(%LifecycleSpec{} = lifecycle, acc) do
    Enum.reduce(lifecycle.transitions, acc, fn transition, inner_acc ->
      put_transition(inner_acc, lifecycle.subject_kind, transition)
    end)
  end

  @spec put_transition(
          %{
            CompiledPack.state_key() => %{
              CompiledPack.trigger_key() => LifecycleSpec.transition()
            }
          },
          String.t(),
          LifecycleSpec.transition()
        ) ::
          %{
            CompiledPack.state_key() => %{
              CompiledPack.trigger_key() => LifecycleSpec.transition()
            }
          }
  defp put_transition(acc, subject_kind, transition) do
    key = H.state_lookup_key(subject_kind, transition.from)
    transition_map = %{transition.trigger => transition}

    Map.update(acc, key, transition_map, &Map.put(&1, transition.trigger, transition))
  end
end
