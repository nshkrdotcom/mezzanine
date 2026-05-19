defmodule Mezzanine.Pack.SchemaValidator do
  @moduledoc false

  alias Mezzanine.Pack.{
    ContextSourceSpec,
    DecisionSpec,
    EvidenceBinding,
    EvidenceSpec,
    ExecutionRecipeSpec,
    LifecycleSpec,
    Manifest,
    OperationDependency,
    OperationGraph,
    OperationRole,
    OperatorActionSpec,
    ProjectionSpec,
    ResourceEffectBinding,
    RuntimeBinding,
    SourceBinding,
    SourceBindingSpec,
    SourceKindSpec,
    SourcePublicationBinding,
    SourcePublishSpec,
    SubjectKindSpec,
    ToolBinding,
    ValidationError,
    WorkflowSpec
  }

  alias Mezzanine.Pack.Canonicalizer, as: H
  alias Mezzanine.Pack.ManifestOperationValidator

  @spec diagnostics(Manifest.t(), keyword()) :: [ValidationError.t()]
  def diagnostics(%Manifest{} = manifest, opts \\ []) when is_list(opts) do
    validate_manifest(manifest) ++
      validate_profile_slots(manifest.profile_slots) ++
      validate_subject_kind_specs(manifest.subject_kind_specs) ++
      validate_source_kind_specs(manifest.source_kind_specs) ++
      validate_binding_specs(manifest.binding_specs) ++
      validate_source_binding_specs(manifest.source_binding_specs) ++
      validate_source_publish_specs(manifest.source_publish_specs) ++
      validate_context_source_specs(manifest.context_source_specs) ++
      validate_lifecycle_specs(manifest.lifecycle_specs) ++
      validate_recipe_specs(manifest.execution_recipe_specs) ++
      validate_operation_graph_specs(manifest.operation_graph_specs) ++
      validate_workflow_specs(manifest.workflow_specs) ++
      validate_decision_specs(manifest.decision_specs) ++
      validate_evidence_specs(manifest.evidence_specs) ++
      validate_operator_action_specs(manifest.operator_action_specs) ++
      validate_projection_specs(manifest.projection_specs) ++
      validate_cross_references(manifest) ++
      ManifestOperationValidator.diagnostics(manifest, opts)
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

  defp validate_binding_specs(specs) do
    duplicate_identifier_issues(specs, :binding_ref, [:binding_specs], "binding") ++
      (specs
       |> Enum.with_index()
       |> Enum.flat_map(fn {spec, index} ->
         validate_binding_spec(spec, [:binding_specs, index])
       end))
  end

  defp validate_binding_spec(%SourceBinding{} = spec, path) do
    validate_common_binding(spec, path, "source")
    |> append(identifier_issue(spec.source_kind, path ++ [:source_kind], "source_kind"))
    |> append(identifier_issue(spec.subject_kind, path ++ [:subject_kind], "subject_kind"))
    |> append(optional_identifier_issue(spec.adapter_ref, path ++ [:adapter_ref], "adapter_ref"))
    |> append(
      optional_identifier_issue(spec.connection_ref, path ++ [:connection_ref], "connection_ref")
    )
    |> append(
      optional_identifier_issue(
        spec.candidate_filter_ref,
        path ++ [:candidate_filter_ref],
        "candidate_filter_ref"
      )
    )
    |> append(
      optional_identifier_issue(
        spec.cursor_policy_ref,
        path ++ [:cursor_policy_ref],
        "cursor_policy_ref"
      )
    )
    |> append(
      optional_identifier_issue(
        spec.projection_profile_ref,
        path ++ [:projection_profile_ref],
        "projection_profile_ref"
      )
    )
  end

  defp validate_binding_spec(%SourcePublicationBinding{} = spec, path) do
    validate_common_binding(spec, path, "source publication")
    |> append(
      identifier_issue(
        spec.source_binding_ref,
        path ++ [:source_binding_ref],
        "source_binding_ref"
      )
    )
    |> append(identifier_issue(spec.template_ref, path ++ [:template_ref], "template_ref"))
    |> append(
      source_publish_idempotency_scope_issue(spec.idempotency_scope, path ++ [:idempotency_scope])
    )
    |> append(
      optional_identifier_issue(
        spec.publication_profile_ref,
        path ++ [:publication_profile_ref],
        "publication_profile_ref"
      )
    )
  end

  defp validate_binding_spec(%RuntimeBinding{} = spec, path) do
    validate_common_binding(spec, path, "runtime")
    |> append(runtime_binding_family_issue(spec.runtime_family, path ++ [:runtime_family]))
    |> append(
      optional_identifier_issue(
        spec.session_policy_ref,
        path ++ [:session_policy_ref],
        "session_policy_ref"
      )
    )
    |> append(
      optional_identifier_issue(
        spec.tool_catalog_ref,
        path ++ [:tool_catalog_ref],
        "tool_catalog_ref"
      )
    )
  end

  defp validate_binding_spec(%ToolBinding{} = spec, path) do
    validate_common_binding(spec, path, "runtime tool")
    |> append(
      identifier_issue(
        spec.runtime_binding_ref,
        path ++ [:runtime_binding_ref],
        "runtime_binding_ref"
      )
    )
    |> append(
      identifier_issue(
        spec.authorization_class,
        path ++ [:authorization_class],
        "authorization_class"
      )
    )
    |> append(
      optional_identifier_issue(
        spec.tool_schema_ref,
        path ++ [:tool_schema_ref],
        "tool_schema_ref"
      )
    )
    |> append(
      optional_identifier_issue(
        spec.input_policy_ref,
        path ++ [:input_policy_ref],
        "input_policy_ref"
      )
    )
  end

  defp validate_binding_spec(%EvidenceBinding{} = spec, path) do
    validate_common_binding(spec, path, "evidence")
    |> append(identifier_issue(spec.evidence_kind, path ++ [:evidence_kind], "evidence_kind"))
    |> append(
      optional_identifier_issue(
        spec.collection_policy_ref,
        path ++ [:collection_policy_ref],
        "collection_policy_ref"
      )
    )
  end

  defp validate_binding_spec(%ResourceEffectBinding{} = spec, path) do
    validate_common_binding(spec, path, "resource effect")
    |> append(identifier_issue(spec.effect_kind, path ++ [:effect_kind], "effect_kind"))
    |> append(
      identifier_issue(
        spec.operation_group_ref,
        path ++ [:operation_group_ref],
        "operation_group_ref"
      )
    )
    |> append(
      identifier_issue(
        spec.confirmation_policy_ref,
        path ++ [:confirmation_policy_ref],
        "confirmation_policy_ref"
      )
    )
  end

  defp validate_binding_spec(spec, path) do
    [
      ValidationError.error(
        path,
        "binding_specs must contain explicit Mezzanine.Pack binding structs, got: #{inspect(spec)}"
      )
    ]
  end

  defp validate_common_binding(spec, path, label) do
    []
    |> append(identifier_issue(spec.binding_ref, path ++ [:binding_ref], "#{label} binding_ref"))
    |> append(identifier_issue(spec.connector_ref, path ++ [:connector_ref], "connector_ref"))
    |> append(identifier_issue(spec.manifest_ref, path ++ [:manifest_ref], "manifest_ref"))
    |> append(operation_refs_issue(spec.operation_refs, path ++ [:operation_refs]))
    |> append(
      identifier_issue(
        spec.credential_binding_ref,
        path ++ [:credential_binding_ref],
        "credential_binding_ref"
      )
    )
    |> append(
      optional_identifier_issue(
        Map.get(spec, :retry_policy_ref),
        path ++ [:retry_policy_ref],
        "retry_policy_ref"
      )
    )
    |> append(map_issue(Map.get(spec, :metadata, %{}), path ++ [:metadata]))
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
           map_issue(spec.dispatch_ref_requirements, [
             :execution_recipe_specs,
             index,
             :dispatch_ref_requirements
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

  defp validate_operation_graph_specs(specs) do
    duplicate_identifier_issues(specs, :graph_ref, [:operation_graph_specs], "operation graph") ++
      (specs
       |> Enum.with_index()
       |> Enum.flat_map(fn
         {%OperationGraph{} = spec, index} ->
           validate_operation_graph_spec(spec, [:operation_graph_specs, index])

         {spec, index} ->
           [
             ValidationError.error(
               [:operation_graph_specs, index],
               "operation_graph_specs must contain explicit Mezzanine.Pack.OperationGraph structs, got: #{inspect(spec)}"
             )
           ]
       end))
  end

  defp validate_operation_graph_spec(%OperationGraph{} = spec, path) do
    role_refs = identifier_set(Enum.map(spec.roles, &field_value(&1, :role_ref)))

    []
    |> append(identifier_issue(spec.graph_ref, path ++ [:graph_ref], "operation graph ref"))
    |> append(identifier_issue(spec.workflow_ref, path ++ [:workflow_ref], "workflow ref"))
    |> append(non_empty_list_issue(spec.roles, path ++ [:roles], "operation graph roles"))
    |> append(
      duplicate_identifier_issues(
        spec.roles,
        :role_ref,
        path ++ [:roles],
        "operation graph role"
      )
    )
    |> append(
      spec.roles
      |> Enum.with_index()
      |> Enum.flat_map(fn
        {%OperationRole{} = role, role_index} ->
          validate_operation_role(role, path ++ [:roles, role_index])

        {role, role_index} ->
          [
            ValidationError.error(
              path ++ [:roles, role_index],
              "operation graph roles must contain explicit Mezzanine.Pack.OperationRole structs, got: #{inspect(role)}"
            )
          ]
      end)
    )
    |> append(list_issue(spec.dependencies, path ++ [:dependencies], "operation dependencies"))
    |> append(
      spec.dependencies
      |> Enum.with_index()
      |> Enum.flat_map(fn
        {%OperationDependency{} = dependency, dependency_index} ->
          validate_operation_dependency(
            dependency,
            role_refs,
            path ++ [:dependencies, dependency_index]
          )

        {dependency, dependency_index} ->
          [
            ValidationError.error(
              path ++ [:dependencies, dependency_index],
              "operation dependencies must contain explicit Mezzanine.Pack.OperationDependency structs, got: #{inspect(dependency)}"
            )
          ]
      end)
    )
    |> append(list_issue(spec.joins, path ++ [:joins], "operation graph joins"))
    |> append(map_issue(spec.metadata, path ++ [:metadata]))
  end

  defp validate_operation_role(%OperationRole{} = role, path) do
    []
    |> append(identifier_issue(role.role_ref, path ++ [:role_ref], "operation role ref"))
    |> append(identifier_issue(role.binding_ref, path ++ [:binding_ref], "binding ref"))
    |> append(identifier_issue(role.operation_role, path ++ [:operation_role], "operation role"))
    |> append(operation_class_issue(role.operation_class, path ++ [:operation_class]))
    |> append(
      positive_integer_issue(
        role.projection_order_key,
        path ++ [:projection_order_key],
        "projection_order_key"
      )
    )
    |> append(completion_policy_issue(role.completion_policy, path ++ [:completion_policy]))
    |> append(failure_policy_issue(role.failure_policy, path ++ [:failure_policy]))
    |> append(map_issue(role.metadata, path ++ [:metadata]))
  end

  defp validate_operation_dependency(%OperationDependency{} = dependency, role_refs, path) do
    []
    |> append(
      reference_issue(
        dependency.from_role,
        role_refs,
        path ++ [:from_role],
        "dependency from role"
      )
    )
    |> append(
      reference_issue(dependency.to_role, role_refs, path ++ [:to_role], "dependency to role")
    )
    |> append(operation_relation_issue(dependency.relation, path ++ [:relation]))
    |> append(completion_policy_issue(dependency.completion_policy, path ++ [:completion_policy]))
    |> append(failure_policy_issue(dependency.failure_policy, path ++ [:failure_policy]))
    |> append(
      optional_identifier_issue(
        dependency.review_policy_ref,
        path ++ [:review_policy_ref],
        "review_policy_ref"
      )
    )
    |> append(
      optional_identifier_issue(
        dependency.confirmation_policy_ref,
        path ++ [:confirmation_policy_ref],
        "confirmation_policy_ref"
      )
    )
    |> append(map_issue(dependency.metadata, path ++ [:metadata]))
  end

  defp validate_workflow_specs(specs) do
    duplicate_identifier_issues(specs, :workflow_ref, [:workflow_specs], "workflow") ++
      (specs
       |> Enum.with_index()
       |> Enum.flat_map(fn
         {%WorkflowSpec{} = spec, index} ->
           []
           |> append(
             identifier_issue(
               spec.workflow_ref,
               [:workflow_specs, index, :workflow_ref],
               "workflow ref"
             )
           )
           |> append(
             identifier_issue(
               spec.operation_graph_ref,
               [:workflow_specs, index, :operation_graph_ref],
               "operation graph ref"
             )
           )
           |> append(
             optional_identifier_issue(
               spec.source_role_ref,
               [:workflow_specs, index, :source_role_ref],
               "source_role_ref"
             )
           )
           |> append(
             optional_identifier_issue(
               spec.runtime_role_ref,
               [:workflow_specs, index, :runtime_role_ref],
               "runtime_role_ref"
             )
           )
           |> append(
             optional_identifier_issue(
               spec.publication_role_ref,
               [:workflow_specs, index, :publication_role_ref],
               "publication_role_ref"
             )
           )
           |> append(
             identifier_list_issues(
               spec.evidence_role_refs,
               [:workflow_specs, index, :evidence_role_refs],
               "evidence role ref"
             )
           )
           |> append(
             identifier_list_issues(
               spec.resource_effect_role_refs,
               [:workflow_specs, index, :resource_effect_role_refs],
               "resource effect role ref"
             )
           )
           |> append(map_issue(spec.metadata, [:workflow_specs, index, :metadata]))

         {spec, index} ->
           [
             ValidationError.error(
               [:workflow_specs, index],
               "workflow_specs must contain explicit Mezzanine.Pack.WorkflowSpec structs, got: #{inspect(spec)}"
             )
           ]
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

    generic_source_binding_refs =
      manifest.binding_specs
      |> Enum.flat_map(fn
        %SourceBinding{} = binding -> [binding.binding_ref]
        _binding -> []
      end)
      |> identifier_set()

    generic_runtime_binding_refs =
      manifest.binding_specs
      |> Enum.flat_map(fn
        %RuntimeBinding{} = binding -> [binding.binding_ref]
        _binding -> []
      end)
      |> identifier_set()

    generic_binding_refs =
      identifier_set(Enum.map(manifest.binding_specs, &field_value(&1, :binding_ref)))

    generic_binding_operation_roles =
      Map.new(manifest.binding_specs, fn binding ->
        operation_refs = field_value(binding, :operation_refs)

        operation_roles =
          if is_map(operation_refs), do: Map.keys(operation_refs), else: []

        {canonical_identifier_or_nil(field_value(binding, :binding_ref)),
         identifier_set(operation_roles)}
      end)

    operation_graph_refs =
      identifier_set(Enum.map(manifest.operation_graph_specs, &field_value(&1, :graph_ref)))

    operation_graph_roles_by_ref =
      Map.new(manifest.operation_graph_specs, fn graph ->
        roles = field_value(graph, :roles) || []

        {canonical_identifier_or_nil(field_value(graph, :graph_ref)),
         roles |> Enum.map(&field_value(&1, :role_ref)) |> identifier_set()}
      end)

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

    generic_binding_issues =
      manifest.binding_specs
      |> Enum.with_index()
      |> Enum.flat_map(fn
        {%SourceBinding{} = spec, index} ->
          reference_issue(
            spec.subject_kind,
            subject_kinds,
            [:binding_specs, index, :subject_kind],
            "source binding subject kind"
          )

        {%SourcePublicationBinding{} = spec, index} ->
          reference_issue(
            spec.source_binding_ref,
            generic_source_binding_refs,
            [:binding_specs, index, :source_binding_ref],
            "source publication binding"
          )

        {%ToolBinding{} = spec, index} ->
          reference_issue(
            spec.runtime_binding_ref,
            generic_runtime_binding_refs,
            [:binding_specs, index, :runtime_binding_ref],
            "runtime tool binding"
          )

        {_spec, _index} ->
          []
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

    operation_graph_issues =
      manifest.operation_graph_specs
      |> Enum.with_index()
      |> Enum.flat_map(fn {%OperationGraph{} = spec, index} ->
        role_refs =
          Map.get(
            operation_graph_roles_by_ref,
            canonical_identifier_or_nil(spec.graph_ref),
            MapSet.new()
          )

        role_issues =
          spec.roles
          |> Enum.with_index()
          |> Enum.flat_map(fn
            {%OperationRole{} = role, role_index} ->
              binding_ref = canonical_identifier_or_nil(role.binding_ref)

              []
              |> append(
                reference_issue(
                  role.binding_ref,
                  generic_binding_refs,
                  [:operation_graph_specs, index, :roles, role_index, :binding_ref],
                  "operation role binding"
                )
              )
              |> append(
                reference_issue(
                  role.operation_role,
                  Map.get(generic_binding_operation_roles, binding_ref, MapSet.new()),
                  [:operation_graph_specs, index, :roles, role_index, :operation_role],
                  "binding operation role"
                )
              )

            {_role, _role_index} ->
              []
          end)

        dependency_issues =
          spec.dependencies
          |> Enum.with_index()
          |> Enum.flat_map(fn
            {%OperationDependency{} = dependency, dependency_index} ->
              []
              |> append(
                reference_issue(
                  dependency.from_role,
                  role_refs,
                  [:operation_graph_specs, index, :dependencies, dependency_index, :from_role],
                  "dependency from role"
                )
              )
              |> append(
                reference_issue(
                  dependency.to_role,
                  role_refs,
                  [:operation_graph_specs, index, :dependencies, dependency_index, :to_role],
                  "dependency to role"
                )
              )

            {_dependency, _dependency_index} ->
              []
          end)

        role_issues ++ dependency_issues
      end)

    workflow_issues =
      manifest.workflow_specs
      |> Enum.with_index()
      |> Enum.flat_map(fn
        {%WorkflowSpec{} = spec, index} ->
          role_refs =
            Map.get(
              operation_graph_roles_by_ref,
              canonical_identifier_or_nil(spec.operation_graph_ref),
              MapSet.new()
            )

          workflow_role_refs =
            [
              {:source_role_ref, spec.source_role_ref},
              {:runtime_role_ref, spec.runtime_role_ref},
              {:publication_role_ref, spec.publication_role_ref}
            ] ++
              Enum.map(Enum.with_index(spec.evidence_role_refs), fn {role_ref, role_index} ->
                {{:evidence_role_refs, role_index}, role_ref}
              end) ++
              Enum.map(Enum.with_index(spec.resource_effect_role_refs), fn {role_ref, role_index} ->
                {{:resource_effect_role_refs, role_index}, role_ref}
              end)

          []
          |> append(
            reference_issue(
              spec.operation_graph_ref,
              operation_graph_refs,
              [:workflow_specs, index, :operation_graph_ref],
              "workflow operation graph"
            )
          )
          |> append(
            workflow_role_refs
            |> Enum.reject(fn {_path_key, role_ref} -> is_nil(role_ref) end)
            |> Enum.flat_map(fn {path_key, role_ref} ->
              reference_issue(
                role_ref,
                role_refs,
                workflow_role_path(index, path_key),
                "workflow operation role"
              )
            end)
          )

        {_spec, _index} ->
          []
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
      generic_binding_issues ++
      lifecycle_issues ++
      recipe_issues ++
      operation_graph_issues ++
      workflow_issues ++
      decision_issues ++ evidence_issues ++ operator_action_issues ++ projection_issues
  end

  defp workflow_role_path(index, {list_field, role_index}),
    do: [:workflow_specs, index, list_field, role_index]

  defp workflow_role_path(index, field), do: [:workflow_specs, index, field]

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

  defp non_empty_list_issue(value, path, label) when is_list(value) do
    if value == [] do
      [ValidationError.error(path, "#{label} must not be empty")]
    else
      []
    end
  end

  defp non_empty_list_issue(_value, path, label),
    do: [ValidationError.error(path, "#{label} must be a list")]

  defp list_issue(value, _path, _label) when is_list(value), do: []

  defp list_issue(_value, path, label),
    do: [ValidationError.error(path, "#{label} must be a list")]

  defp map_issue(value, _path) when is_map(value), do: []
  defp map_issue(_value, path), do: [ValidationError.error(path, "must be a map")]

  defp operation_refs_issue(value, path) when is_map(value) do
    base_issues =
      if map_size(value) == 0 do
        [ValidationError.error(path, "operation_refs must not be empty")]
      else
        []
      end

    operation_issues =
      value
      |> Enum.flat_map(fn {role_ref, operation_ref} ->
        identifier_issue(role_ref, path ++ [:role], "operation_refs role") ++
          identifier_issue(operation_ref, path ++ [role_ref], "operation_refs operation ref")
      end)

    base_issues ++ operation_issues
  end

  defp operation_refs_issue(_value, path) do
    [
      ValidationError.error(
        path,
        "operation_refs must be a non-empty map of role refs to operation refs"
      )
    ]
  end

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

  defp operation_class_issue(value, path) do
    if value in [
         :source_read,
         :source_write,
         :runtime_operation,
         :runtime_tool_invocation,
         :evidence_collection,
         :resource_effect
       ] do
      []
    else
      [
        ValidationError.error(
          path,
          "operation_class must be a supported generic operation class"
        )
      ]
    end
  end

  defp operation_relation_issue(value, path) do
    if value in [
         :before,
         :after,
         :parallel_allowed,
         :blocks_on_success,
         :blocks_on_review,
         :blocks_on_confirmation
       ] do
      []
    else
      [
        ValidationError.error(
          path,
          "operation dependency relation is outside the supported relation set"
        )
      ]
    end
  end

  defp completion_policy_issue(value, path) do
    if value in [:required, :optional] do
      []
    else
      [ValidationError.error(path, "completion_policy must be :required or :optional")]
    end
  end

  defp failure_policy_issue(value, path) do
    if value in [:fail_closed, :degrade, :retry, :cancel] do
      []
    else
      [
        ValidationError.error(
          path,
          "failure_policy must be :fail_closed, :degrade, :retry, or :cancel"
        )
      ]
    end
  end

  defp runtime_binding_family_issue(value, path) do
    if value in [:direct, :session, :workflow, :playbook, :scan, :inference] do
      []
    else
      [
        ValidationError.error(
          path,
          "runtime_family must be :direct, :session, :workflow, :playbook, :scan, or :inference"
        )
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

  defp field_value(value, field) when is_map(value), do: Map.get(value, field)
  defp field_value(_value, _field), do: nil

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
