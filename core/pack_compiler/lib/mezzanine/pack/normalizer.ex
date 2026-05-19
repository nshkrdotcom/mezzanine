defmodule Mezzanine.Pack.Normalizer do
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
    WorkflowSpec
  }

  alias Mezzanine.Pack.Canonicalizer, as: H

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
      binding_specs:
        manifest.binding_specs
        |> Enum.map(&normalize_binding/1)
        |> Enum.sort_by(& &1.binding_ref),
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
      operation_graph_specs:
        manifest.operation_graph_specs
        |> Enum.map(&normalize_operation_graph/1)
        |> Enum.sort_by(& &1.graph_ref),
      workflow_specs:
        manifest.workflow_specs
        |> Enum.map(&normalize_workflow/1)
        |> Enum.sort_by(& &1.workflow_ref),
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

  @spec normalize_binding(Mezzanine.Pack.BindingSpec.binding_record()) ::
          Mezzanine.Pack.BindingSpec.binding_record()
  defp normalize_binding(%SourceBinding{} = spec) do
    %SourceBinding{
      spec
      | binding_ref: H.canonicalize_identifier!(spec.binding_ref),
        source_kind: H.canonicalize_identifier!(spec.source_kind),
        subject_kind: H.canonicalize_identifier!(spec.subject_kind),
        connector_ref: H.canonicalize_identifier!(spec.connector_ref),
        manifest_ref: H.canonicalize_identifier!(spec.manifest_ref),
        operation_refs: normalize_operation_refs(spec.operation_refs),
        credential_binding_ref: H.canonicalize_identifier!(spec.credential_binding_ref),
        adapter_ref: normalize_optional_identifier(spec.adapter_ref),
        connection_ref: normalize_optional_identifier(spec.connection_ref),
        candidate_filter_ref: normalize_optional_identifier(spec.candidate_filter_ref),
        cursor_policy_ref: normalize_optional_identifier(spec.cursor_policy_ref),
        projection_profile_ref: normalize_optional_identifier(spec.projection_profile_ref),
        retry_policy_ref: normalize_optional_identifier(spec.retry_policy_ref)
    }
  end

  defp normalize_binding(%SourcePublicationBinding{} = spec) do
    %SourcePublicationBinding{
      spec
      | binding_ref: H.canonicalize_identifier!(spec.binding_ref),
        source_binding_ref: H.canonicalize_identifier!(spec.source_binding_ref),
        connector_ref: H.canonicalize_identifier!(spec.connector_ref),
        manifest_ref: H.canonicalize_identifier!(spec.manifest_ref),
        operation_refs: normalize_operation_refs(spec.operation_refs),
        credential_binding_ref: H.canonicalize_identifier!(spec.credential_binding_ref),
        template_ref: H.canonicalize_identifier!(spec.template_ref),
        publication_profile_ref: normalize_optional_identifier(spec.publication_profile_ref),
        retry_policy_ref: normalize_optional_identifier(spec.retry_policy_ref)
    }
  end

  defp normalize_binding(%RuntimeBinding{} = spec) do
    %RuntimeBinding{
      spec
      | binding_ref: H.canonicalize_identifier!(spec.binding_ref),
        connector_ref: H.canonicalize_identifier!(spec.connector_ref),
        manifest_ref: H.canonicalize_identifier!(spec.manifest_ref),
        operation_refs: normalize_operation_refs(spec.operation_refs),
        credential_binding_ref: H.canonicalize_identifier!(spec.credential_binding_ref),
        session_policy_ref: normalize_optional_identifier(spec.session_policy_ref),
        tool_catalog_ref: normalize_optional_identifier(spec.tool_catalog_ref),
        retry_policy_ref: normalize_optional_identifier(spec.retry_policy_ref)
    }
  end

  defp normalize_binding(%ToolBinding{} = spec) do
    %ToolBinding{
      spec
      | binding_ref: H.canonicalize_identifier!(spec.binding_ref),
        runtime_binding_ref: H.canonicalize_identifier!(spec.runtime_binding_ref),
        connector_ref: H.canonicalize_identifier!(spec.connector_ref),
        manifest_ref: H.canonicalize_identifier!(spec.manifest_ref),
        operation_refs: normalize_operation_refs(spec.operation_refs),
        authorization_class: H.canonicalize_identifier!(spec.authorization_class),
        credential_binding_ref: H.canonicalize_identifier!(spec.credential_binding_ref),
        tool_schema_ref: normalize_optional_identifier(spec.tool_schema_ref),
        input_policy_ref: normalize_optional_identifier(spec.input_policy_ref),
        retry_policy_ref: normalize_optional_identifier(spec.retry_policy_ref)
    }
  end

  defp normalize_binding(%EvidenceBinding{} = spec) do
    %EvidenceBinding{
      spec
      | binding_ref: H.canonicalize_identifier!(spec.binding_ref),
        evidence_kind: H.canonicalize_identifier!(spec.evidence_kind),
        connector_ref: H.canonicalize_identifier!(spec.connector_ref),
        manifest_ref: H.canonicalize_identifier!(spec.manifest_ref),
        operation_refs: normalize_operation_refs(spec.operation_refs),
        credential_binding_ref: H.canonicalize_identifier!(spec.credential_binding_ref),
        collection_policy_ref: normalize_optional_identifier(spec.collection_policy_ref),
        retry_policy_ref: normalize_optional_identifier(spec.retry_policy_ref)
    }
  end

  defp normalize_binding(%ResourceEffectBinding{} = spec) do
    %ResourceEffectBinding{
      spec
      | binding_ref: H.canonicalize_identifier!(spec.binding_ref),
        effect_kind: H.canonicalize_identifier!(spec.effect_kind),
        connector_ref: H.canonicalize_identifier!(spec.connector_ref),
        manifest_ref: H.canonicalize_identifier!(spec.manifest_ref),
        operation_refs: normalize_operation_refs(spec.operation_refs),
        operation_group_ref: H.canonicalize_identifier!(spec.operation_group_ref),
        credential_binding_ref: H.canonicalize_identifier!(spec.credential_binding_ref),
        confirmation_policy_ref: normalize_optional_identifier(spec.confirmation_policy_ref),
        retry_policy_ref: normalize_optional_identifier(spec.retry_policy_ref)
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
        hook_stages: Enum.uniq(spec.hook_stages),
        dispatch_ref_requirements:
          normalize_dispatch_ref_requirements(spec.dispatch_ref_requirements)
    }
  end

  @spec normalize_operation_graph(OperationGraph.t()) :: OperationGraph.t()
  defp normalize_operation_graph(%OperationGraph{} = spec) do
    %OperationGraph{
      spec
      | graph_ref: H.canonicalize_identifier!(spec.graph_ref),
        workflow_ref: H.canonicalize_identifier!(spec.workflow_ref),
        roles:
          spec.roles
          |> Enum.map(&normalize_operation_role/1)
          |> Enum.sort_by(&{&1.projection_order_key, &1.role_ref}),
        dependencies:
          spec.dependencies
          |> Enum.map(&normalize_operation_dependency/1)
          |> Enum.sort_by(&{&1.to_role, &1.from_role, &1.relation})
    }
  end

  @spec normalize_operation_role(OperationRole.t()) :: OperationRole.t()
  defp normalize_operation_role(%OperationRole{} = role) do
    %OperationRole{
      role
      | role_ref: H.canonicalize_identifier!(role.role_ref),
        binding_ref: H.canonicalize_identifier!(role.binding_ref),
        operation_role: H.canonicalize_identifier!(role.operation_role)
    }
  end

  @spec normalize_operation_dependency(OperationDependency.t()) :: OperationDependency.t()
  defp normalize_operation_dependency(%OperationDependency{} = dependency) do
    %OperationDependency{
      dependency
      | from_role: H.canonicalize_identifier!(dependency.from_role),
        to_role: H.canonicalize_identifier!(dependency.to_role),
        review_policy_ref: normalize_optional_identifier(dependency.review_policy_ref),
        confirmation_policy_ref: normalize_optional_identifier(dependency.confirmation_policy_ref)
    }
  end

  @spec normalize_workflow(WorkflowSpec.t()) :: WorkflowSpec.t()
  defp normalize_workflow(%WorkflowSpec{} = spec) do
    %WorkflowSpec{
      spec
      | workflow_ref: H.canonicalize_identifier!(spec.workflow_ref),
        source_role_ref: normalize_optional_identifier(spec.source_role_ref),
        runtime_role_ref: normalize_optional_identifier(spec.runtime_role_ref),
        publication_role_ref: normalize_optional_identifier(spec.publication_role_ref),
        evidence_role_refs:
          spec.evidence_role_refs
          |> Enum.map(&H.canonicalize_identifier!/1)
          |> Enum.uniq()
          |> Enum.sort(),
        resource_effect_role_refs:
          spec.resource_effect_role_refs
          |> Enum.map(&H.canonicalize_identifier!/1)
          |> Enum.uniq()
          |> Enum.sort(),
        operation_graph_ref: H.canonicalize_identifier!(spec.operation_graph_ref)
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

  defp normalize_dispatch_ref_requirements(requirements) when is_map(requirements) do
    Map.new(requirements, fn {key, value} ->
      {H.canonicalize_identifier!(key), normalize_dispatch_requirement_value(value)}
    end)
  end

  defp normalize_dispatch_requirement_value(value) when is_atom(value), do: Atom.to_string(value)
  defp normalize_dispatch_requirement_value(value), do: value

  defp normalize_operation_refs(operation_refs) when is_map(operation_refs) do
    Map.new(operation_refs, fn {role_ref, operation_ref} ->
      {H.canonicalize_identifier!(role_ref), H.canonicalize_identifier!(operation_ref)}
    end)
  end

  defp normalize_optional_identifier(nil), do: nil
  defp normalize_optional_identifier(value), do: H.canonicalize_identifier!(value)
end
