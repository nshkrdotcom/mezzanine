defmodule Mezzanine.Pack.ManifestEmitter do
  @moduledoc false

  alias Mezzanine.Pack.{
    BindingSpec,
    CompiledPack,
    ContextSourceSpec,
    DecisionSpec,
    EvidenceSpec,
    ExecutionRecipeSpec,
    LifecycleSpec,
    Manifest,
    OperationGraphCompiler
  }

  alias Mezzanine.Pack.Canonicalizer, as: H

  @spec emit(Manifest.t()) :: CompiledPack.t()
  def emit(%Manifest{} = manifest) do
    subject_kinds = Map.new(manifest.subject_kind_specs, &{&1.name, &1})

    %CompiledPack{
      pack_slug: manifest.pack_slug,
      version: manifest.version,
      manifest: manifest,
      subject_kinds: subject_kinds,
      source_kinds: Map.new(manifest.source_kind_specs, &{&1.name, &1}),
      bindings_by_ref: Map.new(manifest.binding_specs, &{&1.binding_ref, &1}),
      bindings_by_kind: build_binding_kind_index(manifest.binding_specs),
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
      operation_graphs_by_ref: Map.new(manifest.operation_graph_specs, &{&1.graph_ref, &1}),
      compiled_operation_graphs_by_ref:
        OperationGraphCompiler.compile_index(
          manifest.operation_graph_specs,
          manifest.binding_specs
        ),
      workflows_by_ref: Map.new(manifest.workflow_specs, &{&1.workflow_ref, &1}),
      decision_specs_by_kind: Map.new(manifest.decision_specs, &{&1.decision_kind, &1}),
      evidence_specs_by_kind: Map.new(manifest.evidence_specs, &{&1.evidence_kind, &1}),
      operator_actions_by_kind: Map.new(manifest.operator_action_specs, &{&1.action_kind, &1}),
      projections_by_name: Map.new(manifest.projection_specs, &{&1.name, &1}),
      decision_triggers_by_event: build_decision_event_index(manifest.decision_specs),
      evidence_triggers_by_event: build_evidence_event_index(manifest.evidence_specs)
    }
  end

  @spec build_binding_kind_index([BindingSpec.binding_record()]) ::
          %{BindingSpec.binding_kind() => [BindingSpec.binding_record()]}
  defp build_binding_kind_index(binding_specs) do
    binding_specs
    |> Enum.group_by(&BindingSpec.kind/1)
    |> Map.new(fn {kind, bindings} ->
      {kind, Enum.sort_by(bindings, & &1.binding_ref)}
    end)
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
