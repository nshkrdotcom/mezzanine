defmodule Mezzanine.Pack.Serializer do
  @moduledoc """
  Storage serializer for compiled packs persisted through the neutral registry.

  The durable payload remains JSON-shaped and canonicalizes pack-defined
  identifiers to strings. On load, the stored manifest is recompiled to rebuild
  the O(1) runtime indices.
  """

  alias Mezzanine.Pack.{
    CompiledPack,
    Compiler,
    ContextSourceSpec,
    DecisionSpec,
    EvidenceSpec,
    ExecutionRecipeSpec,
    LifecycleSpec,
    Manifest,
    OperatorActionSpec,
    ProjectionSpec,
    SourceKindSpec,
    SubjectKindSpec
  }

  @format_version 1

  @spec version() :: pos_integer()
  def version, do: @format_version

  @spec serialize_compiled(CompiledPack.t()) :: map()
  def serialize_compiled(%CompiledPack{} = compiled_pack) do
    %{
      "format_version" => @format_version,
      "pack_slug" => compiled_pack.pack_slug,
      "version" => compiled_pack.version,
      "manifest" => serialize_manifest(compiled_pack.manifest)
    }
  end

  @spec deserialize_compiled(map()) :: {:ok, CompiledPack.t()} | {:error, term()}
  def deserialize_compiled(%{"manifest" => manifest_payload}) when is_map(manifest_payload) do
    with {:ok, manifest} <- deserialize_manifest(manifest_payload) do
      Compiler.compile(manifest)
    end
  end

  def deserialize_compiled(_payload), do: {:error, :invalid_compiled_manifest}

  @spec serialize_manifest(Manifest.t()) :: map()
  def serialize_manifest(%Manifest{} = manifest) do
    %{
      "pack_slug" => serialize_identifier(manifest.pack_slug),
      "version" => manifest.version,
      "description" => manifest.description,
      "migration_strategy" => Atom.to_string(manifest.migration_strategy),
      "max_supersession_depth" => manifest.max_supersession_depth,
      "subject_kind_specs" => Enum.map(manifest.subject_kind_specs, &serialize_subject_kind/1),
      "source_kind_specs" => Enum.map(manifest.source_kind_specs, &serialize_source_kind/1),
      "context_source_specs" =>
        Enum.map(manifest.context_source_specs, &serialize_context_source/1),
      "lifecycle_specs" => Enum.map(manifest.lifecycle_specs, &serialize_lifecycle/1),
      "execution_recipe_specs" =>
        Enum.map(manifest.execution_recipe_specs, &serialize_execution_recipe/1),
      "decision_specs" => Enum.map(manifest.decision_specs, &serialize_decision_spec/1),
      "evidence_specs" => Enum.map(manifest.evidence_specs, &serialize_evidence_spec/1),
      "operator_action_specs" =>
        Enum.map(manifest.operator_action_specs, &serialize_operator_action/1),
      "projection_specs" => Enum.map(manifest.projection_specs, &serialize_projection_spec/1)
    }
  end

  @spec deserialize_manifest(map()) :: {:ok, Manifest.t()} | {:error, term()}
  def deserialize_manifest(%{} = payload) do
    {:ok, build_manifest(payload)}
  rescue
    error in [ArgumentError, KeyError] ->
      {:error, {:invalid_manifest_payload, Exception.message(error)}}
  end

  defp serialize_subject_kind(%SubjectKindSpec{} = spec) do
    %{
      "name" => serialize_identifier(spec.name),
      "description" => spec.description,
      "payload_schema" => serialize_map(spec.payload_schema),
      "normalizer_mod" => serialize_module(spec.normalizer_mod)
    }
  end

  defp deserialize_subject_kind(payload) do
    %SubjectKindSpec{
      name: payload["name"],
      description: payload["description"],
      payload_schema: deserialize_map(payload["payload_schema"] || %{}),
      normalizer_mod: deserialize_module(payload["normalizer_mod"])
    }
  end

  defp serialize_source_kind(%SourceKindSpec{} = spec) do
    %{
      "name" => serialize_identifier(spec.name),
      "subject_kind" => serialize_identifier(spec.subject_kind),
      "description" => spec.description,
      "adapter_mod" => serialize_module(spec.adapter_mod)
    }
  end

  defp deserialize_source_kind(payload) do
    %SourceKindSpec{
      name: payload["name"],
      subject_kind: payload["subject_kind"],
      description: payload["description"],
      adapter_mod: deserialize_module(payload["adapter_mod"])
    }
  end

  defp serialize_context_source(%ContextSourceSpec{} = spec) do
    %{
      "source_ref" => serialize_identifier(spec.source_ref),
      "description" => spec.description,
      "binding_key" => serialize_identifier(spec.binding_key),
      "usage_phase" => Atom.to_string(spec.usage_phase),
      "required?" => spec.required?,
      "timeout_ms" => spec.timeout_ms,
      "schema_ref" => spec.schema_ref,
      "max_fragments" => spec.max_fragments,
      "merge_strategy" => Atom.to_string(spec.merge_strategy)
    }
  end

  defp deserialize_context_source(payload) do
    %ContextSourceSpec{
      source_ref: payload["source_ref"],
      description: payload["description"],
      binding_key: payload["binding_key"],
      usage_phase: deserialize_atom(payload["usage_phase"], [:preprocess, :retrieval, :repair]),
      required?: Map.get(payload, "required?", false),
      timeout_ms: Map.get(payload, "timeout_ms", 1_000),
      schema_ref: payload["schema_ref"],
      max_fragments: Map.get(payload, "max_fragments", 5),
      merge_strategy:
        deserialize_atom(payload["merge_strategy"], [:append, :ranked_append, :replace_slot])
    }
  end

  defp serialize_lifecycle(%LifecycleSpec{} = spec) do
    %{
      "subject_kind" => serialize_identifier(spec.subject_kind),
      "initial_state" => serialize_identifier(spec.initial_state),
      "terminal_states" => Enum.map(spec.terminal_states, &serialize_identifier/1),
      "transitions" => Enum.map(spec.transitions, &serialize_transition/1)
    }
  end

  defp deserialize_lifecycle(payload) do
    %LifecycleSpec{
      subject_kind: payload["subject_kind"],
      initial_state: payload["initial_state"],
      terminal_states: payload["terminal_states"] || [],
      transitions: Enum.map(payload["transitions"] || [], &deserialize_transition/1)
    }
  end

  defp serialize_transition(transition) do
    %{
      "from" => serialize_identifier(transition.from),
      "to" => serialize_identifier(transition.to),
      "trigger" => serialize_trigger(transition.trigger),
      "guard" => serialize_guard(transition[:guard])
    }
  end

  defp deserialize_transition(payload) do
    transition = %{
      from: payload["from"],
      to: payload["to"],
      trigger: deserialize_trigger(payload["trigger"])
    }

    case deserialize_guard(payload["guard"]) do
      nil -> transition
      guard -> Map.put(transition, :guard, guard)
    end
  end

  defp serialize_execution_recipe(%ExecutionRecipeSpec{} = spec) do
    %{
      "recipe_ref" => serialize_identifier(spec.recipe_ref),
      "description" => spec.description,
      "runtime_class" => Atom.to_string(spec.runtime_class),
      "placement_ref" => serialize_identifier(spec.placement_ref),
      "required_lifecycle_hints" =>
        Enum.map(spec.required_lifecycle_hints, &serialize_identifier/1),
      "grant_spec" => serialize_map(spec.grant_spec),
      "retry_config" => serialize_map(spec.retry_config),
      "workspace_policy" => serialize_map(spec.workspace_policy),
      "execution_params" => serialize_map(spec.execution_params),
      "applicable_to" => Enum.map(spec.applicable_to, &serialize_identifier/1)
    }
  end

  defp deserialize_execution_recipe(payload) do
    %ExecutionRecipeSpec{
      recipe_ref: payload["recipe_ref"],
      description: payload["description"],
      runtime_class:
        deserialize_atom(payload["runtime_class"], [
          :session,
          :workflow,
          :playbook,
          :scan,
          :inference
        ]),
      placement_ref: payload["placement_ref"],
      required_lifecycle_hints: payload["required_lifecycle_hints"] || [],
      grant_spec: deserialize_map(payload["grant_spec"] || %{}),
      retry_config: deserialize_retry_config(payload["retry_config"] || %{}),
      workspace_policy: deserialize_workspace_policy(payload["workspace_policy"] || %{}),
      execution_params: deserialize_map(payload["execution_params"] || %{}),
      applicable_to: payload["applicable_to"] || []
    }
  end

  defp serialize_decision_spec(%DecisionSpec{} = spec) do
    %{
      "decision_kind" => serialize_identifier(spec.decision_kind),
      "description" => spec.description,
      "trigger" => serialize_decision_trigger(spec.trigger),
      "required_evidence_kinds" =>
        Enum.map(spec.required_evidence_kinds, &serialize_identifier/1),
      "authorized_actors" => Enum.map(spec.authorized_actors, &serialize_identifier/1),
      "allowed_decisions" => Enum.map(spec.allowed_decisions, &Atom.to_string/1),
      "required_within_hours" => spec.required_within_hours
    }
  end

  defp deserialize_decision_spec(payload) do
    %DecisionSpec{
      decision_kind: payload["decision_kind"],
      description: payload["description"],
      trigger: deserialize_decision_trigger(payload["trigger"]),
      required_evidence_kinds: payload["required_evidence_kinds"] || [],
      authorized_actors: payload["authorized_actors"] || [],
      allowed_decisions:
        Enum.map(
          payload["allowed_decisions"] || [],
          &deserialize_atom(&1, [:accept, :reject, :waive, :expired])
        ),
      required_within_hours: payload["required_within_hours"]
    }
  end

  defp serialize_evidence_spec(%EvidenceSpec{} = spec) do
    %{
      "evidence_kind" => serialize_identifier(spec.evidence_kind),
      "description" => spec.description,
      "collector_ref" => serialize_identifier(spec.collector_ref),
      "collection_strategy" => Atom.to_string(spec.collection_strategy),
      "collected_on" => serialize_evidence_trigger(spec.collected_on),
      "schema" => serialize_nullable_map(spec.schema)
    }
  end

  defp deserialize_evidence_spec(payload) do
    %EvidenceSpec{
      evidence_kind: payload["evidence_kind"],
      description: payload["description"],
      collector_ref: payload["collector_ref"],
      collection_strategy:
        deserialize_atom(payload["collection_strategy"], [:automatic, :manual, :on_demand]),
      collected_on: deserialize_evidence_trigger(payload["collected_on"]),
      schema: deserialize_nullable_map(payload["schema"])
    }
  end

  defp serialize_operator_action(%OperatorActionSpec{} = spec) do
    %{
      "action_kind" => serialize_identifier(spec.action_kind),
      "description" => spec.description,
      "applicable_states" => Enum.map(spec.applicable_states, &serialize_identifier/1),
      "authorized_roles" => Enum.map(spec.authorized_roles, &serialize_identifier/1),
      "effect" => serialize_operator_effect(spec.effect)
    }
  end

  defp deserialize_operator_action(payload) do
    %OperatorActionSpec{
      action_kind: payload["action_kind"],
      description: payload["description"],
      applicable_states: payload["applicable_states"] || [],
      authorized_roles: payload["authorized_roles"] || [],
      effect: deserialize_operator_effect(payload["effect"])
    }
  end

  defp serialize_projection_spec(%ProjectionSpec{} = spec) do
    %{
      "name" => serialize_identifier(spec.name),
      "description" => spec.description,
      "subject_kinds" => Enum.map(spec.subject_kinds, &serialize_identifier/1),
      "default_filters" => serialize_map(spec.default_filters),
      "sort" =>
        Enum.map(spec.sort, fn {field, dir} ->
          %{"field" => serialize_identifier(field), "dir" => Atom.to_string(dir)}
        end),
      "included_fields" => serialize_included_fields(spec.included_fields)
    }
  end

  defp deserialize_projection_spec(payload) do
    %ProjectionSpec{
      name: payload["name"],
      description: payload["description"],
      subject_kinds: payload["subject_kinds"] || [],
      default_filters: deserialize_map(payload["default_filters"] || %{}),
      sort:
        Enum.map(payload["sort"] || [], fn entry ->
          {deserialize_projection_field(entry["field"]),
           deserialize_atom(entry["dir"], [:asc, :desc])}
        end),
      included_fields: deserialize_included_fields(payload["included_fields"])
    }
  end

  defp serialize_guard(nil), do: nil

  defp serialize_guard(%{module: module, function: function}) do
    %{"module" => serialize_module(module), "function" => Atom.to_string(function)}
  end

  defp deserialize_guard(nil), do: nil

  defp deserialize_guard(payload) do
    %{
      module: deserialize_module(payload["module"]),
      function: String.to_existing_atom(payload["function"])
    }
  end

  defp serialize_trigger(:auto), do: %{"kind" => "auto"}

  defp serialize_trigger({:execution_requested, recipe_ref}),
    do: %{"kind" => "execution_requested", "recipe_ref" => serialize_identifier(recipe_ref)}

  defp serialize_trigger({:execution_completed, recipe_ref}),
    do: %{"kind" => "execution_completed", "recipe_ref" => serialize_identifier(recipe_ref)}

  defp serialize_trigger({:execution_failed, recipe_ref}),
    do: %{"kind" => "execution_failed", "recipe_ref" => serialize_identifier(recipe_ref)}

  defp serialize_trigger({:execution_failed, recipe_ref, failure_kind}) do
    %{
      "kind" => "execution_failed",
      "recipe_ref" => serialize_identifier(recipe_ref),
      "failure_kind" => Atom.to_string(failure_kind)
    }
  end

  defp serialize_trigger({:join_completed, join_step_ref}) do
    %{
      "kind" => "join_completed",
      "join_step_ref" => serialize_identifier(join_step_ref)
    }
  end

  defp serialize_trigger({:decision_made, decision_kind, decision_value}) do
    %{
      "kind" => "decision_made",
      "decision_kind" => serialize_identifier(decision_kind),
      "decision_value" => Atom.to_string(decision_value)
    }
  end

  defp serialize_trigger({:operator_action, action_kind}) do
    %{"kind" => "operator_action", "action_kind" => serialize_identifier(action_kind)}
  end

  defp serialize_trigger({:subject_entered_state, state}) do
    %{"kind" => "subject_entered_state", "state" => serialize_identifier(state)}
  end

  defp deserialize_trigger(%{"kind" => "auto"}), do: :auto

  defp deserialize_trigger(%{"kind" => "execution_requested", "recipe_ref" => recipe_ref}),
    do: {:execution_requested, recipe_ref}

  defp deserialize_trigger(%{"kind" => "execution_completed", "recipe_ref" => recipe_ref}),
    do: {:execution_completed, recipe_ref}

  defp deserialize_trigger(%{
         "kind" => "execution_failed",
         "recipe_ref" => recipe_ref,
         "failure_kind" => failure_kind
       }) do
    {:execution_failed, recipe_ref, deserialize_runtime_failure_kind(failure_kind)}
  end

  defp deserialize_trigger(%{"kind" => "execution_failed", "recipe_ref" => recipe_ref}) do
    {:execution_failed, recipe_ref}
  end

  defp deserialize_trigger(%{"kind" => "join_completed", "join_step_ref" => join_step_ref}) do
    {:join_completed, join_step_ref}
  end

  defp deserialize_trigger(%{
         "kind" => "decision_made",
         "decision_kind" => decision_kind,
         "decision_value" => decision_value
       }) do
    {:decision_made, decision_kind,
     deserialize_atom(decision_value, [:accept, :reject, :waive, :expired])}
  end

  defp deserialize_trigger(%{"kind" => "operator_action", "action_kind" => action_kind}) do
    {:operator_action, action_kind}
  end

  defp deserialize_trigger(%{"kind" => "subject_entered_state", "state" => state}) do
    {:subject_entered_state, state}
  end

  defp serialize_decision_trigger({:after_execution_completed, recipe_ref}) do
    %{"kind" => "after_execution_completed", "recipe_ref" => serialize_identifier(recipe_ref)}
  end

  defp serialize_decision_trigger({:after_decision, decision_kind, decision_value}) do
    %{
      "kind" => "after_decision",
      "decision_kind" => serialize_identifier(decision_kind),
      "decision_value" => Atom.to_string(decision_value)
    }
  end

  defp serialize_decision_trigger({:on_subject_entered_state, state}) do
    %{"kind" => "on_subject_entered_state", "state" => serialize_identifier(state)}
  end

  defp deserialize_decision_trigger(%{
         "kind" => "after_execution_completed",
         "recipe_ref" => recipe_ref
       }) do
    {:after_execution_completed, recipe_ref}
  end

  defp deserialize_decision_trigger(%{
         "kind" => "after_decision",
         "decision_kind" => decision_kind,
         "decision_value" => decision_value
       }) do
    {:after_decision, decision_kind,
     deserialize_atom(decision_value, [:accept, :reject, :waive, :expired])}
  end

  defp deserialize_decision_trigger(%{"kind" => "on_subject_entered_state", "state" => state}) do
    {:on_subject_entered_state, state}
  end

  defp serialize_evidence_trigger({:execution_completed, recipe_ref}) do
    %{"kind" => "execution_completed", "recipe_ref" => serialize_identifier(recipe_ref)}
  end

  defp serialize_evidence_trigger({:decision_created, decision_kind}) do
    %{"kind" => "decision_created", "decision_kind" => serialize_identifier(decision_kind)}
  end

  defp serialize_evidence_trigger({:subject_entered_state, state}) do
    %{"kind" => "subject_entered_state", "state" => serialize_identifier(state)}
  end

  defp deserialize_evidence_trigger(%{
         "kind" => "execution_completed",
         "recipe_ref" => recipe_ref
       }) do
    {:execution_completed, recipe_ref}
  end

  defp deserialize_evidence_trigger(%{
         "kind" => "decision_created",
         "decision_kind" => decision_kind
       }) do
    {:decision_created, decision_kind}
  end

  defp deserialize_evidence_trigger(%{"kind" => "subject_entered_state", "state" => state}) do
    {:subject_entered_state, state}
  end

  defp serialize_operator_effect({:advance_lifecycle, state}) do
    %{"kind" => "advance_lifecycle", "state" => serialize_identifier(state)}
  end

  defp serialize_operator_effect(:block_subject), do: %{"kind" => "block_subject"}
  defp serialize_operator_effect(:unblock_subject), do: %{"kind" => "unblock_subject"}

  defp serialize_operator_effect({:dispatch_effect, effect_kind}) do
    %{"kind" => "dispatch_effect", "effect_kind" => serialize_identifier(effect_kind)}
  end

  defp serialize_operator_effect(:cancel_active_execution),
    do: %{"kind" => "cancel_active_execution"}

  defp serialize_operator_effect({:collect_evidence, evidence_kind}) do
    %{"kind" => "collect_evidence", "evidence_kind" => serialize_identifier(evidence_kind)}
  end

  defp deserialize_operator_effect(%{"kind" => "advance_lifecycle", "state" => state}) do
    {:advance_lifecycle, state}
  end

  defp deserialize_operator_effect(%{"kind" => "block_subject"}), do: :block_subject
  defp deserialize_operator_effect(%{"kind" => "unblock_subject"}), do: :unblock_subject

  defp deserialize_operator_effect(%{"kind" => "dispatch_effect", "effect_kind" => effect_kind}) do
    {:dispatch_effect, effect_kind}
  end

  defp deserialize_operator_effect(%{"kind" => "cancel_active_execution"}),
    do: :cancel_active_execution

  defp deserialize_operator_effect(%{
         "kind" => "collect_evidence",
         "evidence_kind" => evidence_kind
       }) do
    {:collect_evidence, evidence_kind}
  end

  defp serialize_identifier(value) when is_atom(value), do: Atom.to_string(value)
  defp serialize_identifier(value) when is_binary(value), do: value

  defp serialize_module(nil), do: nil
  defp serialize_module(module) when is_atom(module), do: Atom.to_string(module)

  defp deserialize_module(nil), do: nil

  defp deserialize_module(module_name) when is_binary(module_name) do
    String.to_existing_atom(module_name)
  rescue
    ArgumentError -> nil
  end

  defp serialize_map(map) when is_map(map) do
    Map.new(map, fn {key, value} ->
      {serialize_map_key(key), serialize_value(value)}
    end)
  end

  defp serialize_nullable_map(nil), do: nil
  defp serialize_nullable_map(map), do: serialize_map(map)

  defp deserialize_map(map) when is_map(map) do
    Map.new(map, fn {key, value} ->
      {deserialize_map_key(key), deserialize_value(value)}
    end)
  end

  defp deserialize_nullable_map(nil), do: nil
  defp deserialize_nullable_map(map), do: deserialize_map(map)

  defp serialize_value(value) when is_map(value), do: serialize_map(value)
  defp serialize_value(value) when is_list(value), do: Enum.map(value, &serialize_value/1)
  defp serialize_value(value) when is_boolean(value) or is_nil(value), do: value
  defp serialize_value(value) when is_atom(value), do: Atom.to_string(value)
  defp serialize_value(value), do: value

  defp deserialize_value(value) when is_map(value), do: deserialize_map(value)
  defp deserialize_value(value) when is_list(value), do: Enum.map(value, &deserialize_value/1)
  defp deserialize_value(value), do: value

  defp serialize_map_key(key) when is_atom(key), do: Atom.to_string(key)
  defp serialize_map_key(key), do: key

  defp deserialize_map_key(key) when is_binary(key) do
    String.to_existing_atom(key)
  rescue
    ArgumentError -> key
  end

  defp deserialize_map_key(key), do: key

  defp serialize_included_fields(:all), do: "all"

  defp serialize_included_fields(fields) when is_list(fields),
    do: Enum.map(fields, &serialize_identifier/1)

  defp deserialize_included_fields("all"), do: :all

  defp deserialize_included_fields(fields) when is_list(fields) do
    Enum.map(fields, &deserialize_projection_field/1)
  end

  defp deserialize_projection_field(field) when is_binary(field), do: field
  defp deserialize_projection_field(field) when is_atom(field), do: Atom.to_string(field)

  defp deserialize_atom(value, allowed_atoms) when is_binary(value) do
    atom = String.to_existing_atom(value)

    if atom in allowed_atoms do
      atom
    else
      raise ArgumentError, "unexpected atom value: #{value}"
    end
  end

  defp deserialize_runtime_failure_kind(value) do
    deserialize_atom(value, [
      :transient_failure,
      :timeout,
      :infrastructure_error,
      :auth_error,
      :semantic_failure,
      :fatal_error
    ])
  end

  defp deserialize_retry_config(payload) do
    payload
    |> deserialize_map()
    |> Map.update(:backoff, :exponential, &deserialize_atom(&1, [:linear, :exponential]))
    |> Map.update(:retry_on, [], fn retry_on ->
      Enum.map(retry_on, &deserialize_runtime_failure_kind/1)
    end)
    |> Map.update(:rekey_on, [], fn rekey_on ->
      Enum.map(rekey_on, &deserialize_runtime_failure_kind/1)
    end)
  end

  defp deserialize_workspace_policy(payload) do
    payload
    |> deserialize_map()
    |> Map.update(
      :strategy,
      :per_subject,
      &deserialize_atom(&1, [:per_subject, :per_execution, :shared, :none])
    )
    |> Map.update(:cleanup, nil, fn
      nil -> nil
      cleanup -> deserialize_atom(cleanup, [:on_completion, :on_terminal, :never])
    end)
    |> Map.update(:reuse, nil, &deserialize_boolean_like/1)
  end

  defp deserialize_boolean_like(value) when is_boolean(value), do: value
  defp deserialize_boolean_like("true"), do: true
  defp deserialize_boolean_like("false"), do: false
  defp deserialize_boolean_like(value), do: value

  defp build_manifest(payload) do
    %Manifest{
      pack_slug: Map.fetch!(payload, "pack_slug"),
      version: Map.fetch!(payload, "version"),
      description: Map.get(payload, "description"),
      migration_strategy:
        payload
        |> Map.fetch!("migration_strategy")
        |> deserialize_atom([:additive, :force]),
      max_supersession_depth: Map.get(payload, "max_supersession_depth", 8),
      subject_kind_specs:
        deserialize_manifest_entries(payload, "subject_kind_specs", &deserialize_subject_kind/1),
      source_kind_specs:
        deserialize_manifest_entries(payload, "source_kind_specs", &deserialize_source_kind/1),
      context_source_specs:
        deserialize_manifest_entries(
          payload,
          "context_source_specs",
          &deserialize_context_source/1
        ),
      lifecycle_specs:
        deserialize_manifest_entries(payload, "lifecycle_specs", &deserialize_lifecycle/1),
      execution_recipe_specs:
        deserialize_manifest_entries(
          payload,
          "execution_recipe_specs",
          &deserialize_execution_recipe/1
        ),
      decision_specs:
        deserialize_manifest_entries(payload, "decision_specs", &deserialize_decision_spec/1),
      evidence_specs:
        deserialize_manifest_entries(payload, "evidence_specs", &deserialize_evidence_spec/1),
      operator_action_specs:
        deserialize_manifest_entries(
          payload,
          "operator_action_specs",
          &deserialize_operator_action/1
        ),
      projection_specs:
        deserialize_manifest_entries(payload, "projection_specs", &deserialize_projection_spec/1)
    }
  end

  defp deserialize_manifest_entries(payload, key, fun) do
    payload
    |> Map.get(key, [])
    |> Enum.map(fun)
  end
end
