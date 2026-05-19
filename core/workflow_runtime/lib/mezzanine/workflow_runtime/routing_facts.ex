defmodule Mezzanine.WorkflowRuntime.RoutingFacts do
  @moduledoc """
  Execution-lifecycle routing fact decoder.

  Routing facts are the compact, deterministic data a workflow needs to choose
  governed lower operations. This module owns the string-key compatibility
  contract and rejects unknown facts instead of extending a workflow-wide key
  normalization table.
  """

  alias Mezzanine.WorkflowExecutionLifecycleInput

  @fields [
    :acceptable_attestation,
    :action_id,
    :actor_ref,
    :allowed_operations,
    :allowed_tools,
    :attempt_ref,
    :attestation_requirement_ref,
    :boundary_class,
    :capability,
    :capability_id,
    :capability_negotiation_ref,
    :capability_negotiations,
    :cedar_schema_hash,
    :cedar_schema_ref,
    :connector_manifest_hash,
    :connector_manifest_ref,
    :connector_manifest_refs,
    :connector_manifest_state,
    :connector_manifests,
    :connector_ref,
    :cwd,
    :declared_actions,
    :downstream_scope,
    :evidence_artifact_refs,
    :evidence_profile_ref,
    :execution_id,
    :execution_intent,
    :execution_intent_family,
    :expected_installation_revision,
    :filesystem_policy_ref,
    :governed_lower_envelope,
    :idempotency_class,
    :input_hash,
    :input_ref,
    :installation_id,
    :installation_revision,
    :intent_id,
    :lower_request_ref,
    :lower_runtime_kind,
    :max_turns,
    :network_policy_ref,
    :package_refs,
    :placement_ref,
    :policy_bundle_hash,
    :policy_bundle_ref,
    :policy_bundle_refs,
    :policy_epoch,
    :policy_pack_id,
    :policy_profile_ref,
    :policy_refs,
    :policy_version,
    :provider_evidence,
    :provider_object_refs,
    :rate_limit,
    :redaction_profile_ref,
    :required_evidence,
    :resource_scope_refs,
    :review_required,
    :risk_band,
    :risk_hints,
    :routing_tags,
    :run_ref,
    :runtime_class,
    :runtime_profile,
    :runtime_profile_kind,
    :runtime_profile_ref,
    :sandbox_level,
    :sandbox_profile_ref,
    :scope_kind,
    :script_api_version,
    :script_hash,
    :script_ref,
    :service_id,
    :side_effect_class,
    :source_publication,
    :source_publication_request,
    :source_publish_ref,
    :substrate_trace_id,
    :subject_id,
    :subject_ref,
    :target_id,
    :target_kind,
    :target_ref,
    :terminal_class,
    :workflow_ref,
    :workspace_mutability,
    :workspace_ref,
    :workspace_root
  ]

  @field_set MapSet.new(@fields)
  @key_lookup Map.new(@fields, &{Atom.to_string(&1), &1})
  @routing_atom_lookup %{"session" => :session, "workflow" => :workflow}

  @required_by_operation %{
    compile_citadel_authority: [:installation_revision, :capability, :subject_id],
    submit_jido_lower_run: [:installation_revision, :subject_id],
    publish_source: [:installation_revision, :capability, :subject_id]
  }

  @spec fields() :: [atom()]
  def fields, do: @fields

  @spec decode(WorkflowExecutionLifecycleInput.t() | map() | keyword(), keyword()) ::
          {:ok, map()} | {:error, term()}
  def decode(input_or_facts, opts \\ [])

  def decode(%WorkflowExecutionLifecycleInput{} = input, opts) do
    input.routing_facts
    |> decode(Keyword.put_new(opts, :subject_ref, input.subject_ref))
  end

  def decode(facts, opts) when is_list(facts) do
    facts
    |> Map.new()
    |> decode(opts)
  end

  def decode(facts, opts) when is_map(facts) do
    with {:ok, normalized} <- normalize_facts(facts) do
      {:ok, put_fallback_subject_id(normalized, Keyword.get(opts, :subject_ref))}
    end
  end

  @spec decode!(WorkflowExecutionLifecycleInput.t() | map() | keyword()) :: map()
  def decode!(input_or_facts) do
    case decode(input_or_facts) do
      {:ok, facts} -> facts
      {:error, reason} -> raise ArgumentError, error_message(reason)
    end
  end

  @spec for_operation(WorkflowExecutionLifecycleInput.t() | map() | keyword(), atom()) ::
          {:ok, map()} | {:error, term()}
  def for_operation(input_or_facts, operation) when is_atom(operation) do
    with {:ok, facts} <- decode(input_or_facts),
         [] <- missing_required(facts, Map.get(@required_by_operation, operation, [])) do
      {:ok, facts}
    else
      missing when is_list(missing) ->
        {:error, {:missing_routing_facts, operation, missing}}

      {:error, reason} ->
        {:error, reason}
    end
  end

  @spec atom(map(), atom(), atom()) :: atom()
  def atom(facts, key, default) when is_map(facts) and is_atom(key) and is_atom(default) do
    case Map.get(facts, key, default) do
      value when is_atom(value) -> value
      value when is_binary(value) -> Map.get(@routing_atom_lookup, value, default)
      _other -> default
    end
  end

  @spec installation_id(WorkflowExecutionLifecycleInput.t(), map()) :: term()
  def installation_id(%WorkflowExecutionLifecycleInput{} = input, facts) when is_map(facts) do
    Map.get(facts, :installation_id) || parse_installation_ref(input.installation_ref)
  end

  @spec normalize_known_key(String.t() | atom()) :: {:ok, atom()} | :error
  def normalize_known_key(key) when is_atom(key) do
    if MapSet.member?(@field_set, key), do: {:ok, key}, else: :error
  end

  def normalize_known_key(key) when is_binary(key), do: Map.fetch(@key_lookup, key)

  @spec error_message(term()) :: String.t()
  def error_message({:unknown_routing_fact_keys, keys}),
    do: "unknown execution lifecycle routing facts #{inspect(keys)}"

  def error_message({:missing_routing_facts, operation, keys}),
    do: "missing #{operation} routing facts #{inspect(keys)}"

  def error_message(reason), do: inspect(reason)

  defp normalize_facts(facts) do
    {normalized, unknown} =
      Enum.reduce(facts, {%{}, []}, fn {key, value}, {normalized, unknown} ->
        case normalize_known_key(key) do
          {:ok, normalized_key} -> {Map.put(normalized, normalized_key, value), unknown}
          :error -> {normalized, [to_string(key) | unknown]}
        end
      end)

    case Enum.reverse(unknown) do
      [] -> {:ok, normalized}
      unknown_keys -> {:error, {:unknown_routing_fact_keys, unknown_keys}}
    end
  end

  defp put_fallback_subject_id(facts, subject_ref) do
    case Map.get(facts, :subject_id) do
      nil -> Map.put_new(facts, :subject_id, subject_id(subject_ref))
      "" -> Map.put(facts, :subject_id, subject_id(subject_ref))
      _value -> facts
    end
  end

  defp subject_id(value) when is_binary(value), do: value
  defp subject_id(%{} = map), do: Map.get(map, "id") || Map.get(map, :id)
  defp subject_id(_value), do: nil

  defp missing_required(facts, required) do
    Enum.reject(required, &present?(Map.get(facts, &1)))
  end

  defp present?(value) when is_binary(value), do: String.trim(value) != ""
  defp present?(value) when is_list(value), do: value != []
  defp present?(value) when is_map(value), do: map_size(value) > 0
  defp present?(value), do: not is_nil(value)

  defp parse_installation_ref("installation://" <> rest), do: rest |> String.split("@") |> hd()
  defp parse_installation_ref(value), do: value
end
