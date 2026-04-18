defmodule Mezzanine.CitadelBridge.RunIntentCompiler do
  @moduledoc """
  Pure lowering from `Mezzanine.Intent.RunIntent` into `Citadel.HostIngress.RunRequest`.
  """

  alias Citadel.HostIngress.RunRequest
  alias Mezzanine.CitadelBridge.PlacementBinder
  alias Mezzanine.Intent.RunIntent

  @spec compile(RunIntent.t(), map()) :: {:ok, RunRequest.t()} | {:error, term()}
  def compile(%RunIntent{} = intent, attrs \\ %{}) when is_map(attrs) do
    %{target: target, constraints: constraints} = PlacementBinder.bind(intent, attrs)

    {:ok,
     RunRequest.new!(%{
       run_request_id: intent.intent_id,
       capability_id: intent.capability,
       objective: objective(intent, attrs),
       subject_selectors: [intent.work_id],
       result_kind: Map.get(attrs, :result_kind, "runtime_submission"),
       scope: %{
         scope_kind: Map.get(attrs, :scope_kind, "work_object"),
         scope_id: intent.work_id,
         workspace_root: value(intent.metadata, :workspace_root, nil),
         environment: Map.get(attrs, :environment),
         preference: Map.get(attrs, :scope_preference, :required)
       },
       target: target,
       constraints: constraints,
       execution: %{
         execution_intent_family: Map.get(attrs, :execution_intent_family, "process"),
         execution_intent: Map.get(attrs, :execution_intent, intent.input),
         allowed_operations: Map.get(attrs, :allowed_operations, [intent.capability]),
         allowed_tools:
           Map.get(attrs, :allowed_tools, value(intent.grant_profile, :allowed_tools, [])),
         effect_classes: Map.get(attrs, :effect_classes, []),
         workspace_mutability: Map.get(attrs, :workspace_mutability, "read_write"),
         placement_intent: Map.get(attrs, :placement_intent, "citadel_host_ingress"),
         downstream_scope: Map.get(attrs, :downstream_scope, "work:#{intent.work_id}"),
         step_id: value(intent.metadata, :step_id, nil)
       },
       risk_hints: Map.get(attrs, :risk_hints, []),
       success_criteria: Map.get(attrs, :success_criteria, []),
       resolution_provenance: %{
         source_kind: "mezzanine.run_intent",
         resolver_kind: value(intent.metadata, :resolver_kind, "mezzanine"),
         resolver_version: value(intent.metadata, :resolver_version, "v1"),
         policy_version: value(intent.metadata, :policy_version, nil),
         confidence: Map.get(attrs, :confidence, 1.0),
         ambiguity_flags: Map.get(attrs, :ambiguity_flags, []),
         raw_input_refs: Map.get(attrs, :raw_input_refs, []),
         raw_input_hashes: Map.get(attrs, :raw_input_hashes, []),
         extensions: %{}
       },
       extensions: request_extensions(intent, attrs)
     })}
  rescue
    error in ArgumentError -> {:error, {:invalid_run_intent, Exception.message(error)}}
  end

  defp objective(intent, attrs) do
    Map.get(attrs, :objective) ||
      value(
        intent.metadata,
        :objective,
        "Execute #{intent.capability} for work #{intent.work_id}"
      )
  end

  defp value(map, key, default) when is_map(map) do
    Map.get(map, key) || Map.get(map, Atom.to_string(key)) || default
  end

  defp request_extensions(%RunIntent{} = intent, attrs) do
    attrs
    |> optional_map(:extensions)
    |> Map.merge(%{
      runtime_class: intent.runtime_class,
      grant_profile: intent.grant_profile,
      metadata: intent.metadata
    })
    |> maybe_put_submission_dedupe_key(attrs)
  end

  defp maybe_put_submission_dedupe_key(extensions, attrs) do
    case value(attrs, :submission_dedupe_key, value(attrs, :idempotency_key, nil)) do
      value when is_binary(value) and value != "" ->
        Map.put(extensions, "submission_dedupe_key", value)

      _other ->
        extensions
    end
  end

  defp optional_map(map, key) when is_map(map) do
    case Map.get(map, key) || Map.get(map, Atom.to_string(key)) do
      %{} = value -> value
      nil -> %{}
      other -> raise ArgumentError, "expected #{inspect(key)} to be a map, got: #{inspect(other)}"
    end
  end
end
