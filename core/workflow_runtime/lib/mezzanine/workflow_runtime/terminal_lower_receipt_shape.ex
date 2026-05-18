defmodule Mezzanine.WorkflowRuntime.TerminalLowerReceiptShape do
  @moduledoc """
  Shared terminal lower receipt shape for live workflow and deterministic lower completion.

  The live workflow keeps atom-key receipt maps for existing readback callers.
  Deterministic completion keeps string-key receipt maps because those are stored
  as lower gateway payloads. Normalization makes the two paths comparable.
  """

  @shared_fields [
    "receipt_id",
    "receipt_state",
    "lower_receipt_ref",
    "run_id",
    "attempt_id",
    "lower_event_ref",
    "provider_object_refs",
    "evidence_artifact_refs",
    "artifact_refs",
    "token_totals",
    "token_dedupe",
    "rate_limit",
    "retry",
    "retry_receipts",
    "runtime_events",
    "aitrace",
    "prompt_provenance",
    "memory_context",
    "semantic_failure",
    "provider_account",
    "credential",
    "runtime_profile",
    "governed_lower_envelope",
    "authority_decision",
    "connector_manifests",
    "capability_negotiations",
    "incident_bundles",
    "acceptance",
    "provider_evidence",
    "source_publication",
    "workpad_refs",
    "trace_id",
    "causation_id",
    "idempotency_key"
  ]

  @required_fields [
    "receipt_id",
    "receipt_state",
    "lower_receipt_ref",
    "trace_id",
    "causation_id",
    "idempotency_key"
  ]

  @spec shared_fields() :: [String.t()]
  def shared_fields, do: @shared_fields

  @spec required_fields() :: [String.t()]
  def required_fields, do: @required_fields

  @spec missing_required_fields(map()) :: [String.t()]
  def missing_required_fields(receipt) when is_map(receipt) do
    normalized = normalize(receipt)

    Enum.filter(@required_fields, fn field ->
      value = Map.get(normalized, field)
      value in [nil, "", %{}, []]
    end)
  end

  @spec shared_shape(map()) :: map()
  def shared_shape(receipt) when is_map(receipt) do
    receipt
    |> normalize()
    |> Map.take(@shared_fields)
  end

  @spec normalize(map()) :: map()
  def normalize(map) when is_map(map) do
    Map.new(map, fn {key, value} -> {to_string(key), normalize_value(value)} end)
  end

  @spec from_workflow_signal(map()) :: map()
  def from_workflow_signal(attrs) when is_map(attrs) do
    routing = routing_facts(attrs)

    %{
      receipt_id:
        first_present([map_value(attrs, :signal_id), map_value(attrs, :lower_receipt_ref)]),
      receipt_state:
        first_present([map_value(attrs, :receipt_state), map_value(attrs, :terminal_state)]),
      lower_receipt_ref: map_value(attrs, :lower_receipt_ref),
      run_id: map_value(attrs, :lower_run_ref),
      attempt_id: map_value(attrs, :lower_attempt_ref),
      lower_event_ref: map_value(attrs, :lower_event_ref),
      provider_object_refs: payload_value(attrs, routing, :provider_object_refs, []),
      evidence_artifact_refs: payload_value(attrs, routing, :evidence_artifact_refs, []),
      artifact_refs: payload_value(attrs, routing, :artifact_refs, []),
      token_totals: payload_value(attrs, routing, :token_totals),
      token_dedupe: payload_value(attrs, routing, :token_dedupe),
      rate_limit: payload_value(attrs, routing, :rate_limit),
      retry: payload_value(attrs, routing, :retry),
      retry_receipts: payload_value(attrs, routing, :retry_receipts, []),
      runtime_events: payload_value(attrs, routing, :runtime_events, []),
      aitrace: payload_value(attrs, routing, :aitrace),
      prompt_provenance: payload_value(attrs, routing, :prompt_provenance),
      memory_context: payload_value(attrs, routing, :memory_context),
      semantic_failure: payload_value(attrs, routing, :semantic_failure),
      provider_account: payload_value(attrs, routing, :provider_account),
      credential: payload_value(attrs, routing, :credential),
      runtime_profile: runtime_profile_payload(attrs, routing),
      governed_lower_envelope: lower_envelope_payload(attrs, routing),
      authority_decision: authority_decision_payload(attrs, routing),
      connector_manifests: connector_manifest_payload(attrs, routing),
      capability_negotiations: capability_negotiation_payload(attrs, routing),
      incident_bundles: payload_value(attrs, routing, :incident_bundles, []),
      acceptance: payload_value(attrs, routing, :acceptance),
      provider_evidence: payload_value(attrs, routing, :provider_evidence),
      source_publication: payload_value(attrs, routing, :source_publication),
      workpad_refs: payload_value(attrs, routing, :workpad_refs, []),
      trace_id: map_value(attrs, :trace_id),
      causation_id: map_value(attrs, :correlation_id),
      idempotency_key: map_value(attrs, :idempotency_key)
    }
  end

  @spec from_deterministic_completion(map() | struct(), map(), map(), map()) :: map()
  def from_deterministic_completion(execution, accepted, facts, attrs)
      when is_map(accepted) and is_map(facts) and is_map(attrs) do
    submission_ref = accepted |> map_value(:submission_ref, %{}) |> normalize()
    facts = normalize(facts)
    runtime_events = list_value(facts, "runtime_events")
    lower_event_ref = runtime_events |> List.last() |> map_value(:event_ref)
    lower_receipt_ref = string_value(facts, "lower_receipt_ref")

    %{
      "receipt_id" => lower_receipt_ref,
      "receipt_state" => "succeeded",
      "lower_receipt_ref" => lower_receipt_ref,
      "run_id" => string_value(facts, "run_id") || string_value(submission_ref, "run_id"),
      "attempt_id" =>
        string_value(facts, "attempt_id") || string_value(submission_ref, "attempt_id"),
      "lower_event_ref" => lower_event_ref,
      "provider_object_refs" => list_value(facts, "provider_object_refs"),
      "artifact_refs" => artifact_refs(facts),
      "evidence_artifact_refs" => artifact_refs(facts),
      "runtime_events" => runtime_events,
      "token_totals" => map_value(facts, :token_totals, %{}),
      "token_dedupe" => map_value(facts, :token_dedupe, %{}),
      "rate_limit" => map_value(facts, :rate_limit, %{}),
      "retry" => list_value(facts, "retry"),
      "retry_receipts" => list_value(facts, "retry_receipts"),
      "aitrace" => map_value(facts, :aitrace, %{}),
      "prompt_provenance" => map_value(facts, :prompt_provenance, %{}),
      "memory_context" => map_value(facts, :memory_context, %{}),
      "semantic_failure" => map_value(facts, :semantic_failure),
      "provider_account" => map_value(facts, :provider_account, %{}),
      "credential" => map_value(facts, :credential, %{}),
      "runtime_profile" => map_value(facts, :runtime_profile, %{}),
      "governed_lower_envelope" => map_value(facts, :governed_lower_envelope, %{}),
      "authority_decision" => map_value(facts, :authority_decision, %{}),
      "connector_manifests" => list_value(facts, "connector_manifests"),
      "capability_negotiations" => list_value(facts, "capability_negotiations"),
      "incident_bundles" => list_value(facts, "incident_bundles"),
      "acceptance" => map_value(facts, :acceptance, %{}),
      "provider_evidence" => map_value(facts, :provider_evidence, %{}),
      "source_publication" => map_value(facts, :source_publication, %{}),
      "workpad_refs" => list_value(facts, "workpad_refs"),
      "trace_id" => map_value(execution, :trace_id),
      "causation_id" => map_value(execution, :causation_id),
      "idempotency_key" => map_value(execution, :submission_dedupe_key),
      "ji_submission_key" => string_value(submission_ref, "ji_submission_key"),
      "recorded_by" => "mezzanine_m1_m2_runtime",
      "actor_ref" => actor_ref(attrs)
    }
    |> compact_map()
  end

  defp runtime_profile_payload(attrs, routing) do
    payload_value(attrs, routing, :runtime_profile) ||
      %{
        runtime_profile_ref: payload_value(attrs, routing, :runtime_profile_ref),
        runtime_profile_kind: payload_value(attrs, routing, :runtime_profile_kind)
      }
  end

  defp lower_envelope_payload(attrs, routing) do
    payload_value(attrs, routing, :governed_lower_envelope) ||
      payload_value(attrs, routing, :lower_envelope) ||
      %{
        lower_request_ref: payload_value(attrs, routing, :lower_request_ref),
        lower_runtime_kind: payload_value(attrs, routing, :lower_runtime_kind),
        capability_id:
          payload_value(attrs, routing, :capability_id) ||
            payload_value(attrs, routing, :capability),
        resource_scope_refs: payload_value(attrs, routing, :resource_scope_refs),
        policy_bundle_refs: payload_value(attrs, routing, :policy_bundle_refs),
        script_refs: payload_value(attrs, routing, :script_refs),
        package_refs: payload_value(attrs, routing, :package_refs),
        sandbox_profile_ref: payload_value(attrs, routing, :sandbox_profile_ref),
        attestation_requirement_ref: payload_value(attrs, routing, :attestation_requirement_ref),
        denial_refs: payload_value(attrs, routing, :denial_refs)
      }
  end

  defp authority_decision_payload(attrs, routing) do
    payload_value(attrs, routing, :authority_decision) ||
      %{
        authority_ref:
          payload_value(attrs, routing, :authority_ref) ||
            payload_value(attrs, routing, :permission_decision_ref),
        authority_decision_hash:
          payload_value(attrs, routing, :authority_decision_hash) ||
            payload_value(attrs, routing, :decision_hash)
      }
  end

  defp connector_manifest_payload(attrs, routing) do
    case payload_value(attrs, routing, :connector_manifests) do
      nil ->
        attrs
        |> payload_value(routing, :connector_manifest_refs)
        |> List.wrap()
        |> Enum.map(&%{connector_manifest_ref: &1})

      manifests ->
        manifests
    end
  end

  defp capability_negotiation_payload(attrs, routing) do
    case payload_value(attrs, routing, :capability_negotiations) do
      nil ->
        attrs
        |> payload_value(routing, :capability_negotiation_refs)
        |> List.wrap()
        |> Enum.map(&%{capability_negotiation_ref: &1})

      negotiations ->
        negotiations
    end
  end

  defp payload_value(attrs, routing, key) do
    map_value(routing, key) || map_value(attrs, key)
  end

  defp payload_value(attrs, routing, key, default) do
    payload_value(attrs, routing, key) || default
  end

  defp routing_facts(attrs), do: map_value(attrs, :routing_facts, %{})

  defp first_present(values), do: Enum.find(values, &present?/1)

  defp present?(value), do: value not in [nil, "", %{}, []]

  defp artifact_refs(facts) do
    case list_value(facts, "artifact_refs") do
      [] ->
        facts
        |> list_value("artifact_ref_strings")
        |> Enum.map(&%{"kind" => "artifact", "content_ref" => &1})

      refs ->
        refs
    end
  end

  defp actor_ref(attrs) do
    case map_value(attrs, :actor_ref) do
      %{} = actor -> normalize(actor)
      value when is_binary(value) and value != "" -> %{"kind" => "human", "ref" => value}
      _other -> %{"kind" => "system", "ref" => "deterministic_lower_completion"}
    end
  end

  defp list_value(map, key) do
    case map_value(map, key) do
      nil -> []
      value when is_list(value) -> value
      value -> [value]
    end
  end

  defp map_value(map, key, default), do: map_value(map, key) || default

  defp map_value(%_{} = struct, key), do: struct |> Map.from_struct() |> map_value(key)

  defp map_value(map, key) when is_map(map) and is_atom(key) do
    Map.get(map, key) || Map.get(map, Atom.to_string(key)) || Map.get(map, to_string(key))
  end

  defp map_value(map, key) when is_map(map) and is_binary(key) do
    Map.get(map, key) || map_existing_atom_value(map, key)
  end

  defp map_value(_value, _key), do: nil

  defp map_existing_atom_value(map, key) do
    Enum.find_value(map, fn
      {atom_key, value} when is_atom(atom_key) ->
        if Atom.to_string(atom_key) == key, do: value

      _entry ->
        nil
    end)
  end

  defp string_value(map, key) do
    case map_value(map, key) do
      nil -> nil
      value when is_binary(value) and value in ["", "nil"] -> nil
      value when is_binary(value) and value != "" -> value
      value when is_atom(value) -> Atom.to_string(value)
      value when is_integer(value) -> Integer.to_string(value)
      _other -> nil
    end
  end

  defp normalize_value(%_{} = struct), do: struct |> Map.from_struct() |> normalize_value()
  defp normalize_value(map) when is_map(map), do: normalize(map)
  defp normalize_value(values) when is_list(values), do: Enum.map(values, &normalize_value/1)
  defp normalize_value(nil), do: nil
  defp normalize_value(value) when is_atom(value), do: Atom.to_string(value)
  defp normalize_value(value), do: value

  defp compact_map(map), do: Map.reject(map, fn {_key, value} -> value in [nil, [], %{}] end)
end
