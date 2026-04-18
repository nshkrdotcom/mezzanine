defmodule Mezzanine.Citadel.SubstrateIngress.PacketBuilder do
  @moduledoc """
  Builds explicit substrate governance packets from Mezzanine run intents.
  """

  alias Mezzanine.Intent.RunIntent

  @spec packet(RunIntent.t(), map()) :: {:ok, map()} | {:error, term()}
  def packet(%RunIntent{} = intent, attrs \\ %{}) when is_map(attrs) do
    {:ok,
     %{
       tenant_id: required_string(attrs, :tenant_id, metadata_value(intent, :tenant_id)),
       installation_id:
         required_string(attrs, :installation_id, metadata_value(intent, :installation_id)),
       installation_revision: installation_revision!(intent, attrs),
       actor_ref: required_string(attrs, :actor_ref, value(attrs, :actor_id, "mezzanine")),
       subject_id: required_string(attrs, :subject_id, intent.work_id),
       execution_id:
         required_string(attrs, :execution_id, value(attrs, :request_id, intent.intent_id)),
       decision_id: optional_string(attrs, :decision_id),
       request_trace_id:
         required_string(attrs, :request_trace_id, value(attrs, :trace_id, intent.intent_id)),
       substrate_trace_id:
         required_string(attrs, :substrate_trace_id, value(attrs, :trace_id, intent.intent_id)),
       idempotency_key:
         required_string(
           attrs,
           :idempotency_key,
           value(attrs, :submission_dedupe_key, intent.intent_id)
         ),
       capability_refs: [intent.capability],
       policy_refs: policy_refs(intent, attrs),
       run_intent: run_intent_payload(intent),
       placement_constraints: placement_constraints(intent, attrs),
       risk_hints: normalize_string_list(value(attrs, :risk_hints, [])),
       metadata: normalize_metadata(intent, attrs),
       environment:
         required_string(attrs, :environment, metadata_value(intent, :environment, "dev")),
       policy_epoch: optional_non_neg_integer(attrs, :policy_epoch),
       intent_envelope: intent_envelope(intent, attrs)
     }}
  rescue
    error in ArgumentError -> {:error, {:invalid_substrate_packet, Exception.message(error)}}
  end

  defp intent_envelope(%RunIntent{} = intent, attrs) do
    placement = placement(intent, attrs)
    execution_intent_family = value(attrs, :execution_intent_family, "process")
    execution_intent = value(attrs, :execution_intent, intent.input)

    allowed_operations =
      normalize_string_list(value(attrs, :allowed_operations, [intent.capability]))

    submission_key = value(attrs, :submission_dedupe_key, value(attrs, :idempotency_key, nil))

    %{
      intent_envelope_id: "intent/#{required_string(attrs, :execution_id, intent.intent_id)}",
      scope_selectors: [
        %{
          scope_kind: value(attrs, :scope_kind, "work_object"),
          scope_id: required_string(attrs, :subject_id, intent.work_id),
          workspace_root:
            value(attrs, :workspace_root, metadata_value(intent, :workspace_root, nil)),
          environment:
            required_string(attrs, :environment, metadata_value(intent, :environment, "dev")),
          preference: value(attrs, :scope_preference, :required),
          extensions: %{}
        }
      ],
      desired_outcome: %{
        outcome_kind: :invoke_capability,
        requested_capabilities: [intent.capability],
        result_kind: value(attrs, :result_kind, "runtime_submission"),
        subject_selectors: [intent.work_id],
        extensions: %{}
      },
      constraints: %{
        boundary_requirement: value(attrs, :boundary_requirement, :fresh_or_reuse),
        allowed_boundary_classes: [placement.boundary_class],
        allowed_service_ids: [placement.service_id],
        forbidden_service_ids: list_value(attrs, :forbidden_service_ids, []),
        max_steps: value(attrs, :max_steps, 1),
        review_required: value(attrs, :review_required, false),
        extensions: %{}
      },
      risk_hints: risk_hints(attrs),
      success_criteria: success_criteria(attrs),
      target_hints: [
        %{
          target_kind: placement.target_kind,
          preferred_target_id: placement.target_id,
          preferred_service_id: placement.service_id,
          preferred_boundary_class: placement.boundary_class,
          session_mode_preference: placement.session_mode_preference,
          coordination_mode_preference: placement.coordination_mode_preference,
          routing_tags: placement.routing_tags,
          extensions: %{}
        }
      ],
      plan_hints: %{
        candidate_steps: [
          %{
            step_kind: "capability",
            capability_id: intent.capability,
            allowed_operations: allowed_operations,
            extensions: %{
              "citadel" =>
                citadel_step_extensions(
                  intent,
                  attrs,
                  execution_intent_family,
                  execution_intent,
                  submission_key
                )
            }
          }
        ],
        preferred_targets: [],
        preferred_topology: nil,
        budget_hints: nil,
        extensions: %{}
      },
      resolution_provenance: %{
        source_kind: "mezzanine.run_intent",
        resolver_kind: metadata_value(intent, :resolver_kind, "mezzanine"),
        resolver_version: metadata_value(intent, :resolver_version, "substrate-v1"),
        prompt_version: nil,
        policy_version: metadata_value(intent, :policy_version, nil),
        confidence: value(attrs, :confidence, 1.0),
        ambiguity_flags: list_value(attrs, :ambiguity_flags, []),
        raw_input_refs: list_value(attrs, :raw_input_refs, []),
        raw_input_hashes: list_value(attrs, :raw_input_hashes, []),
        extensions: %{}
      },
      extensions: %{"citadel" => %{"ingress_kind" => "substrate_origin"}}
    }
  end

  defp citadel_step_extensions(
         %RunIntent{} = intent,
         attrs,
         execution_intent_family,
         execution_intent,
         submission_key
       ) do
    %{
      "execution_intent_family" => execution_intent_family,
      "execution_intent" => execution_intent,
      "allowed_tools" =>
        list_value(attrs, :allowed_tools, metadata_value(intent, :allowed_tools, [])),
      "effect_classes" => list_value(attrs, :effect_classes, []),
      "workspace_mutability" => value(attrs, :workspace_mutability, "read_write"),
      "placement_intent" => value(attrs, :placement_intent, "remote_workspace"),
      "downstream_scope" => value(attrs, :downstream_scope, "work:#{intent.work_id}")
    }
    |> maybe_put_execution_envelope(submission_key)
  end

  defp maybe_put_execution_envelope(extensions, value) when is_binary(value) and value != "" do
    Map.put(extensions, "execution_envelope", %{"submission_dedupe_key" => value})
  end

  defp maybe_put_execution_envelope(extensions, _value), do: extensions

  defp placement(%RunIntent{} = intent, attrs) do
    placement = Map.new(intent.placement)

    %{
      target_kind: value(attrs, :target_kind, "runtime_target"),
      target_id:
        value(attrs, :target_id, placement_value(placement, :target_id, "default-target")),
      service_id:
        value(attrs, :service_id, placement_value(placement, :service_id, "default-service")),
      boundary_class:
        value(
          attrs,
          :boundary_class,
          placement_value(placement, :boundary_class, "workspace_session")
        ),
      session_mode_preference:
        value(
          attrs,
          :session_mode_preference,
          placement_value(placement, :session_mode_preference, :attached)
        ),
      coordination_mode_preference:
        value(
          attrs,
          :coordination_mode_preference,
          placement_value(placement, :coordination_mode_preference, :single_target)
        ),
      routing_tags:
        list_value(attrs, :routing_tags, placement_value(placement, :routing_tags, ["substrate"]))
    }
  end

  defp placement_constraints(%RunIntent{} = intent, attrs) do
    placement = placement(intent, attrs)

    %{
      "target_kind" => placement.target_kind,
      "placement_ref" => placement.target_id,
      "service_id" => placement.service_id,
      "boundary_class" => placement.boundary_class,
      "session_mode_preference" => to_string(placement.session_mode_preference),
      "coordination_mode_preference" => to_string(placement.coordination_mode_preference)
    }
  end

  defp risk_hints(attrs) do
    attrs
    |> value(:risk_hints, [])
    |> normalize_string_list()
    |> Enum.map(fn risk_code ->
      %{
        risk_code: risk_code,
        severity: :medium,
        requires_governance: false,
        extensions: %{}
      }
    end)
  end

  defp success_criteria(attrs) do
    case value(attrs, :success_criteria, nil) do
      list when is_list(list) and list != [] ->
        list

      _other ->
        [
          %{
            criterion_kind: :completion,
            metric: "lower_submission_accepted",
            target: %{"status" => "accepted"},
            required: true,
            extensions: %{}
          }
        ]
    end
  end

  defp policy_refs(%RunIntent{} = intent, attrs) do
    attrs
    |> value(:policy_refs, [metadata_value(intent, :policy_version, "default")])
    |> normalize_string_list()
  end

  defp run_intent_payload(%RunIntent{} = intent) do
    %{
      "intent_id" => intent.intent_id,
      "program_id" => intent.program_id,
      "work_id" => intent.work_id,
      "capability" => intent.capability,
      "runtime_class" => to_string(intent.runtime_class),
      "input" => intent.input,
      "metadata" => intent.metadata
    }
  end

  defp normalize_metadata(%RunIntent{} = intent, attrs) do
    intent.metadata
    |> stringify_keys()
    |> Map.merge(stringify_keys(value(attrs, :metadata, %{})))
  end

  defp installation_revision!(intent, attrs) do
    value =
      value(
        attrs,
        :installation_revision,
        value(
          attrs,
          :compiled_pack_revision,
          metadata_value(intent, :compiled_pack_revision, nil)
        )
      )

    case value do
      revision when is_integer(revision) and revision >= 0 ->
        revision

      other ->
        raise ArgumentError,
              "substrate packet installation_revision must be a non-negative integer, got: #{inspect(other)}"
    end
  end

  defp required_string(attrs, key, fallback) do
    case value(attrs, key, fallback) do
      string when is_binary(string) and string != "" ->
        string

      other ->
        raise ArgumentError,
              "substrate packet #{key} must be a non-empty string, got: #{inspect(other)}"
    end
  end

  defp optional_string(attrs, key) do
    case value(attrs, key, nil) do
      string when is_binary(string) and string != "" -> string
      _other -> nil
    end
  end

  defp optional_non_neg_integer(attrs, key) do
    case value(attrs, key, nil) do
      integer when is_integer(integer) and integer >= 0 -> integer
      _other -> nil
    end
  end

  defp metadata_value(%RunIntent{} = intent, key, default \\ nil) do
    value(intent.metadata, key, default)
  end

  defp placement_value(placement, key, default) do
    value(placement, key, default)
  end

  defp value(map, key, default) when is_map(map) do
    Map.get(map, key) || Map.get(map, Atom.to_string(key)) || default
  end

  defp list_value(map, key, default) do
    case value(map, key, default) do
      list when is_list(list) -> list
      nil -> []
      item -> [item]
    end
  end

  defp normalize_string_list(list) when is_list(list) do
    list
    |> Enum.flat_map(fn
      item when is_binary(item) and item != "" -> [item]
      item when is_atom(item) -> [Atom.to_string(item)]
      _other -> []
    end)
    |> Enum.uniq()
  end

  defp normalize_string_list(_value), do: []

  defp stringify_keys(map) when is_map(map) do
    Map.new(map, fn {key, value} -> {to_string(key), value} end)
  end
end
