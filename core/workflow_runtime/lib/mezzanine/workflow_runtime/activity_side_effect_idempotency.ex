defmodule Mezzanine.WorkflowRuntime.ActivitySideEffectIdempotency do
  @moduledoc """
  Activity side-effect idempotency and payload-boundary contract for Phase 4 M29.

  Temporalex workers execute these activity wrappers. Owning repositories keep
  their domain truth and expose idempotent domain functions or service endpoints;
  this module carries only workflow-safe refs, routing facts, and authority
  evidence.
  """

  alias Mezzanine.ActivityLeaseBroker
  alias Mezzanine.ActivityLeaseScopeRequest
  alias Mezzanine.Idempotency

  @release_manifest_ref "phase4-v6-milestone29-activity-side-effect-idempotency"
  @activity_versions %{
    lower_submission: "JidoIntegration.LowerSubmissionActivity.v1",
    execution_side_effect: "ExecutionPlane.ActivitySideEffectIdempotency.v1",
    semantic_payload_boundary: "OuterBrain.SemanticActivityPayloadBoundary.v1"
  }

  @required_activity_fields [
    :tenant_ref,
    :resource_ref,
    :workflow_ref,
    :activity_call_ref,
    :authority_packet_ref,
    :permission_decision_ref,
    :trace_id,
    :idempotency_key
  ]

  @lease_required_fields [
    :policy_revision,
    :lease_epoch,
    :revocation_epoch,
    :activity_type,
    :lower_scope_ref,
    :requested_capabilities,
    :deadline
  ]

  @routing_fact_fields [
    :review_required,
    :semantic_score,
    :confidence_band,
    :risk_band,
    :schema_validation_state,
    :normalization_warning_count,
    :semantic_retry_class,
    :terminal_class,
    :review_reason_code
  ]

  @raw_payload_fields [
    :raw_prompt,
    :raw_context_pack,
    :raw_provider_body,
    :raw_provider_payload,
    :provider_native_body,
    :raw_artifact,
    :temporalex_struct,
    :task_token
  ]

  @doc "M29 activity boundary registry consumed by Temporalex worker registration."
  @spec contract() :: map()
  def contract do
    %{
      release_manifest_ref: @release_manifest_ref,
      activity_versions: @activity_versions,
      temporal_worker_owner: :mezzanine,
      domain_owners: %{
        lower_submission: :jido_integration,
        execution_side_effect: :execution_plane,
        semantic_payload_boundary: :outer_brain
      },
      sdk_import_rule:
        "Temporalex imports stay inside Mezzanine workflow runtime modules and tests",
      idempotency_scopes: %{
        lower_submission: "tenant_ref + submission_dedupe_key",
        execution_side_effect: "intent_id + idempotency_key",
        semantic_payload_boundary: "tenant_ref + semantic_ref + idempotency_key"
      },
      activity_input_required_fields: @required_activity_fields,
      lease_required_fields: @lease_required_fields,
      routing_fact_fields: @routing_fact_fields,
      timeout_posture: "start_to_close_timeout and retry policy are explicit per activity",
      heartbeat_posture:
        "long-running execution activities heartbeat through lease-bound metadata",
      history_policy: "compact_refs_hashes_routing_facts_and_diagnostics_only"
    }
  end

  @doc "Build the workflow-safe lower submission activity result after acquiring a lease bundle."
  @spec lower_submission_activity(map() | keyword()) :: {:ok, map()} | {:error, term()}
  def lower_submission_activity(attrs) do
    with {:ok, input} <- activity_input(attrs, @lease_required_fields ++ [:lower_submission_ref]),
         {:ok, request} <- lease_request(input, "lower.execute"),
         {:ok, bundle} <- ActivityLeaseBroker.acquire(request) do
      %{
        contract_name: @activity_versions.lower_submission,
        owner_repo: :jido_integration,
        activity_call_ref: input.activity_call_ref,
        lower_submission_ref: input.lower_submission_ref,
        submission_dedupe_key: Map.get(input, :submission_dedupe_key, input.idempotency_key),
        tenant_ref: input.tenant_ref,
        idempotency_key: input.idempotency_key,
        lease_ref: bundle.lease_ref,
        lease_evidence_ref: bundle.evidence_ref,
        trace_id: input.trace_id,
        retry_policy: "safe_idempotent",
        timeout_policy: "bounded",
        heartbeat_policy: "not_required_for_submission_intake"
      }
      |> maybe_attach_idempotency_correlation(input, %{
        jido_lower_activity_idempotency_key: input.idempotency_key,
        jido_lower_submission_dedupe_key:
          Map.get(input, :submission_dedupe_key, input.idempotency_key),
        lower_submission_stable_ref: input.lower_submission_ref
      })
    end
  end

  @doc "Build the workflow-safe Execution Plane side-effect activity result."
  @spec execution_side_effect_activity(map() | keyword()) :: {:ok, map()} | {:error, term()}
  def execution_side_effect_activity(attrs) do
    with {:ok, input} <- activity_input(attrs, @lease_required_fields ++ [:intent_id]),
         {:ok, request} <- lease_request(input, "execution.side_effect"),
         {:ok, bundle} <- ActivityLeaseBroker.acquire(request) do
      %{
        contract_name: @activity_versions.execution_side_effect,
        owner_repo: :execution_plane,
        activity_call_ref: input.activity_call_ref,
        intent_id: input.intent_id,
        idempotency_key: input.idempotency_key,
        tenant_ref: input.tenant_ref,
        lease_ref: bundle.lease_ref,
        lease_evidence_ref: bundle.evidence_ref,
        trace_id: input.trace_id,
        retry_policy: "safe_idempotent",
        timeout_policy: "bounded",
        heartbeat_policy: "lease_bound"
      }
      |> maybe_attach_idempotency_correlation(input, %{
        execution_plane_intent_id: input.intent_id,
        execution_plane_envelope_idempotency_key:
          Map.get(input, :execution_plane_envelope_idempotency_key),
        execution_plane_route_id: Map.get(input, :execution_plane_route_id),
        execution_plane_route_idempotency_key:
          Map.get(input, :execution_plane_route_idempotency_key)
      })
    end
  end

  @doc """
  Return the only semantic fields allowed to enter workflow history.

  Large bodies remain in claim-check stores. Routing facts must be present when
  workflow branching depends on payload-derived semantic values.
  """
  @spec semantic_workflow_history_payload(map() | keyword()) :: {:ok, map()} | {:error, term()}
  def semantic_workflow_history_payload(attrs) do
    attrs = normalize(attrs)

    with :ok <- reject_raw_payloads(attrs),
         :ok <- reject_claim_check_only_branch(attrs),
         :ok <-
           require_fields(attrs, [
             :semantic_ref,
             :context_hash,
             :provenance_refs,
             :diagnostics_ref
           ]),
         {:ok, routing_facts} <- routing_facts(attrs) do
      {:ok,
       %{
         contract_name: @activity_versions.semantic_payload_boundary,
         semantic_ref: attrs.semantic_ref,
         context_hash: attrs.context_hash,
         provenance_refs: attrs.provenance_refs,
         validation_state: Map.get(attrs, :validation_state, "valid"),
         diagnostics_ref: attrs.diagnostics_ref,
         routing_facts: routing_facts,
         retry_class: Map.get(attrs, :retry_class, "none"),
         terminal_class: Map.get(attrs, :terminal_class, "none")
       }}
    end
  end

  @doc "Validate the common M29 activity input envelope."
  @spec activity_input(map() | keyword(), [atom()]) :: {:ok, map()} | {:error, term()}
  def activity_input(attrs, extra_fields \\ []) do
    attrs = normalize(attrs)

    case missing(attrs, @required_activity_fields ++ extra_fields) do
      [] -> {:ok, attrs}
      missing_fields -> {:error, {:missing_activity_fields, missing_fields}}
    end
  end

  defp lease_request(input, activity_type) do
    ActivityLeaseScopeRequest.new(%{
      tenant_ref: input.tenant_ref,
      principal_ref: Map.get(input, :principal_ref),
      system_actor_ref: Map.get(input, :system_actor_ref, "system:mezzanine-workflow-activity"),
      resource_ref: input.resource_ref,
      resource_path: Map.get(input, :resource_path),
      authority_packet_ref: input.authority_packet_ref,
      permission_decision_ref: input.permission_decision_ref,
      policy_revision: input.policy_revision,
      lease_epoch: input.lease_epoch,
      revocation_epoch: input.revocation_epoch,
      activity_type: Map.get(input, :activity_type, activity_type),
      activity_id: input.activity_call_ref,
      workflow_ref: input.workflow_ref,
      lower_scope_ref: input.lower_scope_ref,
      requested_capabilities: input.requested_capabilities,
      idempotency_key: input.idempotency_key,
      trace_id: input.trace_id,
      deadline: input.deadline
    })
  end

  defp maybe_attach_idempotency_correlation(result, input, extra) do
    case Map.get(input, :canonical_idempotency_key) do
      canonical_key when is_binary(canonical_key) and canonical_key != "" ->
        attrs =
          %{
            canonical_idempotency_key: canonical_key,
            tenant_id: input.tenant_ref,
            trace_id: input.trace_id,
            causation_id: Map.get(input, :causation_id, Map.get(input, :request_id)),
            client_retry_key: Map.get(input, :client_retry_key),
            platform_envelope_idempotency_key: Map.get(input, :platform_envelope_idempotency_key),
            temporal_workflow_id: Map.get(input, :workflow_id, input.workflow_ref),
            temporal_workflow_run_id: Map.get(input, :workflow_run_id),
            temporal_start_idempotency_key: Map.get(input, :temporal_start_idempotency_key),
            temporal_activity_call_ref: input.activity_call_ref,
            temporal_activity_attempt_number: Map.get(input, :activity_attempt_number),
            release_manifest_ref: Map.get(input, :release_manifest_ref),
            idempotency_key: input.idempotency_key
          }
          |> Map.merge(extra)

        case Idempotency.correlation_evidence(attrs) do
          {:ok, correlation} -> {:ok, Map.put(result, :idempotency_correlation, correlation)}
          {:error, reason} -> {:error, reason}
        end

      _missing ->
        {:ok, result}
    end
  end

  defp routing_facts(attrs) do
    facts = Map.get(attrs, :routing_facts, %{})

    cond do
      not is_map(facts) ->
        {:error, {:missing_routing_facts, @routing_fact_fields}}

      missing(facts, @routing_fact_fields) == [] ->
        {:ok, facts}

      true ->
        {:error, {:missing_routing_facts, missing(facts, @routing_fact_fields)}}
    end
  end

  defp reject_claim_check_only_branch(attrs) do
    claim_check_refs = Map.get(attrs, :claim_check_refs, [])
    routing_facts = Map.get(attrs, :routing_facts, %{})

    if claim_check_refs != [] and routing_facts == %{} do
      {:error, :claim_check_only_routing_result}
    else
      :ok
    end
  end

  defp reject_raw_payloads(attrs) do
    case Enum.find(@raw_payload_fields, &Map.has_key?(attrs, &1)) do
      nil -> :ok
      field -> {:error, {:raw_payload_forbidden, field}}
    end
  end

  defp require_fields(attrs, fields) do
    case missing(attrs, fields) do
      [] -> :ok
      missing_fields -> {:error, {:missing_activity_fields, missing_fields}}
    end
  end

  defp missing(attrs, fields), do: Enum.reject(fields, &present?(Map.get(attrs, &1)))

  defp present?(value) when is_binary(value), do: String.trim(value) != ""
  defp present?(value) when is_list(value), do: value != []
  defp present?(value) when is_map(value), do: map_size(value) > 0
  defp present?(value), do: not is_nil(value)

  defp normalize(attrs) when is_list(attrs), do: Map.new(attrs)

  defp normalize(%{__struct__: _} = attrs) do
    attrs
    |> Map.from_struct()
    |> normalize()
  end

  defp normalize(attrs) when is_map(attrs) do
    Map.new(attrs, fn
      {key, value} when is_binary(key) -> {String.to_existing_atom(key), value}
      pair -> pair
    end)
  rescue
    ArgumentError -> attrs
  end
end
