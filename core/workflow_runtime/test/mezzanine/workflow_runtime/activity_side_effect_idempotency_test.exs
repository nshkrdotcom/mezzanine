defmodule Mezzanine.WorkflowRuntime.ActivitySideEffectIdempotencyTest do
  use ExUnit.Case, async: false

  alias Mezzanine.ActivityLeaseBroker
  alias Mezzanine.Idempotency
  alias Mezzanine.WorkflowRuntime.ActivitySideEffectIdempotency

  setup do
    ActivityLeaseBroker.reset_worker_cache!()
    :ok
  end

  test "declares activity versions, idempotency scopes, and owner repos" do
    contract = ActivitySideEffectIdempotency.contract()

    assert contract.temporal_worker_owner == :mezzanine
    assert contract.domain_owners.lower_submission == :jido_integration
    assert contract.domain_owners.execution_side_effect == :execution_plane
    assert contract.domain_owners.semantic_payload_boundary == :outer_brain

    assert contract.activity_versions.lower_submission ==
             "JidoIntegration.LowerSubmissionActivity.v1"

    assert contract.activity_versions.execution_side_effect ==
             "ExecutionPlane.ActivitySideEffectIdempotency.v1"

    assert contract.activity_versions.semantic_payload_boundary ==
             "OuterBrain.SemanticActivityPayloadBoundary.v1"

    assert contract.idempotency_scopes.lower_submission ==
             "canonical root + lower_submission child key"
  end

  test "lower submission and execution side-effect activities use lease broker and canonical roots" do
    canonical_key = canonical_root_key()
    lower_key = Idempotency.child_key!(canonical_key, :lower_submission, "lower-submission-099")

    assert {:ok, lower} =
             ActivitySideEffectIdempotency.lower_submission_activity(
               activity_attrs(%{
                 activity_call_ref: "activity://wf-099/lower",
                 lower_submission_ref: "lower-submission-099",
                 submission_dedupe_key: lower_key,
                 requested_capabilities: ["lower.submit"]
               })
             )

    assert lower.owner_repo == :jido_integration
    assert lower.submission_dedupe_key == lower_key
    assert lower.idempotency_key == canonical_key
    assert lower.idempotency_correlation["canonical_idempotency_key"] == canonical_key
    assert String.starts_with?(lower.lease_ref, "lease://")

    assert {:ok, execution} =
             ActivitySideEffectIdempotency.execution_side_effect_activity(
               activity_attrs(%{
                 activity_call_ref: "activity://wf-100/execute",
                 intent_id: "intent-100",
                 requested_capabilities: ["execution.run", "execution.heartbeat"]
               })
             )

    assert execution.owner_repo == :execution_plane
    assert execution.intent_id == "intent-100"
    assert execution.idempotency_key == canonical_key
    assert execution.idempotency_correlation["canonical_idempotency_key"] == canonical_key
    assert execution.heartbeat_policy == "lease_bound"
    assert String.starts_with?(execution.lease_evidence_ref, "evidence://activity-lease/")
  end

  test "lower side-effect activities reject missing or unrelated canonical roots" do
    assert {:error, {:missing_activity_fields, missing}} =
             activity_attrs(%{lower_submission_ref: "lower-submission-099"})
             |> Map.delete(:canonical_idempotency_key)
             |> ActivitySideEffectIdempotency.lower_submission_activity()

    assert :canonical_idempotency_key in missing

    canonical_key = canonical_root_key()

    assert {:error,
            {:idempotency_correlation_mismatch, :idempotency_key, ^canonical_key,
             "idem-lower-099"}} =
             ActivitySideEffectIdempotency.lower_submission_activity(
               activity_attrs(%{
                 lower_submission_ref: "lower-submission-099",
                 idempotency_key: "idem-lower-099"
               })
             )

    assert {:error, {:missing_activity_fields, missing}} =
             activity_attrs(%{intent_id: "intent-100"})
             |> Map.delete(:canonical_idempotency_key)
             |> ActivitySideEffectIdempotency.execution_side_effect_activity()

    assert :canonical_idempotency_key in missing
  end

  test "activity outputs carry canonical idempotency correlation evidence when rooted" do
    canonical_key = canonical_root_key()
    lower_key = Idempotency.child_key!(canonical_key, :lower_submission, "lower-submission-099")
    activity_key = Idempotency.child_key!(canonical_key, :activity, "activity://wf-099/lower")

    assert {:ok, lower} =
             ActivitySideEffectIdempotency.lower_submission_activity(
               activity_attrs(%{
                 canonical_idempotency_key: canonical_key,
                 causation_id: "cause-099",
                 platform_envelope_idempotency_key: canonical_key,
                 temporal_start_idempotency_key: canonical_key,
                 workflow_id: "workflow-099",
                 workflow_run_id: "run-099",
                 activity_call_ref: "activity://wf-099/lower",
                 activity_attempt_number: 2,
                 lower_submission_ref: "lower-submission-099",
                 submission_dedupe_key: lower_key,
                 idempotency_key: canonical_key,
                 requested_capabilities: ["lower.submit"],
                 release_manifest_ref: "phase5-v7-idempotency-correlation"
               })
             )

    assert lower.idempotency_correlation["canonical_idempotency_key"] == canonical_key
    assert lower.idempotency_correlation["platform_envelope_idempotency_key"] == canonical_key
    assert lower.idempotency_correlation["temporal_workflow_id"] == "workflow-099"
    assert lower.idempotency_correlation["temporal_workflow_run_id"] == "run-099"
    assert lower.idempotency_correlation["temporal_start_idempotency_key"] == canonical_key

    assert lower.idempotency_correlation["temporal_activity_call_ref"] ==
             "activity://wf-099/lower"

    assert lower.idempotency_correlation["temporal_activity_side_effect_key"] == activity_key
    assert lower.idempotency_correlation["temporal_activity_attempt_number"] == 2
    assert lower.idempotency_correlation["jido_lower_activity_idempotency_key"] == canonical_key
    assert lower.idempotency_correlation["jido_lower_submission_dedupe_key"] == lower_key
    assert lower.idempotency_correlation["trace_id"] == "trace-099"
    assert lower.idempotency_correlation["causation_id"] == "cause-099"
    assert lower.idempotency_correlation["tenant_id"] == "tenant-alpha"
  end

  test "semantic workflow history payload keeps routing facts and rejects raw bodies" do
    assert {:ok, payload} =
             ActivitySideEffectIdempotency.semantic_workflow_history_payload(semantic_attrs())

    assert payload.contract_name == "OuterBrain.SemanticActivityPayloadBoundary.v1"
    assert payload.semantic_ref == "semantic-102"
    assert payload.routing_facts.review_required == false
    refute Map.has_key?(payload, :raw_provider_body)

    assert {:error, {:raw_payload_forbidden, :raw_provider_body}} =
             semantic_attrs()
             |> Map.put(:raw_provider_body, %{"message" => "raw"})
             |> ActivitySideEffectIdempotency.semantic_workflow_history_payload()

    assert {:error, :claim_check_only_routing_result} =
             semantic_attrs()
             |> Map.put(:routing_facts, %{})
             |> ActivitySideEffectIdempotency.semantic_workflow_history_payload()
  end

  test "missing routing facts fail before workflow branch execution" do
    assert {:error, {:missing_routing_facts, missing}} =
             semantic_attrs()
             |> update_in([:routing_facts], &Map.delete(&1, :risk_band))
             |> ActivitySideEffectIdempotency.semantic_workflow_history_payload()

    assert :risk_band in missing
  end

  defp activity_attrs(overrides) do
    Map.merge(
      %{
        tenant_ref: "tenant-alpha",
        principal_ref: "principal-operator",
        resource_ref: "resource-work-1",
        workflow_ref: "workflow-099",
        activity_call_ref: "activity://wf-099/activity",
        authority_packet_ref: "authpkt-099",
        permission_decision_ref: "decision-099",
        trace_id: "trace-099",
        causation_id: "cause-099",
        idempotency_key: canonical_root_key(),
        canonical_idempotency_key: canonical_root_key(),
        platform_envelope_idempotency_key: canonical_root_key(),
        temporal_start_idempotency_key: canonical_root_key(),
        policy_revision: "policy-rev-099",
        lease_epoch: 1,
        revocation_epoch: 1,
        activity_type: "lower.execute",
        lower_scope_ref: "lower-scope-099",
        requested_capabilities: ["lower.execute"],
        deadline: "2999-04-18T00:05:00Z"
      },
      overrides
    )
  end

  defp semantic_attrs do
    %{
      semantic_ref: "semantic-102",
      context_hash: "sha256:context-102",
      provenance_refs: ["provenance:normalizer-102"],
      claim_check_refs: ["claim:provider-output-102"],
      validation_state: "coerced_valid",
      diagnostics_ref: "diagnostics:semantic-102",
      routing_facts: %{
        review_required: false,
        semantic_score: 0.87,
        confidence_band: "medium",
        risk_band: "low",
        schema_validation_state: "coerced_valid",
        normalization_warning_count: 1,
        semantic_retry_class: "none",
        terminal_class: "none",
        review_reason_code: "none"
      },
      retry_class: "none",
      terminal_class: "none"
    }
  end

  defp canonical_root_key do
    Idempotency.canonical_key!(%{
      tenant_id: "tenant-alpha",
      installation_id: "installation-main",
      operation_family: "workflow.activity",
      operation_ref: "activity://wf-099/lower",
      causation_id: "cause-099",
      authority_decision_ref: "decision-099",
      subject_ref: "resource-work-1",
      payload_hash: "sha256:payload-099",
      source_event_position: "event:099"
    })
  end
end
