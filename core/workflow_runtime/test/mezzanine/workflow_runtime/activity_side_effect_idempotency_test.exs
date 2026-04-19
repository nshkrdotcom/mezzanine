defmodule Mezzanine.WorkflowRuntime.ActivitySideEffectIdempotencyTest do
  use ExUnit.Case, async: false

  alias Mezzanine.ActivityLeaseBroker
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
             "tenant_ref + submission_dedupe_key"
  end

  test "lower submission and execution side-effect activities use lease broker and idempotency keys" do
    assert {:ok, lower} =
             ActivitySideEffectIdempotency.lower_submission_activity(
               activity_attrs(%{
                 activity_call_ref: "activity://wf-099/lower",
                 lower_submission_ref: "lower-submission-099",
                 submission_dedupe_key: "tenant-alpha:submission-099",
                 idempotency_key: "idem-lower-099",
                 requested_capabilities: ["lower.submit"]
               })
             )

    assert lower.owner_repo == :jido_integration
    assert lower.submission_dedupe_key == "tenant-alpha:submission-099"
    assert lower.idempotency_key == "idem-lower-099"
    assert String.starts_with?(lower.lease_ref, "lease://")

    assert {:ok, execution} =
             ActivitySideEffectIdempotency.execution_side_effect_activity(
               activity_attrs(%{
                 activity_call_ref: "activity://wf-100/execute",
                 intent_id: "intent-100",
                 idempotency_key: "idem-exec-100",
                 requested_capabilities: ["execution.run", "execution.heartbeat"]
               })
             )

    assert execution.owner_repo == :execution_plane
    assert execution.intent_id == "intent-100"
    assert execution.heartbeat_policy == "lease_bound"
    assert String.starts_with?(execution.lease_evidence_ref, "evidence://activity-lease/")
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
        idempotency_key: "idem-099",
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
end
