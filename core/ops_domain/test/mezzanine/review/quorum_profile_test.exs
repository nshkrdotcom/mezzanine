defmodule Mezzanine.Review.QuorumProfileTest do
  use ExUnit.Case, async: true

  alias Mezzanine.Review.{QuorumProfile, ReviewUnit}

  @now ~U[2026-04-20 00:00:00Z]

  test "defines the required review quorum field set" do
    assert QuorumProfile.required_fields() == [
             :review_quorum_ref,
             :review_unit_id,
             :work_object_id,
             :review_kind,
             :decision_profile_hash,
             :quorum_mode,
             :required_decision_count,
             :eligible_actor_refs,
             :eligible_role_refs,
             :required_role_groups,
             :minimum_distinct_actors,
             :requester_actor_ref,
             :self_approval_allowed?,
             :duplicate_actor_policy,
             :reject_policy,
             :waiver_policy_ref,
             :escalation_policy_ref,
             :decision_idempotency_key,
             :actor_evidence_refs,
             :accepted_decision_refs,
             :rejected_decision_refs,
             :waived_decision_refs,
             :quorum_state,
             :quorum_met?,
             :quorum_result,
             :quorum_evaluated_at,
             :release_manifest_ref
           ]
  end

  test "normalizes default single-decision review profile fields" do
    profile =
      ReviewUnit
      |> review_unit(%{
        decision_profile: %{"required_decisions" => 1},
        reviewer_actor: %{"kind" => "human", "ref" => "ops_lead"},
        status: :pending
      })
      |> QuorumProfile.normalize()

    assert profile.quorum_mode == "single_decision"
    assert profile.required_decision_count == 1
    assert profile.minimum_distinct_actors == 1
    assert profile.eligible_actor_refs == ["ops_lead"]
    assert profile.self_approval_allowed? == false
    assert profile.duplicate_actor_policy == "latest_only"
    assert profile.reject_policy == "any_reject_veto"
    assert profile.quorum_state == "pending"
    assert profile.quorum_met? == false
    assert profile.quorum_result == "pending"
    assert profile.quorum_evaluated_at == DateTime.to_iso8601(@now)
    assert String.starts_with?(profile.review_quorum_ref, "review-quorum:")
    assert String.starts_with?(profile.decision_idempotency_key, "review-decision:")
  end

  test "normalizes two-person and role-diverse quorum fields without closing quorum" do
    profile =
      ReviewUnit
      |> review_unit(%{
        decision_profile: %{
          "quorum_mode" => "two_person",
          "required_decisions" => 2,
          "minimum_distinct_actors" => 2,
          "eligible_actor_refs" => ["ops_lead", "security_lead"],
          "eligible_role_refs" => ["ops", "security"],
          "required_role_groups" => [["ops"], ["security"]],
          "requester_actor_ref" => "submitter",
          "self_approval_allowed" => false,
          "duplicate_actor_policy" => "first_only",
          "reject_policy" => "majority_reject",
          "waiver_policy_ref" => "waiver-policy:security",
          "escalation_policy_ref" => "escalation-policy:security",
          "actor_evidence_refs" => ["actor-evidence:ops_lead"],
          "release_manifest_ref" => "phase5_hardening_metrics[9]"
        },
        reviewer_actor: %{"kind" => "human", "ref" => "ops_lead"},
        status: :accepted
      })
      |> QuorumProfile.normalize()

    assert profile.quorum_mode == "two_person"
    assert profile.required_decision_count == 2
    assert profile.minimum_distinct_actors == 2
    assert profile.eligible_actor_refs == ["ops_lead", "security_lead"]
    assert profile.eligible_role_refs == ["ops", "security"]
    assert profile.required_role_groups == [["ops"], ["security"]]
    assert profile.requester_actor_ref == "submitter"
    assert profile.duplicate_actor_policy == "first_only"
    assert profile.reject_policy == "majority_reject"
    assert profile.waiver_policy_ref == "waiver-policy:security"
    assert profile.escalation_policy_ref == "escalation-policy:security"
    assert profile.actor_evidence_refs == ["actor-evidence:ops_lead"]
    assert profile.quorum_state == "accepted"
    assert profile.quorum_met? == true
    assert profile.quorum_result == "met"
    assert profile.release_manifest_ref == "phase5_hardening_metrics[9]"
  end

  defp review_unit(module, attrs) do
    defaults = %{
      id: "11111111-1111-1111-1111-111111111111",
      tenant_id: "tenant-review",
      work_object_id: "22222222-2222-2222-2222-222222222222",
      run_id: "33333333-3333-3333-3333-333333333333",
      review_kind: :operator_review,
      required_by: @now,
      decision_profile: %{},
      reviewer_actor: %{},
      status: :pending,
      inserted_at: @now,
      updated_at: @now
    }

    struct!(module, Map.merge(defaults, attrs))
  end
end
