defmodule Mezzanine.Review.QuorumCloseBehaviorTest do
  use ExUnit.Case, async: true

  alias Mezzanine.Review.{QuorumCloseBehavior, QuorumProfile, ReviewUnit}

  @now ~U[2026-04-20 00:00:00Z]

  test "defines close behavior for every source-visible quorum mode" do
    assert QuorumCloseBehavior.modes() == QuorumProfile.quorum_modes()
    assert Enum.map(QuorumCloseBehavior.all(), & &1.quorum_mode) == QuorumProfile.quorum_modes()

    for behavior <- QuorumCloseBehavior.all() do
      assert Map.keys(behavior) |> Enum.sort() ==
               QuorumCloseBehavior.required_fields() |> Enum.sort()

      assert behavior.close_owner == "review_owner_quorum_resolver"
      assert behavior.close_state_on_accept == "accepted"
      assert behavior.close_state_on_reject == "rejected"
      assert behavior.counting_rule == "one_actor_counts_once"
    end
  end

  test "single decision can close on the first eligible accept" do
    behavior = QuorumCloseBehavior.fetch!("single_decision")

    assert behavior.accept_condition == "first_eligible_accept"
    assert behavior.required_decision_count_rule == "exactly_one"
    assert behavior.minimum_distinct_actors_rule == "exactly_one"
    refute Map.fetch!(behavior, :pending_after_first_accept?)
    refute Map.fetch!(behavior, :fail_closed_when_scope_missing?)
  end

  test "multi-actor modes stay pending after the first eligible accept" do
    for mode <- ~w(m_of_n two_person role_diverse_m_of_n unanimous all_required_roles) do
      behavior = QuorumCloseBehavior.fetch!(mode)

      assert Map.fetch!(behavior, :pending_after_first_accept?)
      assert behavior.accept_condition != "first_eligible_accept"
    end

    assert QuorumCloseBehavior.fetch!("two_person").accept_condition ==
             "at_least_two_distinct_eligible_actors_accept"

    assert QuorumCloseBehavior.fetch!("unanimous").accept_condition ==
             "every_eligible_actor_accepts"
  end

  test "role-aware modes fail closed when actor or role scope is missing" do
    for mode <- ~w(role_diverse_m_of_n unanimous all_required_roles) do
      assert mode
             |> QuorumCloseBehavior.fetch!()
             |> Map.fetch!(:fail_closed_when_scope_missing?)
    end

    assert QuorumCloseBehavior.fetch!("role_diverse_m_of_n").role_scope ==
             "required_role_groups_must_each_have_accepted_actor_evidence"

    assert QuorumCloseBehavior.fetch!("all_required_roles").required_decision_count_rule ==
             "count(required_role_groups)"
  end

  test "quorum profile resolves the specified close behavior for a review unit" do
    review_unit =
      struct!(ReviewUnit, %{
        id: "11111111-1111-1111-1111-111111111111",
        tenant_id: "tenant-review",
        work_object_id: "22222222-2222-2222-2222-222222222222",
        run_id: "33333333-3333-3333-3333-333333333333",
        review_kind: :operator_review,
        required_by: @now,
        decision_profile: %{
          "quorum_mode" => "two_person",
          "required_decisions" => 2,
          "minimum_distinct_actors" => 2
        },
        reviewer_actor: %{"kind" => "human", "ref" => "ops_lead"},
        status: :pending,
        inserted_at: @now,
        updated_at: @now
      })

    assert QuorumProfile.close_behavior(review_unit).quorum_mode == "two_person"

    assert QuorumProfile.close_behavior(review_unit).accept_condition ==
             "at_least_two_distinct_eligible_actors_accept"
  end
end
