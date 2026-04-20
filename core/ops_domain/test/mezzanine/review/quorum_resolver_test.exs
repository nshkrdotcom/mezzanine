defmodule Mezzanine.Review.QuorumResolverTest do
  use ExUnit.Case, async: true

  alias Mezzanine.Review.{QuorumResolver, ReviewDecision, ReviewUnit}

  @now ~U[2026-04-20 00:00:00Z]

  test "single decision accepts from the first eligible accept input" do
    review_unit = review_unit(%{"required_decisions" => 1})
    decision = decision(:accept, "ops_lead", "decision-1")

    resolution = QuorumResolver.resolve(review_unit, [decision])

    assert resolution.terminal_action == :accept
    assert resolution.quorum_state == :accepted
    assert resolution.accepted_decision_refs == ["decision-1"]
  end

  test "two person quorum stays pending after one accept and accepts after two distinct accepts" do
    review_unit =
      review_unit(%{
        "quorum_mode" => "two_person",
        "required_decisions" => 2,
        "minimum_distinct_actors" => 2
      })

    first = QuorumResolver.resolve(review_unit, [decision(:accept, "ops_a", "decision-1")])

    assert first.terminal_action == nil
    assert first.quorum_state == :pending

    second =
      QuorumResolver.resolve(review_unit, [
        decision(:accept, "ops_a", "decision-1"),
        decision(:accept, "ops_b", "decision-2", 1)
      ])

    assert second.terminal_action == :accept
    assert second.quorum_state == :accepted
    assert second.accepted_actor_refs == ["ops_a", "ops_b"]
  end

  test "role diverse quorum requires both count and required role group coverage" do
    review_unit =
      review_unit(%{
        "quorum_mode" => "role_diverse_m_of_n",
        "required_decisions" => 2,
        "minimum_distinct_actors" => 2,
        "required_role_groups" => ["ops", "security"]
      })

    missing_role =
      QuorumResolver.resolve(review_unit, [
        decision(:accept, "ops_a", "decision-1", 0, %{"role_groups" => ["ops"]}),
        decision(:accept, "ops_b", "decision-2", 1, %{"role_groups" => ["ops"]})
      ])

    assert missing_role.terminal_action == nil
    assert missing_role.quorum_state == :pending

    covered =
      QuorumResolver.resolve(review_unit, [
        decision(:accept, "ops_a", "decision-1", 0, %{"role_groups" => ["ops"]}),
        decision(:accept, "sec_a", "decision-3", 2, %{"role_groups" => ["security"]})
      ])

    assert covered.terminal_action == :accept
    assert covered.quorum_state == :accepted
  end

  test "unanimous quorum uses eligible actor decision rows as inputs" do
    review_unit =
      review_unit(%{
        "quorum_mode" => "unanimous",
        "eligible_actor_refs" => ["ops_a", "ops_b"]
      })

    first = QuorumResolver.resolve(review_unit, [decision(:accept, "ops_a", "decision-1")])

    assert first.terminal_action == nil

    second =
      QuorumResolver.resolve(review_unit, [
        decision(:accept, "ops_a", "decision-1"),
        decision(:accept, "ops_b", "decision-2", 1)
      ])

    assert second.terminal_action == :accept
    assert second.quorum_state == :accepted
  end

  defp review_unit(decision_profile) do
    struct!(ReviewUnit, %{
      id: "11111111-1111-1111-1111-111111111111",
      tenant_id: "tenant-review",
      work_object_id: "22222222-2222-2222-2222-222222222222",
      run_id: "33333333-3333-3333-3333-333333333333",
      review_kind: :operator_review,
      required_by: @now,
      decision_profile: decision_profile,
      reviewer_actor: %{"kind" => "human", "ref" => "ops_lead"},
      status: :pending,
      inserted_at: @now,
      updated_at: @now
    })
  end

  defp decision(decision, actor_ref, id, offset_seconds \\ 0, payload \\ %{}) do
    struct!(ReviewDecision, %{
      id: id,
      tenant_id: "tenant-review",
      review_unit_id: "11111111-1111-1111-1111-111111111111",
      decision: decision,
      actor_kind: :human,
      actor_ref: actor_ref,
      reason: "reviewed",
      payload: payload,
      decided_at: DateTime.add(@now, offset_seconds, :second),
      inserted_at: DateTime.add(@now, offset_seconds, :second),
      updated_at: DateTime.add(@now, offset_seconds, :second)
    })
  end
end
