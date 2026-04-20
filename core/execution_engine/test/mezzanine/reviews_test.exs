defmodule Mezzanine.ReviewsTest do
  use Mezzanine.Execution.DataCase, async: false

  alias Mezzanine.Programs.{PolicyBundle, Program}
  alias Mezzanine.Review.ReviewUnit
  alias Mezzanine.Reviews
  alias Mezzanine.Work.{WorkClass, WorkObject}

  test "record_decision accepts a review unit and reports release readiness" do
    %{tenant_id: tenant_id, program: program, review_unit: review_unit} =
      fixture_stack("tenant-g")

    assert {:ok, [summary]} = Reviews.pending_review_summaries(tenant_id, program.id)
    assert summary.payload.quorum_profile.quorum_mode == "single_decision"
    assert summary.payload.quorum_profile.required_decision_count == 1
    assert summary.payload.quorum_profile.review_unit_id == review_unit.id

    assert {:ok, %{review_unit: accepted_review_unit}} =
             Reviews.record_decision(tenant_id, review_unit.id, %{
               program_id: program.id,
               decision: :accept,
               actor_kind: :human,
               actor_ref: "ops_lead",
               reason: "looks good",
               payload: %{"summary" => "approved"}
             })

    assert accepted_review_unit.status == :accepted
    assert {:ok, true} = Reviews.release_ready?(tenant_id, review_unit.work_object_id)
  end

  test "record_decision keeps two-person review pending until resolver sees two decision inputs" do
    %{tenant_id: tenant_id, program: program, review_unit: review_unit} =
      fixture_stack("tenant-quorum", %{
        "quorum_mode" => "two_person",
        "required_decisions" => 2,
        "minimum_distinct_actors" => 2
      })

    assert {:ok, %{review_unit: pending_review_unit, quorum_resolution: first_resolution}} =
             Reviews.record_decision(tenant_id, review_unit.id, %{
               program_id: program.id,
               decision: :accept,
               actor_kind: :human,
               actor_ref: "ops_a",
               reason: "first approval",
               payload: %{"summary" => "first"}
             })

    assert pending_review_unit.status == :pending
    assert first_resolution.quorum_state == :pending
    assert first_resolution.terminal_action == nil
    assert {:ok, false} = Reviews.release_ready?(tenant_id, review_unit.work_object_id)

    assert {:ok, %{review_unit: accepted_review_unit, quorum_resolution: second_resolution}} =
             Reviews.record_decision(tenant_id, review_unit.id, %{
               program_id: program.id,
               decision: :accept,
               actor_kind: :human,
               actor_ref: "ops_b",
               reason: "second approval",
               payload: %{"summary" => "second"}
             })

    assert accepted_review_unit.status == :accepted
    assert second_resolution.quorum_state == :accepted
    assert second_resolution.terminal_action == :accept
    assert second_resolution.accepted_actor_refs == ["ops_a", "ops_b"]
    assert {:ok, true} = Reviews.release_ready?(tenant_id, review_unit.work_object_id)

    assert {:ok, detail} = Reviews.review_detail(tenant_id, review_unit.id)
    assert Enum.map(detail.decisions, & &1.actor_ref) |> Enum.sort() == ["ops_a", "ops_b"]
  end

  test "record_decision counts one actor once for quorum" do
    %{tenant_id: tenant_id, program: program, review_unit: review_unit} =
      fixture_stack("tenant-one-actor-once", %{
        "quorum_mode" => "two_person",
        "required_decisions" => 2,
        "minimum_distinct_actors" => 2
      })

    assert {:ok, %{review_unit: first_pending}} =
             Reviews.record_decision(tenant_id, review_unit.id, %{
               program_id: program.id,
               decision: :accept,
               actor_kind: :human,
               actor_ref: "ops_a",
               reason: "first approval",
               payload: %{"summary" => "first"}
             })

    assert first_pending.status == :pending

    assert {:ok, %{review_unit: second_pending, quorum_resolution: resolution}} =
             Reviews.record_decision(tenant_id, review_unit.id, %{
               program_id: program.id,
               decision: :accept,
               actor_kind: :human,
               actor_ref: "ops_a",
               reason: "same actor retry",
               payload: %{"summary" => "same actor retry"}
             })

    assert second_pending.status == :pending
    assert resolution.terminal_action == nil
    assert resolution.accepted_actor_refs == ["ops_a"]
    assert resolution.actor_counting.counting_rule == "one_actor_counts_once"
    assert resolution.actor_counting.multi_role_counting_allowed? == false
    assert {:ok, false} = Reviews.release_ready?(tenant_id, review_unit.work_object_id)
  end

  test "waive_review creates a waiver and updates the review unit" do
    %{tenant_id: tenant_id, program: program, review_unit: review_unit} =
      fixture_stack("tenant-h")

    assert {:ok, %{review_unit: waived_review_unit, waiver: waiver}} =
             Reviews.waive_review(tenant_id, review_unit.id, %{
               program_id: program.id,
               actor_ref: "ops_lead",
               reason: "temporary exception",
               conditions: ["follow_up"],
               expires_at: DateTime.add(DateTime.utc_now(), 3_600, :second)
             })

    assert waived_review_unit.status == :waived
    assert waiver.status == :active
  end

  test "escalate_review creates an escalation and keeps release blocked" do
    %{tenant_id: tenant_id, program: program, review_unit: review_unit} =
      fixture_stack("tenant-i")

    assert {:ok, %{review_unit: escalated_review_unit, escalation: escalation}} =
             Reviews.escalate_review(tenant_id, review_unit.id, %{
               program_id: program.id,
               actor_ref: "ops_lead",
               reason: "needs security signoff",
               assigned_to: "security",
               priority: :urgent
             })

    assert escalated_review_unit.status == :escalated
    assert escalation.status == :open
    assert {:ok, false} = Reviews.release_ready?(tenant_id, review_unit.work_object_id)
  end

  defp fixture_stack(tenant_id, decision_profile \\ %{"required_decisions" => 1}) do
    actor = %{tenant_id: tenant_id}

    {:ok, program} =
      Program.create_program(
        %{
          slug: "reviews-#{System.unique_integer([:positive])}",
          name: "Reviews Program",
          product_family: "operator_stack",
          configuration: %{},
          metadata: %{}
        },
        actor: actor,
        tenant: tenant_id
      )

    {:ok, bundle} =
      PolicyBundle.load_bundle(
        %{
          program_id: program.id,
          name: "default",
          version: "1.0.0",
          policy_kind: :workflow_md,
          source_ref: "WORKFLOW.md",
          body: workflow_body(),
          metadata: %{}
        },
        actor: actor,
        tenant: tenant_id
      )

    {:ok, work_class} =
      WorkClass.create_work_class(
        %{
          program_id: program.id,
          name: "coding_task_#{System.unique_integer([:positive])}",
          kind: "coding_task",
          intake_schema: %{"required" => ["title"]},
          policy_bundle_id: bundle.id,
          default_review_profile: %{"required" => true},
          default_run_profile: %{"runtime" => "session"}
        },
        actor: actor,
        tenant: tenant_id
      )

    {:ok, work_object} =
      WorkObject.ingest(
        %{
          program_id: program.id,
          work_class_id: work_class.id,
          external_ref: "linear:ENG-#{System.unique_integer([:positive])}",
          title: "Reviews work",
          description: "Exercise the neutral review service",
          priority: 50,
          source_kind: "linear",
          payload: %{"issue_id" => "ENG-1"},
          normalized_payload: %{"issue_id" => "ENG-1"}
        },
        actor: actor,
        tenant: tenant_id
      )

    {:ok, review_unit} =
      ReviewUnit.create_review_unit(
        %{
          work_object_id: work_object.id,
          review_kind: :operator_review,
          required_by: DateTime.utc_now(),
          decision_profile: decision_profile,
          reviewer_actor: %{"kind" => "human", "ref" => "ops_lead"}
        },
        actor: actor,
        tenant: tenant_id
      )

    %{
      tenant_id: tenant_id,
      actor: actor,
      program: program,
      bundle: bundle,
      work_class: work_class,
      work_object: work_object,
      review_unit: review_unit
    }
  end

  defp workflow_body do
    """
    ---
    tracker:
      kind: linear
      endpoint: https://api.linear.app/graphql
    run:
      profile: default_session
      runtime_class: session
      capability: linear.issue.execute
      target: linear-default
    approval:
      mode: manual
      reviewers:
        - ops_lead
      escalation_required: true
    retry:
      strategy: exponential
      max_attempts: 4
      initial_backoff_ms: 5000
      max_backoff_ms: 300000
    placement:
      profile_id: default-placement
      strategy: affinity
      target_selector:
        runtime_driver: jido_session
      runtime_preferences:
        locality: same_region
    workspace:
      root_mode: per_work
      sandbox_profile: strict
    review:
      required: true
      required_decisions: 1
      gates:
        - operator
    capability_grants:
      - capability_id: linear.issue.read
        mode: allow
      - capability_id: linear.issue.update
        mode: allow
    ---
    # Operator Prompt
    """
  end
end
