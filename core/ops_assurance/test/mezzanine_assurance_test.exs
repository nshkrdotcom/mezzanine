defmodule Mezzanine.AssuranceTest do
  use ExUnit.Case, async: false

  alias Ecto.Adapters.SQL.Sandbox
  alias Mezzanine.Assurance
  alias Mezzanine.OpsDomain.Repo
  alias Mezzanine.Programs.{PolicyBundle, Program}
  alias Mezzanine.Review.ReviewUnit
  alias Mezzanine.Work.{WorkClass, WorkObject}

  setup do
    pid = Sandbox.start_owner!(Repo, shared: false)
    on_exit(fn -> Sandbox.stop_owner(pid) end)
    :ok
  end

  test "record_decision accepts a review unit and reports release readiness" do
    %{tenant_id: tenant_id, program: program, review_unit: review_unit} =
      fixture_stack("tenant-g")

    assert {:ok, %{review_unit: accepted_review_unit}} =
             Assurance.record_decision(tenant_id, review_unit.id, %{
               program_id: program.id,
               decision: :accept,
               actor_kind: :human,
               actor_ref: "ops_lead",
               reason: "looks good",
               payload: %{"summary" => "approved"}
             })

    assert accepted_review_unit.status == :accepted
    assert {:ok, true} = Assurance.release_ready?(tenant_id, review_unit.work_object_id)
  end

  test "waive_review creates a waiver and updates the review unit" do
    %{tenant_id: tenant_id, program: program, review_unit: review_unit} =
      fixture_stack("tenant-h")

    assert {:ok, %{review_unit: waived_review_unit, waiver: waiver}} =
             Assurance.waive_review(tenant_id, review_unit.id, %{
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
             Assurance.escalate_review(tenant_id, review_unit.id, %{
               program_id: program.id,
               actor_ref: "ops_lead",
               reason: "needs security signoff",
               assigned_to: "security",
               priority: :urgent
             })

    assert escalated_review_unit.status == :escalated
    assert escalation.status == :open
    assert {:ok, false} = Assurance.release_ready?(tenant_id, review_unit.work_object_id)
  end

  defp fixture_stack(tenant_id) do
    actor = %{tenant_id: tenant_id}

    {:ok, program} =
      Program.create_program(
        %{
          slug: "assurance-#{System.unique_integer([:positive])}",
          name: "Assurance Program",
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
          title: "Assurance work",
          description: "Exercise the assurance service",
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
          decision_profile: %{"required_decisions" => 1},
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
