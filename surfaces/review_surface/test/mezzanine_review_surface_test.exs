defmodule MezzanineReviewSurfaceTest do
  use ExUnit.Case, async: false

  alias Ecto.Adapters.SQL.Sandbox
  alias Mezzanine.Evidence.{EvidenceBundle, EvidenceItem}
  alias Mezzanine.OpsDomain.Repo
  alias Mezzanine.Programs.{PolicyBundle, Program}
  alias Mezzanine.Review.ReviewUnit
  alias Mezzanine.Runs.{Run, RunArtifact, RunSeries}
  alias Mezzanine.Surfaces.ReviewSurface
  alias Mezzanine.Work.{WorkClass, WorkObject}

  setup do
    pid = Sandbox.start_owner!(Repo, shared: false)
    on_exit(fn -> Sandbox.stop_owner(pid) end)
    :ok
  end

  test "lists pending reviews and assembles review detail" do
    %{tenant_id: tenant_id, program: program, review_unit: review_unit} =
      fixture_stack("tenant-review-detail")

    assert {:ok, listings} = ReviewSurface.list_pending_reviews(tenant_id, program.id)
    assert Enum.any?(listings, &(&1.review_unit_id == review_unit.id))

    assert {:ok, detail} = ReviewSurface.get_review_detail(tenant_id, review_unit.id)
    assert detail.review_unit.id == review_unit.id
    assert length(detail.evidence_items) == 1
    assert length(detail.run_artifacts) == 1
  end

  test "records review decisions, waivers, escalations, and release" do
    %{tenant_id: tenant_id, work_object: work_object, review_unit: review_unit} =
      fixture_stack("tenant-review-actions")

    assert {:ok, %{review_unit: accepted_review}} =
             ReviewSurface.accept_review(
               tenant_id,
               review_unit.id,
               "looks good",
               %{actor_ref: "ops_lead"}
             )

    assert accepted_review.status == :accepted

    assert {:ok, completed_work} =
             ReviewSurface.release_work(tenant_id, work_object.id, %{actor_ref: "ops_lead"})

    assert completed_work.status == :completed

    %{tenant_id: tenant_two, review_unit: review_unit_two} = fixture_stack("tenant-review-waive")

    assert {:ok, %{review_unit: waived_review}} =
             ReviewSurface.waive_review(
               tenant_two,
               review_unit_two.id,
               %{reason: "temporary waiver", conditions: ["manual follow-up"]},
               %{actor_ref: "ops_lead"}
             )

    assert waived_review.status == :waived

    %{tenant_id: tenant_three, review_unit: review_unit_three} =
      fixture_stack("tenant-review-escalate")

    assert {:ok, %{review_unit: escalated_review}} =
             ReviewSurface.escalate_review(
               tenant_three,
               review_unit_three.id,
               %{reason: "needs specialist", assigned_to: "security", priority: :urgent},
               %{actor_ref: "ops_lead"}
             )

    assert escalated_review.status == :escalated
  end

  defp fixture_stack(tenant_id) do
    actor = %{tenant_id: tenant_id}

    {:ok, program} =
      Program.create_program(
        %{
          slug: "review-surface-#{System.unique_integer([:positive])}",
          name: "Review Surface Program",
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
          title: "Review work",
          description: "Exercise review surface",
          priority: 50,
          source_kind: "linear",
          payload: %{"issue_id" => "ENG-1"},
          normalized_payload: %{"issue_id" => "ENG-1"}
        },
        actor: actor,
        tenant: tenant_id
      )

    {:ok, work_object} =
      WorkObject.compile_plan(work_object, %{}, actor: actor, tenant: tenant_id)

    {:ok, run_series} =
      RunSeries.open_series(%{work_object_id: work_object.id}, actor: actor, tenant: tenant_id)

    {:ok, run} =
      Run.schedule(
        %{
          run_series_id: run_series.id,
          attempt: 1,
          runtime_profile: %{"runtime" => "session"},
          grant_profile: %{"linear.issue.update" => "allow"}
        },
        actor: actor,
        tenant: tenant_id
      )

    {:ok, _run_series} =
      RunSeries.attach_current_run(run_series, %{current_run_id: run.id},
        actor: actor,
        tenant: tenant_id
      )

    {:ok, evidence_bundle} =
      EvidenceBundle.assemble(
        %{
          program_id: program.id,
          work_object_id: work_object.id,
          run_id: run.id,
          summary: "bundle ready",
          evidence_manifest: %{},
          completeness_status: %{},
          assembled_at: DateTime.utc_now()
        },
        actor: actor,
        tenant: tenant_id
      )

    {:ok, _evidence_item} =
      EvidenceItem.record_item(
        %{evidence_bundle_id: evidence_bundle.id, kind: :diff, ref: "diff://1", metadata: %{}},
        actor: actor,
        tenant: tenant_id
      )

    {:ok, _run_artifact} =
      RunArtifact.record_artifact(
        %{run_id: run.id, kind: :pr, ref: "https://github.com/example/pr/1", metadata: %{}},
        actor: actor,
        tenant: tenant_id
      )

    {:ok, review_unit} =
      ReviewUnit.create_review_unit(
        %{
          work_object_id: work_object.id,
          run_id: run.id,
          review_kind: :operator_review,
          required_by: DateTime.utc_now(),
          decision_profile: %{"required_decisions" => 1},
          evidence_bundle_id: evidence_bundle.id,
          reviewer_actor: %{"kind" => "human", "ref" => "ops_lead"}
        },
        actor: actor,
        tenant: tenant_id
      )

    {:ok, _audit} =
      Mezzanine.WorkAudit.record_event(tenant_id, %{
        program_id: program.id,
        work_object_id: work_object.id,
        run_id: run.id,
        review_unit_id: review_unit.id,
        event_kind: :review_created,
        actor_kind: :system,
        actor_ref: "planner",
        payload: %{"gate" => "operator"},
        occurred_at: ~U[2026-04-14 21:10:01Z]
      })

    %{
      tenant_id: tenant_id,
      actor: actor,
      program: program,
      work_class: work_class,
      work_object: work_object,
      run: run,
      evidence_bundle: evidence_bundle,
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
