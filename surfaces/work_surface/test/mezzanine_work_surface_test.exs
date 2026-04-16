defmodule MezzanineWorkSurfaceTest do
  use ExUnit.Case, async: false

  alias Ecto.Adapters.SQL.Sandbox
  alias Mezzanine.OpsDomain.Repo
  alias Mezzanine.Programs.{PolicyBundle, Program}
  alias Mezzanine.Surfaces.WorkSurface
  alias Mezzanine.Work.{WorkClass, WorkObject}

  setup do
    pid = Sandbox.start_owner!(Repo, shared: false)
    on_exit(fn -> Sandbox.stop_owner(pid) end)
    :ok
  end

  test "ingest_work is idempotent on program plus external_ref and refreshes the current plan" do
    %{tenant_id: tenant_id, program: program, work_class: work_class} = fixture_stack("tenant-ws")

    assert {:ok, first_work} =
             WorkSurface.ingest_work(%{
               tenant_id: tenant_id,
               program_id: program.id,
               work_class_id: work_class.id,
               external_ref: "linear:ENG-101",
               title: "First title",
               payload: %{"issue_id" => "ENG-101"},
               source_kind: "linear"
             })

    assert {:ok, second_work} =
             WorkSurface.ingest_work(%{
               tenant_id: tenant_id,
               program_id: program.id,
               work_class_id: work_class.id,
               external_ref: "linear:ENG-101",
               title: "Updated title",
               payload: %{"issue_id" => "ENG-101", "state" => "updated"},
               source_kind: "linear"
             })

    assert first_work.id == second_work.id
    assert second_work.title == "Updated title"
    assert is_binary(second_work.current_plan_id)
  end

  test "get_work_detail returns assembled plan, review, control, and audit projections" do
    %{tenant_id: tenant_id, program: program, work_class: work_class} =
      fixture_stack("tenant-detail")

    assert {:ok, work_object} =
             WorkSurface.ingest_work(%{
               tenant_id: tenant_id,
               program_id: program.id,
               work_class_id: work_class.id,
               external_ref: "linear:ENG-201",
               title: "Detail work",
               payload: %{"issue_id" => "ENG-201"},
               source_kind: "linear"
             })

    {:ok, _audit} =
      Mezzanine.WorkAudit.record_event(tenant_id, %{
        program_id: program.id,
        work_object_id: work_object.id,
        event_kind: :work_planned,
        actor_kind: :system,
        actor_ref: "planner",
        payload: %{"step" => 1},
        occurred_at: ~U[2026-04-14 20:45:01Z]
      })

    assert {:ok, detail} = WorkSurface.get_work_detail(tenant_id, work_object.id)
    assert detail.work_object.id == work_object.id
    assert detail.current_plan.id == work_object.current_plan_id
    assert is_list(detail.pending_reviews)
    assert is_map(detail.gate_status)
    assert detail.timeline_projection.work_object_id == work_object.id
  end

  test "work_queue_stats and work_status_projection summarize active governed work" do
    %{tenant_id: tenant_id, program: program, work_class: work_class} =
      fixture_stack("tenant-stats")

    assert {:ok, work_object} =
             WorkSurface.ingest_work(%{
               tenant_id: tenant_id,
               program_id: program.id,
               work_class_id: work_class.id,
               external_ref: "linear:ENG-301",
               title: "Stats work",
               payload: %{"issue_id" => "ENG-301"},
               source_kind: "linear"
             })

    assert {:ok, stats} = WorkSurface.work_queue_stats(tenant_id, program.id)
    assert stats.program_id == program.id
    assert stats.active_count >= 1

    assert {:ok, projection} = WorkSurface.work_status_projection(tenant_id, work_object.id)
    assert projection.work_object_id == work_object.id
    assert projection.plan_status == :compiled
    assert projection.review_status in [:pending, :approved, :clear]
  end

  defp fixture_stack(tenant_id) do
    actor = %{tenant_id: tenant_id}

    {:ok, program} =
      Program.create_program(
        %{
          slug: "work-surface-#{System.unique_integer([:positive])}",
          name: "Work Surface Program",
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

    {:ok, _existing_work} =
      WorkObject.ingest(
        %{
          program_id: program.id,
          work_class_id: work_class.id,
          external_ref: "linear:SEED-#{System.unique_integer([:positive])}",
          title: "Seed work",
          description: "Seed active work",
          priority: 50,
          source_kind: "linear",
          payload: %{"issue_id" => "SEED"},
          normalized_payload: %{"issue_id" => "SEED"}
        },
        actor: actor,
        tenant: tenant_id
      )

    %{
      tenant_id: tenant_id,
      actor: actor,
      program: program,
      bundle: bundle,
      work_class: work_class
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
