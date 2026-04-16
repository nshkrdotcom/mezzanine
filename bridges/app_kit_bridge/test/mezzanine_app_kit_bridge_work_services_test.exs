defmodule Mezzanine.AppKitBridge.WorkServicesTest do
  use ExUnit.Case, async: false

  alias AppKit.Core.{RequestContext, RunRef, RunRequest}
  alias Ecto.Adapters.SQL.Sandbox
  alias Mezzanine.AppKitBridge.{ReviewQueryService, WorkControlService, WorkQueryService}
  alias Mezzanine.OpsDomain.Repo
  alias Mezzanine.Programs.{PolicyBundle, Program}
  alias Mezzanine.Work.{WorkClass, WorkObject}

  setup do
    pid = Sandbox.start_owner!(Repo, shared: false)
    on_exit(fn -> Sandbox.stop_owner(pid) end)
    :ok
  end

  test "work query service exposes adapter-shaped subject reads without relying on the deprecated work surface" do
    %{tenant_id: tenant_id, program: program, work_class: work_class} =
      fixture_stack("tenant-bridge-work")

    assert {:ok, first_subject} =
             WorkQueryService.ingest_subject(%{
               tenant_id: tenant_id,
               program_id: program.id,
               work_class_id: work_class.id,
               external_ref: "linear:ENG-401",
               title: "Bridge work item",
               payload: %{"issue_id" => "ENG-401"},
               source_kind: "linear"
             })

    assert {:ok, second_subject} =
             WorkQueryService.ingest_subject(%{
               tenant_id: tenant_id,
               program_id: program.id,
               work_class_id: work_class.id,
               external_ref: "linear:ENG-401",
               title: "Bridge work item updated",
               payload: %{"issue_id" => "ENG-401", "state" => "updated"},
               source_kind: "linear"
             })

    assert first_subject.subject_id == second_subject.subject_id
    assert second_subject.title == "Bridge work item updated"
    assert second_subject.subject_kind == :work_object
    assert second_subject.program_id == program.id

    assert {:ok, subjects} = WorkQueryService.list_subjects(tenant_id, program.id, %{})
    assert Enum.any?(subjects, &(&1.subject_id == second_subject.subject_id))

    assert {:ok, detail} =
             WorkQueryService.get_subject_detail(tenant_id, second_subject.subject_id)

    assert detail.subject_id == second_subject.subject_id
    assert detail.subject_kind == :work_object
    assert detail.title == "Bridge work item updated"
    assert is_map(detail.gate_status)
    assert is_list(detail.pending_review_ids)

    assert {:ok, projection} =
             WorkQueryService.get_subject_projection(tenant_id, second_subject.subject_id)

    assert projection.subject_id == second_subject.subject_id
    assert projection.subject_kind == :work_object
    assert projection.work_status == :planned

    assert {:ok, stats} = WorkQueryService.queue_stats(tenant_id, program.id)
    assert stats.program_id == program.id
    assert stats.active_count >= 1
  end

  test "work control service returns the same app-kit compatible run result through the extracted service layer" do
    %{tenant_id: tenant_id, program: program, work_class: work_class} =
      fixture_stack("tenant-bridge-start-run")

    assert {:ok, result} =
             WorkControlService.start_run(
               %{
                 route_name: "operator.dispatch",
                 title: "Dispatch operator task",
                 payload: %{"issue_id" => "ENG-501"}
               },
               tenant_id: tenant_id,
               program_id: program.id,
               work_class_id: work_class.id,
               scope_id: "program/#{program.id}"
             )

    assert result.surface == :work_control
    assert result.state == :waiting_review
    assert %RunRef{} = result.payload.run_ref
    assert result.payload.run_ref.metadata.tenant_id == tenant_id
    assert is_binary(result.payload.work_object_id)
    assert is_binary(result.payload.plan_id)
    assert result.payload.review_required == true
  end

  test "typed work-control start_run persists a run and pending review unit for an existing subject" do
    %{tenant_id: tenant_id, program: program, work_class: work_class} =
      fixture_stack("tenant-bridge-typed-start-run")

    assert {:ok, subject} =
             WorkQueryService.ingest_subject(%{
               tenant_id: tenant_id,
               program_id: program.id,
               work_class_id: work_class.id,
               external_ref: "linear:ENG-601",
               title: "Typed start-run subject",
               payload: %{"issue_id" => "ENG-601"},
               source_kind: "linear"
             })

    context = request_context(tenant_id, program.id, work_class.id)

    assert {:ok, run_request} =
             RunRequest.new(%{
               subject_ref: %{id: subject.subject_id, subject_kind: "work_object"},
               recipe_ref: "triage_ticket",
               params: %{"priority" => "high"}
             })

    assert {:ok, result} = WorkControlService.start_run(context, run_request, [])

    assert result.surface == :work_control
    assert result.state == :waiting_review
    assert result.payload.subject_ref.id == subject.subject_id
    assert is_binary(result.payload.run_ref.run_id)
    assert result.payload.run_ref.metadata.work_object_id == subject.subject_id
    assert result.payload.run_ref.metadata.program_id == program.id
    assert is_binary(result.payload.review_unit_id)

    assert {:ok, detail} = WorkQueryService.get_subject_detail(tenant_id, subject.subject_id)
    assert detail.active_run_id == result.payload.run_ref.run_id
    assert detail.active_run_status == :scheduled
    assert result.payload.review_unit_id in detail.pending_review_ids

    assert {:ok, pending_reviews} = ReviewQueryService.list_pending_reviews(tenant_id, program.id)
    assert Enum.any?(pending_reviews, &(&1.decision_ref.id == result.payload.review_unit_id))
  end

  defp fixture_stack(tenant_id) do
    actor = %{tenant_id: tenant_id}

    {:ok, program} =
      Program.create_program(
        %{
          slug: "bridge-work-#{System.unique_integer([:positive])}",
          name: "Bridge Work Program",
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

  defp request_context(tenant_id, program_id, work_class_id) do
    {:ok, context} =
      RequestContext.new(%{
        trace_id: "trace-work-control-typed-#{System.unique_integer([:positive])}",
        actor_ref: %{id: "ops_lead", kind: :human},
        tenant_ref: %{id: tenant_id},
        metadata: %{program_id: program_id, work_class_id: work_class_id}
      })

    context
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
