defmodule Mezzanine.AppKitBridgeTest do
  use ExUnit.Case, async: false

  alias AppKit.Core.RunRef
  alias Ecto.Adapters.SQL.Sandbox
  alias Mezzanine.AppKitBridge
  alias Mezzanine.OpsDomain.Repo
  alias Mezzanine.Programs.{PolicyBundle, Program}
  alias Mezzanine.Review.ReviewUnit
  alias Mezzanine.Work.{WorkClass, WorkObject}

  setup do
    pid = Sandbox.start_owner!(Repo, shared: false)
    on_exit(fn -> Sandbox.stop_owner(pid) end)
    :ok
  end

  test "start_run ingests work and returns an AppKit-compatible scheduled result" do
    %{tenant_id: tenant_id, program: program, work_class: work_class} = fixture_stack("tenant-ak")

    assert {:ok, result} =
             AppKitBridge.start_run(
               %{
                 route_name: "operator.dispatch",
                 title: "Dispatch operator task",
                 payload: %{"issue_id" => "ENG-101"}
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

  test "run_status returns timeline and gate status for a referenced work object" do
    %{tenant_id: tenant_id, program: program, work_object: work_object} =
      fixture_stack("tenant-ak-status")

    {:ok, _audit} =
      Mezzanine.WorkAudit.record_event(tenant_id, %{
        program_id: program.id,
        work_object_id: work_object.id,
        event_kind: :work_planned,
        actor_kind: :system,
        actor_ref: "planner",
        payload: %{"step" => 1},
        occurred_at: ~U[2026-04-14 20:20:01Z]
      })

    assert {:ok, status} =
             AppKitBridge.run_status(
               %RunRef{
                 run_id: "run/ak-status",
                 scope_id: "program/#{program.id}",
                 metadata: %{tenant_id: tenant_id, work_object_id: work_object.id}
               },
               %{},
               []
             )

    assert status.work_object_id == work_object.id
    assert length(status.timeline) == 1
    assert is_map(status.gate_status)
  end

  test "review_run records a review decision through the assurance seam" do
    %{tenant_id: tenant_id, program: program, work_object: work_object, review_unit: review_unit} =
      fixture_stack("tenant-ak-review")

    run_ref = %RunRef{
      run_id: "run/ak-review",
      scope_id: "program/#{program.id}",
      metadata: %{
        tenant_id: tenant_id,
        program_id: program.id,
        work_object_id: work_object.id,
        review_unit_id: review_unit.id
      }
    }

    assert {:ok, %{decision: decision, review_unit: updated_review_unit}} =
             AppKitBridge.review_run(
               run_ref,
               %{
                 kind: :review_summary,
                 summary: "Looks good",
                 details: %{"checklist" => ["lint", "tests"]}
               },
               reason: "approved after review"
             )

    assert decision.state == :approved
    assert updated_review_unit.status == :accepted
  end

  defp fixture_stack(tenant_id) do
    actor = %{tenant_id: tenant_id}

    {:ok, program} =
      Program.create_program(
        %{
          slug: "app-kit-#{System.unique_integer([:positive])}",
          name: "AppKit Bridge Program",
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
          title: "Bridge work",
          description: "Exercise the AppKit bridge",
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
