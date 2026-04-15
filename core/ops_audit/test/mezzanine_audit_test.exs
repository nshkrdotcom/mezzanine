defmodule Mezzanine.AuditTest do
  use ExUnit.Case, async: false

  alias Ecto.Adapters.SQL.Sandbox
  alias Mezzanine.Audit
  alias Mezzanine.OpsDomain.Repo
  alias Mezzanine.Programs.{PolicyBundle, Program}
  alias Mezzanine.Work.{WorkClass, WorkObject}

  setup do
    pid = Sandbox.start_owner!(Repo, shared: false)
    on_exit(fn -> Sandbox.stop_owner(pid) end)
    :ok
  end

  test "refresh_timeline orders audit events and persists the projection" do
    %{tenant_id: tenant_id, work_object: work_object, program: program} =
      fixture_stack("tenant-a")

    {:ok, _} =
      Audit.record_event(tenant_id, %{
        program_id: program.id,
        work_object_id: work_object.id,
        event_kind: :run_completed,
        actor_kind: :system,
        actor_ref: "runtime",
        payload: %{"step" => 2},
        occurred_at: ~U[2026-04-14 19:00:02Z]
      })

    {:ok, _} =
      Audit.record_event(tenant_id, %{
        program_id: program.id,
        work_object_id: work_object.id,
        event_kind: :work_planned,
        actor_kind: :system,
        actor_ref: "planner",
        payload: %{"step" => 1},
        occurred_at: ~U[2026-04-14 19:00:01Z]
      })

    assert {:ok, projection} = Audit.refresh_timeline(tenant_id, work_object.id)
    assert Enum.map(projection.timeline, & &1.event_kind) == ["work_planned", "run_completed"]
  end

  test "assemble_bundle produces a ready evidence bundle with a manifest summary" do
    %{tenant_id: tenant_id, work_object: work_object, program: program} =
      fixture_stack("tenant-b")

    {:ok, _} =
      Audit.record_event(tenant_id, %{
        program_id: program.id,
        work_object_id: work_object.id,
        event_kind: :work_planned,
        actor_kind: :system,
        actor_ref: "planner",
        payload: %{"step" => 1},
        occurred_at: ~U[2026-04-14 19:10:01Z]
      })

    assert {:ok, bundle} =
             Audit.assemble_bundle(tenant_id, %{
               program_id: program.id,
               work_object_id: work_object.id
             })

    assert bundle.status == :ready
    assert bundle.completeness_status.audit_events == :present
    assert bundle.evidence_manifest.audit_event_count == 1
  end

  defp fixture_stack(tenant_id) do
    actor = %{tenant_id: tenant_id}

    {:ok, program} =
      Program.create_program(
        %{
          slug: "audit-#{System.unique_integer([:positive])}",
          name: "Audit Program",
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
          title: "Audit work",
          description: "Exercise the audit service",
          priority: 50,
          source_kind: "linear",
          payload: %{"issue_id" => "ENG-1"},
          normalized_payload: %{"issue_id" => "ENG-1"}
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
      work_object: work_object
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
