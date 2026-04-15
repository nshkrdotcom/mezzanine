defmodule MezzanineOperatorSurfaceTest do
  use ExUnit.Case, async: false

  alias Ecto.Adapters.SQL.Sandbox
  alias Mezzanine.Control.ControlSession
  alias Mezzanine.OpsDomain.Repo
  alias Mezzanine.Programs.{PolicyBundle, Program}
  alias Mezzanine.Review.ReviewUnit
  alias Mezzanine.Runs.{Run, RunArtifact, RunSeries}
  alias Mezzanine.Surfaces.OperatorSurface
  alias Mezzanine.Work.{WorkClass, WorkObject}

  setup do
    pid = Sandbox.start_owner!(Repo, shared: false)
    on_exit(fn -> Sandbox.stop_owner(pid) end)
    :ok
  end

  test "lists operator alerts and pending reviews for a program" do
    %{tenant_id: tenant_id, program: program, work_object: work_object} =
      fixture_stack("tenant-op-alerts")

    assert {:ok, alerts} = OperatorSurface.list_operator_alerts(tenant_id, program.id)
    assert Enum.any?(alerts, &(&1.work_object_id == work_object.id))

    assert {:ok, pending_reviews} = OperatorSurface.list_pending_reviews(tenant_id, program.id)
    assert Enum.any?(pending_reviews, &(&1.work_object_id == work_object.id))
  end

  test "gets run detail and system health for operator views" do
    %{tenant_id: tenant_id, program: program, run: run} = fixture_stack("tenant-op-run")

    assert {:ok, detail} = OperatorSurface.get_run_detail(tenant_id, run.id)
    assert detail.run.id == run.id
    assert length(detail.run_artifacts) == 1

    assert {:ok, health} = OperatorSurface.get_system_health(tenant_id, program.id)
    assert health.program_id == program.id
    assert health.active_run_count >= 1
  end

  test "dispatches pause and grant-override control commands" do
    %{tenant_id: tenant_id, work_object: work_object} = fixture_stack("tenant-op-control")

    assert {:ok, %{control_session: paused_session}} =
             OperatorSurface.execute_control(
               tenant_id,
               work_object.id,
               :pause,
               %{"reason" => "needs inspection"},
               %{actor_ref: "ops_lead"}
             )

    assert paused_session.current_mode == :paused

    assert {:ok, %{control_session: overridden_session}} =
             OperatorSurface.override_grant_profile(
               tenant_id,
               work_object.id,
               %{:"linear.issue.update" => :allow},
               %{actor_ref: "ops_lead"}
             )

    assert overridden_session.active_override_set["linear.issue.update"] == "allow"
  end

  defp fixture_stack(tenant_id) do
    actor = %{tenant_id: tenant_id}

    {:ok, program} =
      Program.create_program(
        %{
          slug: "operator-surface-#{System.unique_integer([:positive])}",
          name: "Operator Surface Program",
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
          title: "Operator work",
          description: "Exercise operator surface",
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

    {:ok, control_session} =
      ControlSession.open(
        %{program_id: program.id, work_object_id: work_object.id},
        actor: actor,
        tenant: tenant_id
      )

    {:ok, run_series} =
      RunSeries.open_series(
        %{work_object_id: work_object.id, control_session_id: control_session.id},
        actor: actor,
        tenant: tenant_id
      )

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

    {:ok, _run_artifact} =
      RunArtifact.record_artifact(
        %{run_id: run.id, kind: :pr, ref: "https://github.com/example/pr/1", metadata: %{}},
        actor: actor,
        tenant: tenant_id
      )

    {:ok, _review_unit} =
      ReviewUnit.create_review_unit(
        %{
          work_object_id: work_object.id,
          run_id: run.id,
          review_kind: :operator_review,
          required_by: DateTime.utc_now(),
          decision_profile: %{"required_decisions" => 1},
          reviewer_actor: %{"kind" => "human", "ref" => "ops_lead"}
        },
        actor: actor,
        tenant: tenant_id
      )

    {:ok, _audit} =
      Mezzanine.Audit.record_event(tenant_id, %{
        program_id: program.id,
        work_object_id: work_object.id,
        run_id: run.id,
        event_kind: :run_scheduled,
        actor_kind: :system,
        actor_ref: "planner",
        payload: %{"attempt" => 1},
        occurred_at: ~U[2026-04-14 21:05:01Z]
      })

    %{
      tenant_id: tenant_id,
      actor: actor,
      program: program,
      work_class: work_class,
      work_object: work_object,
      run: run
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
