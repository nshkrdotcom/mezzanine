defmodule Mezzanine.AppKitBridge.OperatorServicesTest do
  use ExUnit.Case, async: false

  alias AppKit.Core.RunRef
  alias Ecto.Adapters.SQL.Sandbox

  alias Mezzanine.AppKitBridge.{
    OperatorActionService,
    OperatorProjectionAdapter,
    OperatorQueryService
  }

  alias Mezzanine.Audit.Repo, as: AuditRepo
  alias Mezzanine.Control.ControlSession
  alias Mezzanine.Decisions.Repo, as: DecisionsRepo
  alias Mezzanine.EvidenceLedger.Repo, as: EvidenceRepo
  alias Mezzanine.Execution.Repo, as: ExecutionRepo
  alias Mezzanine.OpsDomain.Repo, as: OpsDomainRepo
  alias Mezzanine.Programs.{PolicyBundle, Program}
  alias Mezzanine.Review.ReviewUnit
  alias Mezzanine.Runs.{Run, RunArtifact, RunSeries}
  alias Mezzanine.Work.{WorkClass, WorkObject}

  defmodule LowerFactsStub do
    def operation_supported?(operation),
      do: operation in [:fetch_run, :events, :attempts, :run_artifacts]

    def fetch_run(run_id) do
      send(self(), {:fetch_run, [run_id]})

      {:ok,
       %{
         run_id: run_id,
         status: :running,
         occurred_at: ~U[2026-04-15 10:02:00Z]
       }}
    end

    def events(run_id) do
      send(self(), {:events, [run_id]})

      [
        %{
          id: "lower-event-1",
          run_id: run_id,
          event_kind: "attempt.started",
          occurred_at: ~U[2026-04-15 10:03:00Z]
        }
      ]
    end

    def attempts(run_id) do
      send(self(), {:attempts, [run_id]})

      [
        %{
          attempt_id: "attempt-1",
          run_id: run_id,
          status: :running,
          occurred_at: ~U[2026-04-15 10:04:00Z]
        }
      ]
    end

    def run_artifacts(run_id) do
      send(self(), {:run_artifacts, [run_id]})

      [
        %{
          artifact_id: "artifact-1",
          run_id: run_id,
          kind: :log,
          occurred_at: ~U[2026-04-15 10:05:00Z]
        }
      ]
    end
  end

  setup do
    ops_domain_owner = Sandbox.start_owner!(OpsDomainRepo, shared: false)
    audit_owner = Sandbox.start_owner!(AuditRepo, shared: false)
    execution_owner = Sandbox.start_owner!(ExecutionRepo, shared: false)
    decisions_owner = Sandbox.start_owner!(DecisionsRepo, shared: false)
    evidence_owner = Sandbox.start_owner!(EvidenceRepo, shared: false)

    on_exit(fn ->
      Sandbox.stop_owner(evidence_owner)
      Sandbox.stop_owner(decisions_owner)
      Sandbox.stop_owner(execution_owner)
      Sandbox.stop_owner(audit_owner)
      Sandbox.stop_owner(ops_domain_owner)
    end)

    :ok
  end

  test "operator query service exposes subject status, timeline, alerts, reviews, and health without the deprecated operator surface" do
    %{tenant_id: tenant_id, program: program, work_object: work_object, run: run} =
      fixture_stack("tenant-operator-services")

    assert {:ok, status} = OperatorQueryService.subject_status(tenant_id, work_object.id)
    assert status.subject_ref.id == work_object.id
    assert status.subject_ref.subject_kind == "work_object"
    assert status.current_execution_ref.id == run.id
    assert Enum.any?(status.available_actions, &(&1.action_kind == "pause"))

    assert {:ok, timeline} = OperatorQueryService.timeline(tenant_id, work_object.id)
    assert timeline.subject_ref.id == work_object.id
    assert Enum.any?(timeline.entries, &(&1.event_kind == "run_scheduled"))

    assert {:ok, alerts} = OperatorQueryService.list_operator_alerts(tenant_id, program.id)
    assert Enum.any?(alerts, &(&1.subject_ref.id == work_object.id))

    assert {:ok, pending_reviews} =
             OperatorQueryService.list_pending_reviews(tenant_id, program.id)

    assert Enum.any?(pending_reviews, &(&1.subject_ref.id == work_object.id))

    assert {:ok, health} = OperatorQueryService.system_health(tenant_id, program.id)
    assert health.program_id == program.id
    assert health.active_run_count >= 1
    assert health.pending_review_count >= 1
  end

  test "operator action service applies control actions and keeps adapter compatibility for review decisions" do
    %{
      tenant_id: tenant_id,
      program: program,
      work_object: work_object,
      review_unit: review_unit
    } = fixture_stack("tenant-operator-actions")

    assert {:ok, pause_result} =
             OperatorActionService.apply_action(
               tenant_id,
               work_object.id,
               :pause,
               %{"reason" => "needs inspection"},
               %{actor_ref: "ops_lead"}
             )

    assert pause_result.status == :completed
    assert pause_result.action_ref.action_kind == "pause"
    assert pause_result.metadata.control_session.current_mode == :paused

    assert {:ok, override_result} =
             OperatorActionService.apply_action(
               tenant_id,
               work_object.id,
               :grant_override,
               %{grant_overrides: %{:"linear.issue.update" => :allow}},
               %{actor_ref: "ops_lead"}
             )

    assert override_result.action_ref.action_kind == "grant_override"

    assert override_result.metadata.control_session.active_override_set["linear.issue.update"] ==
             "allow"

    run_ref = %RunRef{
      run_id: "run/operator-review",
      scope_id: "program/#{program.id}",
      metadata: %{
        tenant_id: tenant_id,
        program_id: program.id,
        work_object_id: work_object.id,
        review_unit_id: review_unit.id
      }
    }

    assert {:ok, %{decision: decision, review_unit: updated_review_unit}} =
             OperatorProjectionAdapter.review_run(
               run_ref,
               %{
                 kind: :review_summary,
                 summary: "Ready to proceed",
                 details: %{"checklist" => ["tests", "credo", "dialyzer"]}
               },
               reason: "approved by operator"
             )

    assert decision.state == :approved
    assert updated_review_unit.status == :accepted
  end

  test "operator query service assembles unified trace through the substrate contract and lower read bridge" do
    %{tenant_id: tenant_id, work_object: work_object} =
      fixture_stack("tenant-operator-unified-trace")

    %{execution: execution, trace_id: trace_id} =
      seed_trace_ledger(tenant_id, work_object.id, "trace-operator-services")

    assert {:ok, trace} =
             OperatorQueryService.get_unified_trace(
               %{
                 tenant_id: tenant_id,
                 actor_id: "ops_lead",
                 installation_id: tenant_id,
                 execution_id: execution.id,
                 trace_id: trace_id
               },
               lower_facts: LowerFactsStub
             )

    assert trace.trace_id == trace_id
    assert trace.installation_id == tenant_id

    assert trace.join_keys == %{
             "trace_id" => trace_id,
             "installation_id" => tenant_id,
             "execution_id" => execution.id
           }

    assert trace.metadata.indexed_join_keys == ["trace_id", "causation_id"]
    assert Enum.any?(trace.steps, &(&1.source == :audit_fact))
    assert Enum.any?(trace.steps, &(&1.source == :execution_record))
    assert Enum.any?(trace.steps, &(&1.source == :decision_record))
    assert Enum.any?(trace.steps, &(&1.source == :evidence_record))

    lower_step = Enum.find(trace.steps, &(&1.source == :lower_run_status))
    assert lower_step.freshness == :lower_authoritative_unreconciled
    refute lower_step.operator_actionable?

    assert_received {:fetch_run, ["lower-run-trace-operator-services"]}
    assert_received {:events, ["lower-run-trace-operator-services"]}
  end

  test "operator query service returns explicit auth denial for unauthorized lower-enriched trace reads" do
    %{tenant_id: tenant_id, work_object: work_object} =
      fixture_stack("tenant-operator-unified-trace-denied")

    %{execution: execution, trace_id: trace_id} =
      seed_trace_ledger(tenant_id, work_object.id, "trace-operator-services-denied")

    assert {:error, :unauthorized_lower_read} =
             OperatorQueryService.get_unified_trace(
               %{
                 tenant_id: tenant_id,
                 actor_id: "ops_lead",
                 installation_id: "inst-other",
                 execution_id: execution.id,
                 trace_id: trace_id
               },
               lower_facts: LowerFactsStub
             )
  end

  defp fixture_stack(tenant_id) do
    actor = %{tenant_id: tenant_id}

    {:ok, program} =
      Program.create_program(
        %{
          slug: "operator-services-#{System.unique_integer([:positive])}",
          name: "Operator Services Program",
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
          description: "Exercise operator services",
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

    {:ok, review_unit} =
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
        occurred_at: ~U[2026-04-15 09:59:00Z]
      })

    %{
      tenant_id: tenant_id,
      actor: actor,
      program: program,
      bundle: bundle,
      work_class: work_class,
      work_object: work_object,
      review_unit: review_unit,
      run: run
    }
  end

  defp seed_trace_ledger(installation_id, subject_id, suffix) do
    execution_id = Ecto.UUID.generate()
    trace_id = "#{suffix}-#{System.unique_integer([:positive])}"
    now = ~U[2026-04-15 10:00:00Z]

    assert {1, _} =
             ExecutionRepo.insert_all("execution_records", [
               %{
                 id: dump_uuid!(execution_id),
                 installation_id: installation_id,
                 subject_id: dump_uuid!(subject_id),
                 recipe_ref: "triage_ticket",
                 trace_id: trace_id,
                 causation_id: "cause-#{suffix}",
                 dispatch_state: "accepted",
                 dispatch_attempt_count: 0,
                 next_dispatch_at: now,
                 submission_ref: %{"id" => "submission-#{suffix}"},
                 lower_receipt: %{"run_id" => "lower-run-#{suffix}"},
                 last_dispatch_error_payload: %{},
                 row_version: 1,
                 inserted_at: now,
                 updated_at: now,
                 compiled_pack_revision: 7,
                 binding_snapshot: %{"placement_ref" => "local-session"}
               }
             ])

    assert {1, _} =
             AuditRepo.insert_all("audit_facts", [
               %{
                 id: dump_uuid!(Ecto.UUID.generate()),
                 installation_id: installation_id,
                 subject_id: subject_id,
                 execution_id: execution_id,
                 trace_id: trace_id,
                 causation_id: "cause-#{suffix}",
                 fact_kind: "execution_dispatched",
                 actor_ref: %{kind: :scheduler},
                 payload: %{dispatch_state: "accepted"},
                 occurred_at: now,
                 inserted_at: now,
                 updated_at: now
               }
             ])

    assert {1, _} =
             AuditRepo.insert_all("execution_lineage_records", [
               %{
                 id: dump_uuid!(Ecto.UUID.generate()),
                 trace_id: trace_id,
                 causation_id: "cause-#{suffix}",
                 installation_id: installation_id,
                 subject_id: subject_id,
                 execution_id: execution_id,
                 dispatch_outbox_entry_id: Ecto.UUID.generate(),
                 ji_submission_key: "submission-#{suffix}",
                 lower_run_id: "lower-run-#{suffix}",
                 lower_attempt_id: "attempt-#{suffix}",
                 artifact_refs: ["artifact-1"],
                 inserted_at: now,
                 updated_at: now
               }
             ])

    assert {1, _} =
             DecisionsRepo.insert_all("decision_records", [
               %{
                 id: dump_uuid!(Ecto.UUID.generate()),
                 installation_id: installation_id,
                 subject_id: dump_uuid!(subject_id),
                 execution_id: dump_uuid!(execution_id),
                 decision_kind: "human_review_required",
                 lifecycle_state: "pending",
                 required_by: ~U[2026-04-20 00:00:00Z],
                 trace_id: trace_id,
                 causation_id: execution_id,
                 row_version: 1,
                 inserted_at: ~U[2026-04-15 10:01:00Z],
                 updated_at: ~U[2026-04-15 10:01:00Z]
               }
             ])

    assert {1, _} =
             EvidenceRepo.insert_all("evidence_records", [
               %{
                 id: dump_uuid!(Ecto.UUID.generate()),
                 installation_id: installation_id,
                 subject_id: dump_uuid!(subject_id),
                 execution_id: dump_uuid!(execution_id),
                 evidence_kind: "run_log",
                 collector_ref: "jido_run_output",
                 content_ref: "artifact://#{suffix}",
                 status: "collected",
                 metadata: %{"size" => 128},
                 collected_at: ~U[2026-04-15 10:02:00Z],
                 trace_id: trace_id,
                 causation_id: execution_id,
                 row_version: 1,
                 inserted_at: ~U[2026-04-15 10:02:00Z],
                 updated_at: ~U[2026-04-15 10:02:00Z]
               }
             ])

    %{execution: %{id: execution_id}, trace_id: trace_id}
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

  defp dump_uuid!(value), do: Ecto.UUID.dump!(value)
end
