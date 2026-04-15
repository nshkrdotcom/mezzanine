defmodule Mezzanine.OpsDomainPhase7DomainsTest do
  use Mezzanine.OpsDomain.DataCase, async: false

  alias Ash
  alias Ash.Resource.Info
  alias Mezzanine.Control.{ControlSession, OperatorIntervention}
  alias Mezzanine.Evidence.{AuditEvent, EvidenceBundle, EvidenceItem, TimelineProjection}
  alias Mezzanine.Programs.{PolicyBundle, Program}
  alias Mezzanine.Review.{Escalation, ReviewDecision, ReviewUnit, Waiver}
  alias Mezzanine.Runs.{Run, RunArtifact, RunGrant, RunSeries}
  alias Mezzanine.Work.{WorkClass, WorkObject}

  @workflow_fixture Path.expand("../../../ops_policy/test/fixtures/workflow.md", __DIR__)

  test "run lifecycle persists scheduling, grants, artifacts, and evidence attachment" do
    actor = %{tenant_id: "tenant-a"}
    {:ok, %{program: program, work_object: work_object}} = create_work_fixture(actor)

    assert {:ok, %RunSeries{} = run_series} =
             RunSeries
             |> Ash.Changeset.for_create(:open_series, %{work_object_id: work_object.id})
             |> Ash.Changeset.set_tenant(actor.tenant_id)
             |> Ash.create(actor: actor, domain: Mezzanine.Runs)

    assert {:ok, %Run{status: :scheduled} = run} =
             Run
             |> Ash.Changeset.for_create(:schedule, %{
               run_series_id: run_series.id,
               attempt: 1,
               runtime_profile: %{"runtime" => "session"},
               grant_profile: %{"connector" => "linear"}
             })
             |> Ash.Changeset.set_tenant(actor.tenant_id)
             |> Ash.create(actor: actor, domain: Mezzanine.Runs)

    assert {:ok, %RunSeries{current_run_id: current_run_id}} =
             run_series
             |> Ash.Changeset.for_update(:attach_current_run, %{current_run_id: run.id})
             |> Ash.Changeset.set_tenant(actor.tenant_id)
             |> Ash.update(actor: actor, domain: Mezzanine.Runs)

    assert current_run_id == run.id

    assert {:ok, %RunGrant{grant_kind: :connector}} =
             RunGrant
             |> Ash.Changeset.for_create(:grant, %{
               run_id: run.id,
               grant_kind: :connector,
               scope: %{"connector" => "linear"},
               approval_class: :pre_approved
             })
             |> Ash.Changeset.set_tenant(actor.tenant_id)
             |> Ash.create(actor: actor, domain: Mezzanine.Runs)

    assert {:ok, %RunArtifact{kind: :diff} = artifact} =
             RunArtifact
             |> Ash.Changeset.for_create(:record_artifact, %{
               run_id: run.id,
               kind: :diff,
               ref: "artifact://diff/1",
               metadata: %{"lines" => 42}
             })
             |> Ash.Changeset.set_tenant(actor.tenant_id)
             |> Ash.create(actor: actor, domain: Mezzanine.Runs)

    assert artifact.status == :pending

    assert {:ok, %Run{status: :running} = running_run} =
             run
             |> Ash.Changeset.for_update(:record_started, %{
               raw_runtime_ref: "runtime:run-1",
               started_at: DateTime.utc_now()
             })
             |> Ash.Changeset.set_tenant(actor.tenant_id)
             |> Ash.update(actor: actor, domain: Mezzanine.Runs)

    assert {:ok, %EvidenceBundle{} = evidence_bundle} =
             EvidenceBundle
             |> Ash.Changeset.for_create(:assemble, %{
               program_id: program.id,
               work_object_id: work_object.id,
               run_id: running_run.id,
               summary: "Run evidence",
               evidence_manifest: %{"diff" => "present"},
               completeness_status: %{"diff" => "present"}
             })
             |> Ash.Changeset.set_tenant(actor.tenant_id)
             |> Ash.create(actor: actor, domain: Mezzanine.Evidence)

    assert {:ok, %Run{status: :completed, evidence_bundle_id: evidence_bundle_id} = completed_run} =
             running_run
             |> Ash.Changeset.for_update(:record_completed, %{
               completed_at: DateTime.utc_now(),
               result_summary: "Applied patch",
               token_usage: %{"input" => 120, "output" => 80},
               evidence_bundle_id: evidence_bundle.id
             })
             |> Ash.Changeset.set_tenant(actor.tenant_id)
             |> Ash.update(actor: actor, domain: Mezzanine.Runs)

    assert evidence_bundle_id == evidence_bundle.id
    assert completed_run.raw_runtime_ref == "runtime:run-1"

    assert {:ok, [%RunArtifact{id: artifact_id}]} =
             RunArtifact
             |> Ash.Query.for_read(:list_for_run, %{run_id: run.id})
             |> Ash.Query.set_tenant(actor.tenant_id)
             |> Ash.read(actor: actor, domain: Mezzanine.Runs)

    assert artifact_id == artifact.id
  end

  test "review, control, and evidence records track the governed operator lifecycle" do
    actor = %{tenant_id: "tenant-a"}
    {:ok, %{program: program, work_object: work_object}} = create_work_fixture(actor)

    {:ok, run_series} =
      RunSeries
      |> Ash.Changeset.for_create(:open_series, %{work_object_id: work_object.id})
      |> Ash.Changeset.set_tenant(actor.tenant_id)
      |> Ash.create(actor: actor, domain: Mezzanine.Runs)

    {:ok, run} =
      Run
      |> Ash.Changeset.for_create(:schedule, %{
        run_series_id: run_series.id,
        attempt: 1,
        runtime_profile: %{"runtime" => "session"}
      })
      |> Ash.Changeset.set_tenant(actor.tenant_id)
      |> Ash.create(actor: actor, domain: Mezzanine.Runs)

    {:ok, evidence_bundle} =
      EvidenceBundle
      |> Ash.Changeset.for_create(:assemble, %{
        program_id: program.id,
        work_object_id: work_object.id,
        run_id: run.id,
        summary: "Run evidence",
        evidence_manifest: %{"transcript" => "present"},
        completeness_status: %{"transcript" => "present"}
      })
      |> Ash.Changeset.set_tenant(actor.tenant_id)
      |> Ash.create(actor: actor, domain: Mezzanine.Evidence)

    assert {:ok, %ReviewUnit{status: :pending} = review_unit} =
             ReviewUnit
             |> Ash.Changeset.for_create(:create_review_unit, %{
               work_object_id: work_object.id,
               run_id: run.id,
               review_kind: :operator_review,
               evidence_bundle_id: evidence_bundle.id,
               decision_profile: %{"options" => ["accept", "reject", "waive", "escalate"]},
               reviewer_actor: %{"kind" => "human", "ref" => "operator-1"}
             })
             |> Ash.Changeset.set_tenant(actor.tenant_id)
             |> Ash.create(actor: actor, domain: Mezzanine.Review)

    assert {:ok, %ReviewUnit{status: :in_review} = in_review} =
             review_unit
             |> Ash.Changeset.for_update(:begin_review, %{})
             |> Ash.Changeset.set_tenant(actor.tenant_id)
             |> Ash.update(actor: actor, domain: Mezzanine.Review)

    assert {:ok, %ReviewDecision{decision: :accept}} =
             ReviewDecision
             |> Ash.Changeset.for_create(:record_decision, %{
               review_unit_id: in_review.id,
               decision: :accept,
               actor_kind: :human,
               actor_ref: "operator-1",
               reason: "Looks good",
               decided_at: DateTime.utc_now()
             })
             |> Ash.Changeset.set_tenant(actor.tenant_id)
             |> Ash.create(actor: actor, domain: Mezzanine.Review)

    assert {:ok, %ReviewUnit{status: :accepted}} =
             in_review
             |> Ash.Changeset.for_update(:accept, %{})
             |> Ash.Changeset.set_tenant(actor.tenant_id)
             |> Ash.update(actor: actor, domain: Mezzanine.Review)

    assert {:ok, %ReviewUnit{} = escalated_review} =
             ReviewUnit
             |> Ash.Changeset.for_create(:create_review_unit, %{
               work_object_id: work_object.id,
               run_id: run.id,
               review_kind: :policy_review,
               evidence_bundle_id: evidence_bundle.id,
               decision_profile: %{"options" => ["escalate"]},
               reviewer_actor: %{"kind" => "human", "ref" => "operator-2"}
             })
             |> Ash.Changeset.set_tenant(actor.tenant_id)
             |> Ash.create(actor: actor, domain: Mezzanine.Review)

    assert {:ok, %Escalation{status: :open}} =
             Escalation
             |> Ash.Changeset.for_create(:raise_escalation, %{
               review_unit_id: escalated_review.id,
               work_object_id: work_object.id,
               reason: "Needs policy admin",
               escalated_by: "operator-2",
               assigned_to: "admin-1",
               priority: :urgent
             })
             |> Ash.Changeset.set_tenant(actor.tenant_id)
             |> Ash.create(actor: actor, domain: Mezzanine.Review)

    assert {:ok, %ReviewUnit{status: :escalated}} =
             escalated_review
             |> Ash.Changeset.for_update(:escalate, %{})
             |> Ash.Changeset.set_tenant(actor.tenant_id)
             |> Ash.update(actor: actor, domain: Mezzanine.Review)

    assert {:ok, %Waiver{status: :active}} =
             Waiver
             |> Ash.Changeset.for_create(:grant_waiver, %{
               review_unit_id: escalated_review.id,
               work_object_id: work_object.id,
               reason: "Emergency release",
               granted_by: "super-operator"
             })
             |> Ash.Changeset.set_tenant(actor.tenant_id)
             |> Ash.create(actor: actor, domain: Mezzanine.Review)

    assert {:ok, %ControlSession{current_mode: :normal} = control_session} =
             ControlSession
             |> Ash.Changeset.for_create(:open, %{
               program_id: program.id,
               work_object_id: work_object.id
             })
             |> Ash.Changeset.set_tenant(actor.tenant_id)
             |> Ash.create(actor: actor, domain: Mezzanine.Control)

    assert {:ok, %ControlSession{current_mode: :paused} = paused_session} =
             control_session
             |> Ash.Changeset.for_update(:pause, %{})
             |> Ash.Changeset.set_tenant(actor.tenant_id)
             |> Ash.update(actor: actor, domain: Mezzanine.Control)

    assert {:ok, %OperatorIntervention{intervention_kind: :pause}} =
             OperatorIntervention
             |> Ash.Changeset.for_create(:record_intervention, %{
               control_session_id: control_session.id,
               operator_ref: "operator-1",
               intervention_kind: :pause,
               occurred_at: DateTime.utc_now()
             })
             |> Ash.Changeset.set_tenant(actor.tenant_id)
             |> Ash.create(actor: actor, domain: Mezzanine.Control)

    assert {:ok,
            %ControlSession{current_mode: :paused, active_override_set: overrides} =
              overridden_session} =
             paused_session
             |> Ash.Changeset.for_update(:apply_grant_override, %{
               active_override_set: %{"github.pr.write" => "approved"}
             })
             |> Ash.Changeset.set_tenant(actor.tenant_id)
             |> Ash.update(actor: actor, domain: Mezzanine.Control)

    assert overrides["github.pr.write"] == "approved"

    assert {:ok, %ControlSession{current_mode: :normal}} =
             overridden_session
             |> Ash.Changeset.for_update(:resume, %{})
             |> Ash.Changeset.set_tenant(actor.tenant_id)
             |> Ash.update(actor: actor, domain: Mezzanine.Control)
  end

  test "evidence and audit resources are durable and audit events stay append-only" do
    actor = %{tenant_id: "tenant-a"}
    {:ok, %{program: program, work_object: work_object}} = create_work_fixture(actor)

    assert {:ok, %EvidenceBundle{} = evidence_bundle} =
             EvidenceBundle
             |> Ash.Changeset.for_create(:assemble, %{
               program_id: program.id,
               work_object_id: work_object.id,
               summary: "Initial evidence",
               evidence_manifest: %{"issue" => "present"},
               completeness_status: %{"issue" => "present"}
             })
             |> Ash.Changeset.set_tenant(actor.tenant_id)
             |> Ash.create(actor: actor, domain: Mezzanine.Evidence)

    assert {:ok, %EvidenceItem{status: :pending}} =
             EvidenceItem
             |> Ash.Changeset.for_create(:record_item, %{
               evidence_bundle_id: evidence_bundle.id,
               kind: :log,
               ref: "artifact://log/1",
               metadata: %{"source" => "scheduler"}
             })
             |> Ash.Changeset.set_tenant(actor.tenant_id)
             |> Ash.create(actor: actor, domain: Mezzanine.Evidence)

    assert {:ok, %AuditEvent{event_kind: :work_planned} = audit_event} =
             AuditEvent
             |> Ash.Changeset.for_create(:record, %{
               program_id: program.id,
               work_object_id: work_object.id,
               event_kind: :work_planned,
               actor_kind: :system,
               actor_ref: "planner",
               payload: %{"plan_version" => 1},
               occurred_at: DateTime.utc_now()
             })
             |> Ash.Changeset.set_tenant(actor.tenant_id)
             |> Ash.create(actor: actor, domain: Mezzanine.Evidence)

    assert {:ok, %TimelineProjection{timeline: [%{"event_kind" => "work_planned"}]}} =
             TimelineProjection
             |> Ash.Changeset.for_create(:project, %{
               work_object_id: work_object.id,
               timeline: [%{"event_kind" => "work_planned", "actor_ref" => "planner"}],
               last_event_at: audit_event.occurred_at,
               projected_at: DateTime.utc_now()
             })
             |> Ash.Changeset.set_tenant(actor.tenant_id)
             |> Ash.create(actor: actor, domain: Mezzanine.Evidence)

    refute Enum.any?(Info.actions(AuditEvent), &(&1.type in [:update, :destroy]))
  end

  defp create_work_fixture(actor) do
    with {:ok, program} <- create_program(actor, "phase7-program"),
         {:ok, body} <- File.read(@workflow_fixture),
         {:ok, bundle} <- create_policy_bundle(actor, program, body),
         {:ok, work_class} <- create_work_class(actor, program, bundle),
         {:ok, work_object} <- create_work_object(actor, program, work_class) do
      {:ok,
       %{
         program: program,
         policy_bundle: bundle,
         work_class: work_class,
         work_object: work_object
       }}
    end
  end

  defp create_program(actor, slug) do
    Program
    |> Ash.Changeset.for_create(:create_program, %{
      slug: slug,
      name: String.capitalize(slug),
      product_family: "operator_stack",
      configuration: %{},
      metadata: %{}
    })
    |> Ash.Changeset.set_tenant(actor.tenant_id)
    |> Ash.create(actor: actor, domain: Mezzanine.Programs)
  end

  defp create_policy_bundle(actor, program, body) do
    PolicyBundle
    |> Ash.Changeset.for_create(:load_bundle, %{
      program_id: program.id,
      name: "default",
      version: "1.0.0",
      policy_kind: :workflow_md,
      source_ref: "WORKFLOW.md",
      body: body
    })
    |> Ash.Changeset.set_tenant(actor.tenant_id)
    |> Ash.create(actor: actor, domain: Mezzanine.Programs)
  end

  defp create_work_class(actor, program, bundle) do
    WorkClass
    |> Ash.Changeset.for_create(:create_work_class, %{
      program_id: program.id,
      name: "coding_task",
      kind: "coding_task",
      intake_schema: %{"required" => ["title"]},
      policy_bundle_id: bundle.id,
      default_review_profile: %{"required" => true},
      default_run_profile: %{"runtime" => "session"}
    })
    |> Ash.Changeset.set_tenant(actor.tenant_id)
    |> Ash.create(actor: actor, domain: Mezzanine.Work)
  end

  defp create_work_object(actor, program, work_class) do
    WorkObject
    |> Ash.Changeset.for_create(:ingest, %{
      program_id: program.id,
      work_class_id: work_class.id,
      external_ref: "linear:ENG-901",
      title: "Governed operator work",
      description: "Exercise the phase 7 durable domains",
      priority: 90,
      source_kind: "linear",
      payload: %{"issue_id" => "ENG-901"},
      normalized_payload: %{"issue_id" => "ENG-901"}
    })
    |> Ash.Changeset.set_tenant(actor.tenant_id)
    |> Ash.create(actor: actor, domain: Mezzanine.Work)
  end
end
