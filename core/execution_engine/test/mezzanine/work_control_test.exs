defmodule Mezzanine.WorkControlTest do
  use Mezzanine.Execution.DataCase, async: false

  alias Mezzanine.Execution.ExecutionRecord
  alias Mezzanine.Programs.{PolicyBundle, Program}
  alias Mezzanine.Work.WorkClass
  alias Mezzanine.Work.WorkObject
  alias Mezzanine.WorkControl
  alias Mezzanine.WorkExecutionHandoff
  alias Mezzanine.WorkQueries

  test "ensure_control_session opens a durable control session once and reuses it" do
    %{tenant_id: tenant_id, work_object: work_object} = fixture_stack("tenant-work-control")

    assert {:ok, nil} = WorkControl.control_session_for_work(tenant_id, work_object.id)

    assert {:ok, first_session} = WorkControl.ensure_control_session(tenant_id, work_object)

    assert {:ok, fetched_session} =
             WorkControl.control_session_for_work(tenant_id, work_object.id)

    assert fetched_session.id == first_session.id

    assert {:ok, second_session} = WorkControl.ensure_control_session(tenant_id, work_object)
    assert second_session.id == first_session.id
  end

  test "open_control_sessions lists active sessions for a program through the neutral seam" do
    %{tenant_id: tenant_id, program: program, work_object: work_object} =
      fixture_stack("tenant-open-control-sessions")

    assert {:ok, _session} = WorkControl.ensure_control_session(tenant_id, work_object)
    assert {:ok, sessions} = WorkControl.open_control_sessions(tenant_id, program.id)

    assert Enum.map(sessions, & &1.work_object_id) == [work_object.id]
    assert Enum.all?(sessions, &(&1.status == :active))
  end

  test "start_run_for_subject stores phase 2 runtime metadata on the scheduled run" do
    %{tenant_id: tenant_id, work_object: work_object} =
      fixture_stack("tenant-work-control-phase2")

    attrs = %{
      trace_id: "trace-phase2",
      recipe_ref: "coding_operations",
      idempotency_key: "idem-phase2",
      pack_revision: 9,
      runtime_profile_ref: "codex_session",
      runtime_profile_kind: "temporal_local",
      runtime_profile_revision: 4,
      lower_runtime_kind: "codex_session",
      requested_capability_ids: ["codex.session.turn", "linear.comments.update"],
      requested_action_ids: ["codex.session.turn"],
      source_binding_refs: ["linear_primary"],
      resource_scope_refs: ["source_binding://linear_primary"],
      workspace_policy_ref: "workspace-policy://extravaganza/coding_operations",
      live_provider_allowed: false,
      evidence_profile_ref: "github_pr_plus_workpad",
      memory_profile_ref: "none",
      context_profile_ref: "outer_brain_optional_context_v1",
      memory_context_required: false,
      memory_context_source_refs: ["workspace_memory"],
      memory_context_binding_keys: ["shared_memory"],
      redaction_profile_ref: "redaction://extravaganza/default",
      prompt_context_recipe_refs: ["coding_agent_system"],
      runtime_policy_config: %{"run" => %{"capability" => "codex.session.turn"}}
    }

    assert {:ok, started} =
             WorkControl.start_run_for_subject(tenant_id, work_object.id, attrs)

    assert started.run.runtime_profile["runtime_profile_ref"] == "codex_session"
    assert started.run.runtime_profile["runtime_profile_kind"] == "temporal_local"
    assert started.run.runtime_profile["runtime_profile_revision"] == 4
    assert started.run.runtime_profile["lower_runtime_kind"] == "codex_session"
    assert started.run.runtime_profile["capability_id"] == "codex.session.turn"
    assert started.run.runtime_profile["requested_action_ids"] == ["codex.session.turn"]
    assert started.run.runtime_profile["memory_profile_ref"] == "none"
    assert started.run.runtime_profile["context_profile_ref"] == "outer_brain_optional_context_v1"
    assert started.run.runtime_profile["memory_context_required"] == false
    assert started.run.runtime_profile["memory_context_source_refs"] == ["workspace_memory"]
    assert started.run.runtime_profile["memory_context_binding_keys"] == ["shared_memory"]
    assert started.run.runtime_profile["idempotency_key"] == "idem-phase2"
    assert started.run.runtime_profile["pack_revision"] == 9
    assert "linear.comments.update" in started.run.grant_profile["capability_ids"]
  end

  test "work execution handoff creates a current execution row for AppKit readback" do
    %{tenant_id: tenant_id, work_object: work_object} =
      fixture_stack("tenant-work-control-execution-handoff")

    attrs = %{
      trace_id: "trace-execution-handoff",
      actor_ref: "ops_lead",
      installation_ref: "installation://tenant-work-control-execution-handoff/default",
      recipe_ref: "coding_operations",
      idempotency_key: "idem-execution-handoff",
      pack_revision: 3,
      runtime_profile_ref: "codex_session",
      runtime_profile_kind: "temporal_local",
      lower_runtime_kind: "codex_session",
      requested_capability_ids: ["codex.session.turn"],
      requested_action_ids: ["codex.session.turn"],
      source_binding_refs: ["linear_primary"],
      resource_scope_refs: ["source_binding://linear_primary"],
      live_provider_allowed: false
    }

    assert {:ok, started} = WorkControl.start_run_for_subject(tenant_id, work_object.id, attrs)

    workflow_handoff = %{
      outbox_row: %{
        outbox_id: "workflow-start:test-execution-handoff",
        workflow_id: "workflow:test-execution-handoff",
        dispatch_state: "queued"
      },
      workflow_start_ref: "workflow-start-outbox://workflow-start:test-execution-handoff",
      evidence_ref: "audit-event://workflow-start:test-execution-handoff"
    }

    assert {:ok, handoff} =
             WorkExecutionHandoff.ensure_current_execution(
               tenant_id,
               started,
               workflow_handoff,
               attrs
             )

    assert handoff.status == :created
    assert handoff.execution.subject_id == work_object.id
    assert handoff.execution.dispatch_state == :queued
    assert handoff.execution.submission_dedupe_key == "idem-execution-handoff"

    assert handoff.execution.dispatch_envelope["workflow_start_ref"] ==
             workflow_handoff.workflow_start_ref

    assert {:ok, [active_execution]} = ExecutionRecord.active_for_subject(work_object.id)
    assert active_execution.id == handoff.execution.id

    assert {:ok, reused} =
             WorkExecutionHandoff.ensure_current_execution(
               tenant_id,
               started,
               workflow_handoff,
               attrs
             )

    assert reused.status == :reused
    assert reused.execution.id == handoff.execution.id

    assert {:ok, projection} =
             WorkQueries.get_subject_runtime_projection(tenant_id, work_object.id)

    assert projection.projection_name == "operator_subject_runtime"
    assert projection.execution["execution_id"] == handoff.execution.id
    assert projection.execution["dispatch_state"] == "queued"
    assert projection.execution["metadata"]["scheduler_state"] == "claim_queued"
    assert projection.execution["metadata"]["claim_state"] == "claimed"
    assert projection.execution["metadata"]["running_state"] == "not_running"
    assert projection.execution["metadata"]["retry_state"] == "none"
    assert projection.runtime["event_counts"]["scheduler_claim_queued"] == 1
    assert projection.runtime["retry_queue"] == []
    refute Map.has_key?(projection.execution["metadata"], "workflow_id")
    assert projection.execution["metadata"]["workflow_ref"] == "workflow:test-execution-handoff"

    assert projection.lower_receipt["lower_receipt_ref"] ==
             "lower-receipt://pending/#{handoff.execution.id}"

    assert [%{"source_ref" => source_ref}] = projection.source_bindings
    assert String.contains?(source_ref, "linear")

    retry_at = ~U[2026-05-10 22:30:00Z]

    Repo.query!(
      """
      UPDATE execution_records
      SET dispatch_state = 'in_flight',
          next_dispatch_at = $2,
          last_dispatch_error_kind = 'restart_recovery',
          last_dispatch_error_payload = jsonb_build_object('reason', 'dispatch_worker_restarted'),
          updated_at = $3
      WHERE id::text = $1
      """,
      [handoff.execution.id, retry_at, retry_at]
    )

    assert {:ok, retry_projection} =
             WorkQueries.get_subject_runtime_projection(tenant_id, work_object.id)

    assert retry_projection.execution["dispatch_state"] == "in_flight"
    assert retry_projection.execution["metadata"]["scheduler_state"] == "retry_scheduled"
    assert retry_projection.execution["metadata"]["claim_state"] == "released"
    assert retry_projection.execution["metadata"]["running_state"] == "not_running"
    assert retry_projection.execution["metadata"]["retry_state"] == "scheduled"
    assert retry_projection.runtime["event_counts"]["scheduler_retry_scheduled"] == 1

    expected_attempt_ref = "attempt://#{handoff.execution.id}/1"

    assert [
             %{
               "attempt_ref" => ^expected_attempt_ref,
               "status" => "scheduled",
               "reason" => "restart_recovery",
               "scheduled_at" => scheduled_at
             }
           ] = retry_projection.runtime["retry_queue"]

    assert DateTime.compare(scheduled_at, retry_at) == :eq

    completed_at = ~U[2026-05-10 22:40:00Z]

    Repo.query!(
      """
      UPDATE execution_records
      SET dispatch_state = 'completed',
          next_dispatch_at = NULL,
          last_dispatch_error_kind = NULL,
          last_dispatch_error_payload = '{}'::jsonb,
          updated_at = $2
      WHERE id::text = $1
      """,
      [handoff.execution.id, completed_at]
    )

    assert {:ok, completed_projection} =
             WorkQueries.get_subject_runtime_projection(tenant_id, work_object.id)

    assert completed_projection.lifecycle_state == "completed"
    assert completed_projection.execution["dispatch_state"] == "completed"
    assert completed_projection.execution["metadata"]["scheduler_state"] == "completed"
    assert completed_projection.execution["metadata"]["claim_state"] == "completed"
    assert completed_projection.execution["metadata"]["running_state"] == "not_running"
    assert completed_projection.execution["metadata"]["retry_state"] == "none"
    assert completed_projection.execution["metadata"]["completion_state"] == "completed"
  end

  test "runtime source binding projection preserves source-engine payload facts" do
    %{tenant_id: tenant_id, program: program, work_class: work_class} =
      fixture_stack("tenant-work-control-source-payload")

    source_ref = "linear://#{tenant_id}/issue/ENG-777"

    source_payload = %{
      "external_ref" => source_ref,
      "source_ref" => source_ref,
      "source_binding_id" => "linear_primary",
      "provider" => "linear",
      "provider_external_ref" => "lin-issue-777",
      "provider_revision" => "2026-05-09T00:00:00Z",
      "source_state" => "Todo",
      "branch_ref" => "nshkrdotcom/extravaganza/eng-777",
      "source_url" => "https://linear.app/example/issue/ENG-777",
      "labels" => ["ops", "phase-7"],
      "blocker_refs" => [
        %{"provider_external_ref" => "lin-issue-776", "terminal?" => false}
      ],
      "state_mapping" => %{
        "lifecycle_state" => "submitted",
        "reason" => "blocked_by_non_terminal"
      }
    }

    assert {:ok, source_subject} =
             WorkQueries.ingest_subject(%{
               tenant_id: tenant_id,
               program_id: program.id,
               work_class_id: work_class.id,
               external_ref: source_ref,
               title: "Source payload subject",
               description: "Preserve Linear source payload facts",
               priority: 42,
               source_kind: "linear",
               payload: source_payload,
               normalized_payload: Map.put(source_payload, "payload", source_payload)
             })

    assert {:ok, summaries} = WorkQueries.list_subjects(tenant_id, program.id, %{})
    summary = Enum.find(summaries, &(&1.subject_id == source_subject.subject_id))
    assert summary
    assert summary.source_payload["provider_external_ref"] == "lin-issue-777"
    assert summary.source_payload["source_binding_id"] == "linear_primary"
    assert summary.source_payload["branch_ref"] == "nshkrdotcom/extravaganza/eng-777"
    assert summary.source_payload["labels"] == ["ops", "phase-7"]

    assert {:ok, detail} = WorkQueries.get_subject_detail(tenant_id, source_subject.subject_id)
    assert detail.source_payload["source_url"] == "https://linear.app/example/issue/ENG-777"
    assert [%{"provider_external_ref" => "lin-issue-776"}] = detail.source_payload["blocker_refs"]
    assert detail.source_payload["state_mapping"]["reason"] == "blocked_by_non_terminal"

    attrs = %{
      trace_id: "trace-source-payload",
      actor_ref: "ops_lead",
      installation_ref: "installation://tenant-work-control-source-payload/default",
      recipe_ref: "coding_operations",
      idempotency_key: "idem-source-payload",
      runtime_profile_ref: "codex_session",
      runtime_profile_kind: "temporal_local",
      lower_runtime_kind: "codex_session",
      requested_capability_ids: ["codex.session.turn"],
      requested_action_ids: ["codex.session.turn"],
      source_binding_refs: ["linear_primary"],
      resource_scope_refs: ["source_binding://linear_primary"],
      live_provider_allowed: false
    }

    assert {:ok, started} =
             WorkControl.start_run_for_subject(tenant_id, source_subject.subject_id, attrs)

    workflow_handoff = %{
      outbox_row: %{
        outbox_id: "workflow-start:test-source-payload",
        workflow_id: "workflow:test-source-payload",
        dispatch_state: "queued"
      },
      workflow_start_ref: "workflow-start-outbox://workflow-start:test-source-payload",
      evidence_ref: "audit-event://workflow-start:test-source-payload"
    }

    assert {:ok, _handoff} =
             WorkExecutionHandoff.ensure_current_execution(
               tenant_id,
               started,
               workflow_handoff,
               attrs
             )

    assert {:ok, projection} =
             WorkQueries.get_subject_runtime_projection(tenant_id, source_subject.subject_id)

    assert [
             %{
               "binding_ref" => "linear_primary",
               "source_ref" => ^source_ref,
               "source_kind" => "linear",
               "external_system" => "linear",
               "source_state" => "Todo",
               "source_url" => "https://linear.app/example/issue/ENG-777",
               "metadata" => metadata
             }
           ] = projection.source_bindings

    assert metadata["provider_external_ref"] == "lin-issue-777"
    assert metadata["branch_ref"] == "nshkrdotcom/extravaganza/eng-777"
    assert metadata["labels"] == ["ops", "phase-7"]
    assert [%{"provider_external_ref" => "lin-issue-776"}] = metadata["blocker_refs"]
  end

  defp fixture_stack(tenant_id) do
    actor = %{tenant_id: tenant_id}

    {:ok, program} =
      Program.create_program(
        %{
          slug: "work-control-#{System.unique_integer([:positive])}",
          name: "Work Control Program",
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
          title: "Work control subject",
          description: "Exercise the control-session seam",
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
    run:
      profile: default_session
      runtime_class: session
      capability: codex.session.turn
      target: linear-default
    approval:
      mode: manual
      reviewers:
        - ops_lead
      escalation_required: true
    review:
      required: true
      required_decisions: 1
      gates:
        - operator
    ---
    # Work control prompt
    """
  end
end
