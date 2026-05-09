defmodule Mezzanine.WorkControlTest do
  use Mezzanine.Execution.DataCase, async: false

  alias Mezzanine.Programs.{PolicyBundle, Program}
  alias Mezzanine.Work.WorkClass
  alias Mezzanine.Work.WorkObject
  alias Mezzanine.WorkControl

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
