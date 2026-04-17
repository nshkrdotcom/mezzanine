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
      capability: linear.issue.execute
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
