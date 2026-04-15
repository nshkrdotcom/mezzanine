defmodule MezzanineProgramSurfaceTest do
  use ExUnit.Case, async: false

  alias Ecto.Adapters.SQL.Sandbox
  alias Mezzanine.OpsDomain.Repo
  alias Mezzanine.Surfaces.ProgramSurface

  setup do
    pid = Sandbox.start_owner!(Repo, shared: false)
    on_exit(fn -> Sandbox.stop_owner(pid) end)
    :ok
  end

  test "creates, updates, and transitions programs" do
    tenant_id = "tenant-program-surface"

    assert {:ok, program} =
             ProgramSurface.create_program(tenant_id, %{
               slug: "program-surface",
               name: "Program Surface",
               product_family: "operator_stack",
               configuration: %{"region" => "local"},
               metadata: %{}
             })

    assert {:ok, updated_program} =
             ProgramSurface.update_program(tenant_id, program.id, %{
               name: "Updated Program Surface",
               configuration: %{"region" => "cluster"}
             })

    assert updated_program.name == "Updated Program Surface"
    assert ProgramSurface.program_id(program) == program.id
    assert ProgramSurface.program_slug(program) == "program-surface"
    assert ProgramSurface.program_name(updated_program) == "Updated Program Surface"

    assert {:ok, active_program} = ProgramSurface.activate_program(tenant_id, program.id)
    assert active_program.status == :active
    assert ProgramSurface.program_status(active_program) == :active

    assert {:ok, suspended_program} = ProgramSurface.suspend_program(tenant_id, program.id)
    assert suspended_program.status == :suspended

    assert {:ok, programs} = ProgramSurface.list_programs(tenant_id)
    assert Enum.any?(programs, &(&1.id == program.id))
  end

  test "manages policy bundles, work classes, and placement profiles" do
    tenant_id = "tenant-program-admin"

    assert {:ok, program} =
             ProgramSurface.create_program(tenant_id, %{
               slug: "program-admin",
               name: "Program Admin",
               product_family: "operator_stack",
               configuration: %{},
               metadata: %{}
             })

    assert {:ok, bundle} =
             ProgramSurface.load_policy_bundle(tenant_id, program.id, %{
               name: "default",
               version: "1.0.0",
               policy_kind: :workflow_md,
               source_ref: "WORKFLOW.md",
               body: workflow_body(),
               metadata: %{}
             })

    assert {:ok, recompiled_bundle} =
             ProgramSurface.recompile_policy_bundle(tenant_id, bundle.id, %{
               version: "1.0.1",
               source_ref: "WORKFLOW.md",
               body: workflow_body(),
               metadata: %{"revision" => 2}
             })

    assert recompiled_bundle.version == "1.0.1"
    assert ProgramSurface.policy_bundle_id(bundle) == bundle.id
    assert ProgramSurface.policy_bundle_name(bundle) == "default"

    assert {:ok, work_class} =
             ProgramSurface.create_work_class(tenant_id, program.id, %{
               name: "coding_task",
               kind: "coding_task",
               intake_schema: %{"required" => ["title"]},
               policy_bundle_id: bundle.id,
               default_review_profile: %{"required" => true},
               default_run_profile: %{"runtime" => "session"}
             })

    assert {:ok, updated_work_class} =
             ProgramSurface.update_work_class(tenant_id, work_class.id, %{
               intake_schema: %{"required" => ["title", "description"]},
               default_run_profile: %{"runtime" => "direct"},
               default_review_profile: %{"required" => false}
             })

    assert updated_work_class.intake_schema["required"] == ["title", "description"]
    assert updated_work_class.default_run_profile["runtime"] == "direct"
    assert updated_work_class.default_review_profile["required"] == false
    assert ProgramSurface.work_class_id(work_class) == work_class.id
    assert ProgramSurface.work_class_name(work_class) == "coding_task"

    assert {:ok, work_classes} = ProgramSurface.list_work_classes(tenant_id, program.id)
    assert Enum.any?(work_classes, &(&1.id == work_class.id))

    assert {:ok, placement_profile} =
             ProgramSurface.create_placement_profile(tenant_id, program.id, %{
               profile_id: "default-placement",
               strategy: "affinity",
               target_selector: %{"runtime_driver" => "jido_session"},
               runtime_preferences: %{"locality" => "same_region"},
               workspace_policy: %{"root_mode" => "per_work"},
               metadata: %{}
             })

    assert {:ok, updated_profile} =
             ProgramSurface.update_placement_profile(tenant_id, placement_profile.id, %{
               runtime_preferences: %{"locality" => "cross_region"},
               metadata: %{"revised" => true}
             })

    assert updated_profile.runtime_preferences["locality"] == "cross_region"

    assert {:ok, active_profile} =
             ProgramSurface.activate_placement_profile(tenant_id, placement_profile.id)

    assert active_profile.status == :active
    assert ProgramSurface.placement_profile_id(placement_profile) == placement_profile.id
    assert ProgramSurface.placement_profile_profile_id(placement_profile) == "default-placement"
    assert ProgramSurface.placement_profile_status(active_profile) == :active

    assert {:ok, placement_profiles} =
             ProgramSurface.list_placement_profiles(tenant_id, program.id)

    assert Enum.any?(placement_profiles, &(&1.id == placement_profile.id))
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
