defmodule Mezzanine.OpsDomainPlanningTest do
  use Mezzanine.OpsDomain.DataCase, async: false

  alias Ash
  alias Mezzanine.Programs.{PolicyBundle, Program}
  alias Mezzanine.Work.{WorkClass, WorkObject, WorkPlan}

  @workflow_fixture Path.expand("../fixtures/workflow.md", __DIR__)

  test "policy bundles compile on load and preserve last good compiled form on failed recompile" do
    actor = %{tenant_id: "tenant-a"}
    {:ok, program} = create_program(actor, "extravaganza")
    {:ok, body} = File.read(@workflow_fixture)

    assert {:ok, %PolicyBundle{} = bundle} =
             PolicyBundle
             |> Ash.Changeset.for_create(:load_bundle, %{
               program_id: program.id,
               name: "linear_default",
               version: "1.0.0",
               policy_kind: :workflow_md,
               source_ref: "WORKFLOW.md",
               body: body
             })
             |> Ash.Changeset.set_tenant(actor.tenant_id)
             |> Ash.create(actor: actor, domain: Mezzanine.Programs)

    assert bundle.status == :compiled
    assert bundle.config["review"]["required"] == true
    assert bundle.compiled_form["run_profile"]["capability"] == "linear.issue.execute"
    assert bundle.compiled_form["approval_posture"]["escalation_required"] == true
    assert bundle.compiled_form["review_rules"]["required"] == true
    assert bundle.compiled_form["retry_profile"]["strategy"] == "exponential"

    assert {:ok, %PolicyBundle{} = stale_bundle} =
             bundle
             |> Ash.Changeset.for_update(:recompile, %{
               body: """
               ---
               approval:
                 mode: nonsense
               ---
               Broken policy.
               """,
               version: "1.0.1"
             })
             |> Ash.Changeset.set_tenant(actor.tenant_id)
             |> Ash.update(actor: actor, domain: Mezzanine.Programs)

    assert stale_bundle.status == :stale_on_error
    assert stale_bundle.compiled_form == bundle.compiled_form
    assert stale_bundle.metadata["compile_error"]
  end

  test "structured config policy bundles keep prompt body separate from runtime config" do
    actor = %{tenant_id: "tenant-structured-policy"}
    {:ok, program} = create_program(actor, "structured-policy")

    assert {:ok, %PolicyBundle{} = bundle} =
             PolicyBundle
             |> Ash.Changeset.for_create(:load_bundle, %{
               program_id: program.id,
               name: "structured_default",
               version: "1.0.0",
               policy_kind: :structured_config,
               source_ref: "structured://default",
               body: "# Prompt Only\n\nOperate on the assigned work.",
               metadata: %{
                 "runtime_policy_config" => %{
                   "run" => %{
                     "profile" => "default_session",
                     "runtime_class" => "session",
                     "capability" => "linear.issue.execute",
                     "target" => "linear-default"
                   },
                   "review" => %{
                     "required" => true,
                     "required_decisions" => 1,
                     "gates" => ["operator"]
                   },
                   "retry" => %{"strategy" => "linear", "max_attempts" => 2}
                 }
               }
             })
             |> Ash.Changeset.set_tenant(actor.tenant_id)
             |> Ash.create(actor: actor, domain: Mezzanine.Programs)

    assert bundle.status == :compiled
    assert bundle.prompt_template =~ "Prompt Only"
    assert bundle.config["review"]["required"] == true
    assert bundle.compiled_form["run_profile"]["capability"] == "linear.issue.execute"
    assert bundle.compiled_form["review_rules"]["required"] == true
    assert bundle.compiled_form["retry_profile"]["strategy"] == "linear"
  end

  test "compile_plan persists a work plan and attaches it to the work object" do
    actor = %{tenant_id: "tenant-a"}
    {:ok, program} = create_program(actor, "mezzanine-core")
    {:ok, body} = File.read(@workflow_fixture)

    {:ok, bundle} =
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

    {:ok, work_class} =
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

    {:ok, work_object} =
      WorkObject
      |> Ash.Changeset.for_create(:ingest, %{
        program_id: program.id,
        work_class_id: work_class.id,
        external_ref: "linear:ENG-900",
        title: "Plan governed execution",
        description: "Persist the first work plan",
        priority: 90,
        source_kind: "linear",
        payload: %{"issue_id" => "ENG-900"},
        normalized_payload: %{"issue_id" => "ENG-900"}
      })
      |> Ash.Changeset.set_tenant(actor.tenant_id)
      |> Ash.create(actor: actor, domain: Mezzanine.Work)

    assert {:ok, %WorkObject{} = planned_work_object} =
             work_object
             |> Ash.Changeset.for_update(:compile_plan, %{})
             |> Ash.Changeset.set_tenant(actor.tenant_id)
             |> Ash.update(actor: actor, domain: Mezzanine.Work)

    assert planned_work_object.status == :planned
    assert planned_work_object.current_plan_id

    assert {:ok, [%WorkPlan{} = plan]} =
             WorkPlan
             |> Ash.Query.for_read(:list_for_work_object, %{work_object_id: work_object.id})
             |> Ash.Query.set_tenant(actor.tenant_id)
             |> Ash.read(actor: actor, domain: Mezzanine.Work)

    assert plan.id == planned_work_object.current_plan_id
    assert plan.status == :compiled
    assert plan.version == 1
    assert length(plan.derived_run_intents) == 1
    assert length(plan.derived_review_intents) == 1
    assert length(plan.obligation_ids) == 1
    assert plan.plan_payload["work_id"] == work_object.id
    assert plan.metadata["retry_profile"]["strategy"] == "exponential"
  end

  test "compile_plan omits review intents when persisted workflow policy disables review" do
    actor = %{tenant_id: "tenant-a"}
    {:ok, program} = create_program(actor, "mezzanine-no-review")

    {:ok, bundle} =
      PolicyBundle
      |> Ash.Changeset.for_create(:load_bundle, %{
        program_id: program.id,
        name: "no_review",
        version: "1.0.0",
        policy_kind: :workflow_md,
        source_ref: "WORKFLOW.md",
        body: """
        ---
        run:
          capability: work.execute
          runtime_class: session
        review:
          required: false
          required_decisions: 0
        ---
        Execute the work.
        """
      })
      |> Ash.Changeset.set_tenant(actor.tenant_id)
      |> Ash.create(actor: actor, domain: Mezzanine.Programs)

    assert bundle.config["review"]["required"] == false
    assert bundle.compiled_form["review_rules"]["required"] == false

    {:ok, work_class} =
      WorkClass
      |> Ash.Changeset.for_create(:create_work_class, %{
        program_id: program.id,
        name: "request_task",
        kind: "request",
        intake_schema: %{"required" => ["title"]},
        policy_bundle_id: bundle.id,
        default_review_profile: %{"required" => false},
        default_run_profile: %{"runtime" => "session"}
      })
      |> Ash.Changeset.set_tenant(actor.tenant_id)
      |> Ash.create(actor: actor, domain: Mezzanine.Work)

    {:ok, work_object} =
      WorkObject
      |> Ash.Changeset.for_create(:ingest, %{
        program_id: program.id,
        work_class_id: work_class.id,
        external_ref: "request:ENG-901",
        title: "Plan unreviewed execution",
        description: "Persist a work plan without review intents",
        priority: 50,
        source_kind: "app_kit",
        payload: %{"request_id" => "ENG-901"},
        normalized_payload: %{"request_id" => "ENG-901"}
      })
      |> Ash.Changeset.set_tenant(actor.tenant_id)
      |> Ash.create(actor: actor, domain: Mezzanine.Work)

    assert {:ok, %WorkObject{} = planned_work_object} =
             work_object
             |> Ash.Changeset.for_update(:compile_plan, %{})
             |> Ash.Changeset.set_tenant(actor.tenant_id)
             |> Ash.update(actor: actor, domain: Mezzanine.Work)

    assert {:ok, [%WorkPlan{} = plan]} =
             WorkPlan
             |> Ash.Query.for_read(:list_for_work_object, %{work_object_id: work_object.id})
             |> Ash.Query.set_tenant(actor.tenant_id)
             |> Ash.read(actor: actor, domain: Mezzanine.Work)

    assert plan.id == planned_work_object.current_plan_id
    assert plan.derived_review_intents == []
    assert plan.obligation_ids == []
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
end
