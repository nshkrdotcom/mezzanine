defmodule Mezzanine.Scheduler.WorkSelectorTest do
  use Mezzanine.OpsScheduler.DataCase, async: false

  alias Ash
  alias Mezzanine.Programs.{PolicyBundle, Program}
  alias Mezzanine.Scheduler.WorkSelector
  alias Mezzanine.Work.{WorkClass, WorkObject}

  @workflow_fixture Path.expand("../../../../ops_policy/test/fixtures/workflow.md", __DIR__)

  test "selects only planned and currently unleased work" do
    actor = %{tenant_id: "tenant-a"}
    {:ok, ready_work} = create_planned_work(actor, "linear:ENG-1001")
    {:ok, blocked_work} = create_planned_work(actor, "linear:ENG-1002")

    {:ok, _blocked} =
      blocked_work
      |> Ash.Changeset.for_update(:block, %{blocked_by_work_id: ready_work.id})
      |> Ash.Changeset.set_tenant(actor.tenant_id)
      |> Ash.update(actor: actor, domain: Mezzanine.Work)

    assert {:ok, due_work} = WorkSelector.ready_work(actor.tenant_id)
    assert Enum.map(due_work, & &1.id) == [ready_work.id]
  end

  test "returns an empty list when nothing is schedulable" do
    assert {:ok, []} = WorkSelector.ready_work("tenant-empty")
  end

  defp create_planned_work(actor, external_ref) do
    with {:ok, program} <- create_program(actor),
         {:ok, bundle} <- create_policy_bundle(actor, program),
         {:ok, work_class} <- create_work_class(actor, program, bundle),
         {:ok, work_object} <- create_work_object(actor, program, work_class, external_ref) do
      work_object
      |> Ash.Changeset.for_update(:compile_plan, %{})
      |> Ash.Changeset.set_tenant(actor.tenant_id)
      |> Ash.update(actor: actor, domain: Mezzanine.Work)
    end
  end

  defp create_program(actor) do
    Program
    |> Ash.Changeset.for_create(:create_program, %{
      slug: Ecto.UUID.generate(),
      name: "Scheduler Program",
      product_family: "operator_stack",
      configuration: %{},
      metadata: %{}
    })
    |> Ash.Changeset.set_tenant(actor.tenant_id)
    |> Ash.create(actor: actor, domain: Mezzanine.Programs)
  end

  defp create_policy_bundle(actor, program) do
    {:ok, body} = File.read(@workflow_fixture)

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
      name: "coding_task_#{System.unique_integer([:positive])}",
      kind: "coding_task",
      intake_schema: %{"required" => ["title"]},
      policy_bundle_id: bundle.id,
      default_review_profile: %{"required" => true},
      default_run_profile: %{"runtime" => "session"}
    })
    |> Ash.Changeset.set_tenant(actor.tenant_id)
    |> Ash.create(actor: actor, domain: Mezzanine.Work)
  end

  defp create_work_object(actor, program, work_class, external_ref) do
    WorkObject
    |> Ash.Changeset.for_create(:ingest, %{
      program_id: program.id,
      work_class_id: work_class.id,
      external_ref: external_ref,
      title: "Selectable work",
      description: "Scheduler selection fixture",
      priority: 50,
      source_kind: "linear",
      payload: %{"issue_id" => external_ref},
      normalized_payload: %{"issue_id" => external_ref}
    })
    |> Ash.Changeset.set_tenant(actor.tenant_id)
    |> Ash.create(actor: actor, domain: Mezzanine.Work)
  end
end
