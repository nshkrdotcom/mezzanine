defmodule Mezzanine.Scheduler.RetryQueueTest do
  use Mezzanine.OpsScheduler.DataCase, async: false

  alias Ash
  alias Mezzanine.Programs.{PolicyBundle, Program}
  alias Mezzanine.Runs.{Run, RunSeries}
  alias Mezzanine.Scheduler.RetryQueue
  alias Mezzanine.Work.{WorkClass, WorkObject}

  @workflow_fixture Path.expand("../../../../ops_policy/test/fixtures/workflow.md", __DIR__)

  test "returns only due failed runs that still have retry budget" do
    actor = %{tenant_id: "tenant-a"}
    {:ok, work_object} = create_planned_work(actor, "linear:ENG-2001")
    {:ok, due_run} = create_failed_run(actor, work_object, 1, ~U[2026-04-14 18:00:00Z])
    {:ok, _exhausted_run} = create_failed_run(actor, work_object, 4, ~U[2026-04-14 18:00:00Z])

    assert {:ok, due_items} = RetryQueue.due_runs(actor.tenant_id, ~U[2026-04-14 19:00:00Z])

    assert Enum.map(due_items, & &1.run.id) == [due_run.id]
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

  defp create_failed_run(actor, work_object, attempt, completed_at) do
    {:ok, run_series} =
      RunSeries
      |> Ash.Changeset.for_create(:open_series, %{work_object_id: work_object.id})
      |> Ash.Changeset.set_tenant(actor.tenant_id)
      |> Ash.create(actor: actor, domain: Mezzanine.Runs)

    {:ok, run} =
      Run
      |> Ash.Changeset.for_create(:schedule, %{
        run_series_id: run_series.id,
        attempt: attempt,
        runtime_profile: %{"runtime" => "session"}
      })
      |> Ash.Changeset.set_tenant(actor.tenant_id)
      |> Ash.create(actor: actor, domain: Mezzanine.Runs)

    run
    |> Ash.Changeset.for_update(:record_failed, %{
      completed_at: completed_at,
      result_summary: "Run failed"
    })
    |> Ash.Changeset.set_tenant(actor.tenant_id)
    |> Ash.update(actor: actor, domain: Mezzanine.Runs)
  end

  defp create_program(actor) do
    Program
    |> Ash.Changeset.for_create(:create_program, %{
      slug: Ecto.UUID.generate(),
      name: "Retry Program",
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
      name: "retry_task_#{System.unique_integer([:positive])}",
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
      title: "Retryable work",
      description: "Scheduler retry fixture",
      priority: 50,
      source_kind: "linear",
      payload: %{"issue_id" => external_ref},
      normalized_payload: %{"issue_id" => external_ref}
    })
    |> Ash.Changeset.set_tenant(actor.tenant_id)
    |> Ash.create(actor: actor, domain: Mezzanine.Work)
  end
end
