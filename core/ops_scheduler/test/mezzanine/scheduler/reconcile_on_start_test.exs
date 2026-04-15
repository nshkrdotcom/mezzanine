defmodule Mezzanine.Scheduler.ReconcileOnStartTest do
  use Mezzanine.OpsScheduler.DataCase, async: false

  alias Ash
  alias Mezzanine.Programs.Program
  alias Mezzanine.Runs.{Run, RunSeries}
  alias Mezzanine.Scheduler.ReconcileOnStart
  alias Mezzanine.Work.{WorkClass, WorkObject}

  test "summarizes mixed running, scheduled, and stalled state on startup" do
    actor = %{tenant_id: "tenant-a"}
    {:ok, work_object} = create_work(actor)

    {:ok, run_series} =
      RunSeries
      |> Ash.Changeset.for_create(:open_series, %{work_object_id: work_object.id})
      |> Ash.Changeset.set_tenant(actor.tenant_id)
      |> Ash.create(actor: actor, domain: Mezzanine.Runs)

    {:ok, scheduled_run} =
      Run
      |> Ash.Changeset.for_create(:schedule, %{
        run_series_id: run_series.id,
        attempt: 1,
        runtime_profile: %{"runtime" => "session"}
      })
      |> Ash.Changeset.set_tenant(actor.tenant_id)
      |> Ash.create(actor: actor, domain: Mezzanine.Runs)

    {:ok, running_run} =
      Run
      |> Ash.Changeset.for_create(:schedule, %{
        run_series_id: run_series.id,
        attempt: 2,
        runtime_profile: %{"runtime" => "session"}
      })
      |> Ash.Changeset.set_tenant(actor.tenant_id)
      |> Ash.create(actor: actor, domain: Mezzanine.Runs)

    {:ok, _running_run} =
      running_run
      |> Ash.Changeset.for_update(:record_started, %{
        raw_runtime_ref: "runtime:live",
        started_at: ~U[2026-04-14 18:55:00Z]
      })
      |> Ash.Changeset.set_tenant(actor.tenant_id)
      |> Ash.update(actor: actor, domain: Mezzanine.Runs)

    assert {:ok, summary} = ReconcileOnStart.reconcile(actor.tenant_id, ~U[2026-04-14 19:00:00Z])

    assert Enum.map(summary.scheduled_runs, & &1.id) == [scheduled_run.id]
    assert Enum.count(summary.running_runs) == 1
    assert summary.stalled_runs == []
  end

  defp create_work(actor) do
    {:ok, program} =
      Program
      |> Ash.Changeset.for_create(:create_program, %{
        slug: Ecto.UUID.generate(),
        name: "Reconcile Program",
        product_family: "operator_stack",
        configuration: %{},
        metadata: %{}
      })
      |> Ash.Changeset.set_tenant(actor.tenant_id)
      |> Ash.create(actor: actor, domain: Mezzanine.Programs)

    {:ok, work_class} =
      WorkClass
      |> Ash.Changeset.for_create(:create_work_class, %{
        program_id: program.id,
        name: "reconcile_task_#{System.unique_integer([:positive])}",
        kind: "generic_task",
        intake_schema: %{},
        default_review_profile: %{},
        default_run_profile: %{"runtime" => "session"}
      })
      |> Ash.Changeset.set_tenant(actor.tenant_id)
      |> Ash.create(actor: actor, domain: Mezzanine.Work)

    WorkObject
    |> Ash.Changeset.for_create(:ingest, %{
      program_id: program.id,
      work_class_id: work_class.id,
      external_ref: "reconcile:1",
      title: "Reconcilable work",
      description: "Reconcile fixture",
      priority: 10,
      source_kind: "api",
      payload: %{},
      normalized_payload: %{}
    })
    |> Ash.Changeset.set_tenant(actor.tenant_id)
    |> Ash.create(actor: actor, domain: Mezzanine.Work)
  end
end
