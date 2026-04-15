defmodule Mezzanine.Scheduler.StallDetectorTest do
  use Mezzanine.OpsScheduler.DataCase, async: false

  alias Ash
  alias Mezzanine.Programs.Program
  alias Mezzanine.Runs.{Run, RunSeries}
  alias Mezzanine.Scheduler.StallDetector
  alias Mezzanine.Work.{WorkClass, WorkObject}

  test "returns running runs older than the threshold" do
    actor = %{tenant_id: "tenant-a"}
    {:ok, work_object} = create_work(actor)

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

    {:ok, running_run} =
      run
      |> Ash.Changeset.for_update(:record_started, %{
        raw_runtime_ref: "runtime:stalled",
        started_at: ~U[2026-04-14 18:00:00Z]
      })
      |> Ash.Changeset.set_tenant(actor.tenant_id)
      |> Ash.update(actor: actor, domain: Mezzanine.Runs)

    assert {:ok, stalled_runs} =
             StallDetector.stalled_runs(actor.tenant_id, 60_000, ~U[2026-04-14 19:00:00Z])

    assert Enum.map(stalled_runs, & &1.id) == [running_run.id]
  end

  defp create_work(actor) do
    {:ok, program} =
      Program
      |> Ash.Changeset.for_create(:create_program, %{
        slug: Ecto.UUID.generate(),
        name: "Stall Program",
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
        name: "stall_task_#{System.unique_integer([:positive])}",
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
      external_ref: "stall:1",
      title: "Stallable work",
      description: "Stall fixture",
      priority: 10,
      source_kind: "api",
      payload: %{},
      normalized_payload: %{}
    })
    |> Ash.Changeset.set_tenant(actor.tenant_id)
    |> Ash.create(actor: actor, domain: Mezzanine.Work)
  end
end
