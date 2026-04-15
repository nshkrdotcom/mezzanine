defmodule Mezzanine.Scheduler.LeaseManagerTest do
  use Mezzanine.OpsScheduler.DataCase, async: false

  alias Ash
  alias Mezzanine.Programs.Program
  alias Mezzanine.Scheduler.LeaseManager
  alias Mezzanine.Work.{WorkClass, WorkObject}

  test "claims work, prevents double-claim, and allows claim after expiry" do
    actor = %{tenant_id: "tenant-a"}

    {:ok, work_object} = create_work(actor)

    assert {:ok, claimed_work} =
             LeaseManager.claim(
               actor.tenant_id,
               work_object.id,
               "scheduler-a",
               100,
               ~U[2026-04-14 19:00:00Z]
             )

    assert claimed_work.lease_owner == "scheduler-a"

    assert {:error, :already_claimed} =
             LeaseManager.claim(
               actor.tenant_id,
               work_object.id,
               "scheduler-b",
               100,
               ~U[2026-04-14 19:00:00Z]
             )

    assert {:ok, reclaimed_work} =
             LeaseManager.claim(
               actor.tenant_id,
               work_object.id,
               "scheduler-b",
               100,
               ~U[2026-04-14 19:10:00Z]
             )

    assert reclaimed_work.lease_owner == "scheduler-b"
  end

  defp create_work(actor) do
    {:ok, program} =
      Program
      |> Ash.Changeset.for_create(:create_program, %{
        slug: Ecto.UUID.generate(),
        name: "Lease Program",
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
        name: "lease_task_#{System.unique_integer([:positive])}",
        kind: "generic_task",
        intake_schema: %{},
        default_review_profile: %{},
        default_run_profile: %{"runtime" => "session"}
      })
      |> Ash.Changeset.set_tenant(actor.tenant_id)
      |> Ash.create(actor: actor, domain: Mezzanine.Work)

    with {:ok, work_object} <-
           WorkObject
           |> Ash.Changeset.for_create(:ingest, %{
             program_id: program.id,
             work_class_id: work_class.id,
             external_ref: "lease:1",
             title: "Leasable work",
             description: "Lease fixture",
             priority: 10,
             source_kind: "api",
             payload: %{},
             normalized_payload: %{}
           })
           |> Ash.Changeset.set_tenant(actor.tenant_id)
           |> Ash.create(actor: actor, domain: Mezzanine.Work) do
      work_object
      |> Ash.Changeset.for_update(:mark_planned, %{})
      |> Ash.Changeset.set_tenant(actor.tenant_id)
      |> Ash.update(actor: actor, domain: Mezzanine.Work)
    end
  end
end
