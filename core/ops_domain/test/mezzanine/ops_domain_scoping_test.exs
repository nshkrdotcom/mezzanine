defmodule Mezzanine.OpsDomainScopingTest do
  use Mezzanine.OpsDomain.DataCase, async: false

  alias Ash
  alias Mezzanine.Programs.Program
  alias Mezzanine.Work.WorkClass
  alias Mezzanine.Work.WorkObject

  test "program reads are tenant-scoped and activation preserves program identity" do
    actor = %{tenant_id: "tenant-a"}

    assert {:ok, %Program{} = program} =
             Program
             |> Ash.Changeset.for_create(:create_program, %{
               slug: "extravaganza",
               name: "Extravaganza",
               product_family: "operator_stack",
               configuration: %{"feature_flags" => ["linear"]},
               metadata: %{}
             })
             |> Ash.Changeset.set_tenant(actor.tenant_id)
             |> Ash.create(actor: actor, domain: Mezzanine.Programs)

    assert {:ok, [%Program{id: program_id}]} =
             Program
             |> Ash.Query.for_read(:list_for_tenant, %{tenant_id: "tenant-a"})
             |> Ash.Query.set_tenant(actor.tenant_id)
             |> Ash.read(actor: actor, domain: Mezzanine.Programs)

    assert program_id == program.id

    assert {:ok, []} =
             Program
             |> Ash.Query.for_read(:list_for_tenant, %{tenant_id: "tenant-b"})
             |> Ash.Query.set_tenant("tenant-b")
             |> Ash.read(actor: %{tenant_id: "tenant-b"}, domain: Mezzanine.Programs)

    assert {:ok, %Program{status: :active}} =
             program
             |> Ash.Changeset.for_update(:activate, %{})
             |> Ash.Changeset.set_tenant(actor.tenant_id)
             |> Ash.update(actor: actor, domain: Mezzanine.Programs)
  end

  test "work reads are scoped to both tenant and program" do
    actor = %{tenant_id: "tenant-a"}

    {:ok, %Program{} = program} =
      Program
      |> Ash.Changeset.for_create(:create_program, %{
        slug: "mezzanine-core",
        name: "Mezzanine Core",
        product_family: "core",
        configuration: %{},
        metadata: %{}
      })
      |> Ash.Changeset.set_tenant(actor.tenant_id)
      |> Ash.create(actor: actor, domain: Mezzanine.Programs)

    {:ok, %WorkClass{} = work_class} =
      WorkClass
      |> Ash.Changeset.for_create(:create_work_class, %{
        program_id: program.id,
        name: "coding_task",
        kind: "coding_task",
        intake_schema: %{"required" => ["title"]},
        default_review_profile: %{"required" => true},
        default_run_profile: %{"runtime" => "session"}
      })
      |> Ash.Changeset.set_tenant(actor.tenant_id)
      |> Ash.create(actor: actor, domain: Mezzanine.Work)

    {:ok, %WorkObject{} = work_object} =
      WorkObject
      |> Ash.Changeset.for_create(:ingest, %{
        program_id: program.id,
        work_class_id: work_class.id,
        external_ref: "linear:ENG-101",
        title: "Implement governed intake",
        description: "Create the first durable work object",
        priority: 80,
        source_kind: "linear",
        payload: %{"issue_id" => "ENG-101"},
        normalized_payload: %{"title" => "Implement governed intake"}
      })
      |> Ash.Changeset.set_tenant(actor.tenant_id)
      |> Ash.create(actor: actor, domain: Mezzanine.Work)

    assert {:ok, [%WorkObject{id: returned_id}]} =
             WorkObject
             |> Ash.Query.for_read(:list_for_program, %{program_id: program.id})
             |> Ash.Query.set_tenant(actor.tenant_id)
             |> Ash.read(actor: actor, domain: Mezzanine.Work)

    assert returned_id == work_object.id

    assert {:ok, []} =
             WorkObject
             |> Ash.Query.for_read(:list_for_program, %{program_id: program.id})
             |> Ash.Query.set_tenant("tenant-b")
             |> Ash.read(actor: %{tenant_id: "tenant-b"}, domain: Mezzanine.Work)

    assert {:ok, %WorkObject{status: :running}} =
             work_object
             |> Ash.Changeset.for_update(:mark_running, %{})
             |> Ash.Changeset.set_tenant(actor.tenant_id)
             |> Ash.update(actor: actor, domain: Mezzanine.Work)
  end
end
