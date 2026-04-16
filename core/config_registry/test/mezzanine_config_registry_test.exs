defmodule MezzanineConfigRegistryTest do
  use Mezzanine.ConfigRegistry.DataCase, async: false

  alias Mezzanine.ConfigRegistry.{Installation, PackRegistration}
  alias Mezzanine.Pack.Registry
  alias Mezzanine.TestPacks.RegistryFixturePack

  test "register_pack persists compiled payload and canonical subject kinds" do
    compiled = fixture_pack()

    assert {:ok, %PackRegistration{} = registration} =
             MezzanineConfigRegistry.register_pack(compiled)

    assert registration.pack_slug == "expense_approval"
    assert registration.version == "1.0.0"
    assert registration.status == :registered
    assert registration.canonical_subject_kinds == ["expense_request"]
    assert is_map(registration.compiled_manifest)
  end

  test "installation activation and binding updates persist revisioned runtime state" do
    registration = register_fixture_pack!()

    assert {:ok, %Installation{} = installation} =
             MezzanineConfigRegistry.create_installation(%{
               tenant_id: "tenant-a",
               environment: "prod",
               pack_registration_id: registration.id
             })

    assert installation.status == :inactive
    assert installation.compiled_pack_revision == 1

    assert {:ok, %Installation{} = active_installation} =
             MezzanineConfigRegistry.activate_installation(installation)

    assert active_installation.status == :active

    assert {:ok, %Installation{} = updated_installation} =
             MezzanineConfigRegistry.update_bindings(active_installation, %{
               "execution_bindings" => %{
                 "expense_capture" => %{
                   "placement_ref" => "local_docker"
                 }
               }
             })

    assert updated_installation.binding_config["execution_bindings"]["expense_capture"][
             "placement_ref"
           ] ==
             "local_docker"

    assert updated_installation.compiled_pack_revision == 2
  end

  test "runtime registry serves warm cache hits without a database query" do
    registration = register_fixture_pack!()

    {:ok, installation} =
      MezzanineConfigRegistry.create_installation(%{
        tenant_id: "tenant-a",
        environment: "prod",
        pack_registration_id: registration.id
      })

    {:ok, installation} = MezzanineConfigRegistry.activate_installation(installation)

    assert {:ok, compiled} =
             Registry.get_compiled_pack(installation.id, installation.compiled_pack_revision)

    assert query_count(fn ->
             assert {:ok, cached_compiled} =
                      Registry.get_compiled_pack(
                        installation.id,
                        installation.compiled_pack_revision
                      )

             assert cached_compiled.pack_slug == compiled.pack_slug
           end) == 0
  end

  defp register_fixture_pack! do
    fixture_pack()
    |> MezzanineConfigRegistry.register_pack!()
  end

  defp fixture_pack do
    RegistryFixturePack.compiled_pack!()
  end

  defp query_count(fun) do
    handler_id = {__MODULE__, make_ref()}
    parent = self()

    :telemetry.attach(
      handler_id,
      [:mezzanine_config_registry, :repo, :query],
      &__MODULE__.handle_repo_query/4,
      %{parent: parent, tag: handler_id}
    )

    try do
      fun.()
      drain_queries(handler_id, 0)
    after
      :telemetry.detach(handler_id)
    end
  end

  defp drain_queries(tag, count) do
    receive do
      {:repo_query, ^tag} -> drain_queries(tag, count + 1)
    after
      0 -> count
    end
  end

  def handle_repo_query(_event, _measurements, _metadata, %{parent: parent, tag: tag}) do
    send(parent, {:repo_query, tag})
  end
end
