defmodule MezzanineConfigRegistryTest do
  use Mezzanine.ConfigRegistry.DataCase, async: false

  alias Ash.Error.Invalid
  alias Mezzanine.ConfigRegistry.{Installation, PackRegistration}

  alias Mezzanine.Pack.{
    CompiledPack,
    Compiler,
    ExecutionRecipeSpec,
    LifecycleSpec,
    Manifest,
    ProjectionSpec,
    Serializer,
    SubjectKindSpec
  }

  alias Mezzanine.Pack.Registry

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

  test "pack activation rejects overlapping canonical subject kinds and allows distinct active packs" do
    first_registration =
      fixture_pack(pack_slug: :expense_approval, version: "1.0.0", subject_kind: :expense_request)
      |> MezzanineConfigRegistry.register_pack!()

    assert {:ok, %PackRegistration{status: :active}} =
             PackRegistration.activate(first_registration)

    overlapping_registration =
      fixture_pack(pack_slug: :invoice_ops, version: "1.0.0", subject_kind: :expense_request)
      |> MezzanineConfigRegistry.register_pack!()

    assert {:error, %Invalid{} = error} = PackRegistration.activate(overlapping_registration)
    assert Exception.message(error) =~ "canonical subject kinds"
    assert Exception.message(error) =~ "expense_request"

    assert {:ok, %PackRegistration{status: :registered}} =
             Ash.get(PackRegistration, overlapping_registration.id)

    distinct_registration =
      fixture_pack(pack_slug: :invoice_ops, version: "1.0.1", subject_kind: :invoice_request)
      |> MezzanineConfigRegistry.register_pack!()

    assert {:ok, %PackRegistration{status: :active}} =
             PackRegistration.activate(distinct_registration)
  end

  test "serializer reload keeps projection field identifiers neutral and preserves booleans" do
    projection_field = "field__never_preexisting_20260416"

    payload =
      fixture_pack()
      |> Serializer.serialize_compiled()
      |> put_in(
        ["manifest", "execution_recipe_specs", Access.at(0), "workspace_policy", "reuse"],
        "true"
      )
      |> put_in(
        ["manifest", "projection_specs", Access.at(0), "default_filters"],
        %{projection_field => true}
      )
      |> put_in(
        ["manifest", "projection_specs", Access.at(0), "sort"],
        [%{"field" => projection_field, "dir" => "asc"}]
      )
      |> put_in(
        ["manifest", "projection_specs", Access.at(0), "included_fields"],
        [projection_field]
      )

    assert {:ok, %CompiledPack{} = compiled} = Serializer.deserialize_compiled(payload)

    [projection] = compiled.manifest.projection_specs
    [recipe] = compiled.manifest.execution_recipe_specs

    assert projection.default_filters == %{projection_field => true}
    assert projection.sort == [{projection_field, :asc}]
    assert projection.included_fields == [projection_field]
    assert recipe.workspace_policy[:reuse] == true
  end

  defp register_fixture_pack! do
    fixture_pack()
    |> MezzanineConfigRegistry.register_pack!()
  end

  defp fixture_pack(opts \\ []) do
    pack_slug = Keyword.get(opts, :pack_slug, :expense_approval)
    version = Keyword.get(opts, :version, "1.0.0")
    subject_kind = Keyword.get(opts, :subject_kind, :expense_request)
    recipe_ref = Keyword.get(opts, :recipe_ref, :"#{subject_kind}_capture")
    terminal_state = Keyword.get(opts, :terminal_state, :"#{subject_kind}_done")
    projection_name = Keyword.get(opts, :projection_name, :"active_#{subject_kind}")

    manifest = %Manifest{
      pack_slug: pack_slug,
      version: version,
      subject_kind_specs: [
        %SubjectKindSpec{name: subject_kind}
      ],
      lifecycle_specs: [
        %LifecycleSpec{
          subject_kind: subject_kind,
          initial_state: :submitted,
          terminal_states: [terminal_state],
          transitions: [
            %{
              from: :submitted,
              to: :processing,
              trigger: {:execution_requested, recipe_ref}
            },
            %{from: :processing, to: terminal_state, trigger: {:execution_completed, recipe_ref}}
          ]
        }
      ],
      execution_recipe_specs: [
        %ExecutionRecipeSpec{
          recipe_ref: recipe_ref,
          runtime_class: :session,
          placement_ref: :local_runner,
          workspace_policy: %{strategy: :per_subject, reuse: true, cleanup: :on_terminal}
        }
      ],
      projection_specs: [
        %ProjectionSpec{name: projection_name, subject_kinds: [subject_kind]}
      ]
    }

    case Compiler.compile(manifest) do
      {:ok, %CompiledPack{} = compiled_pack} -> compiled_pack
      {:error, errors} -> raise "failed to compile registry fixture pack: #{inspect(errors)}"
    end
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
