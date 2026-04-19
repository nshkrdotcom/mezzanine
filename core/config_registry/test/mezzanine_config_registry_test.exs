defmodule MezzanineConfigRegistryTest do
  use Mezzanine.ConfigRegistry.DataCase, async: false

  alias Ash.Error.Invalid
  alias Mezzanine.ConfigRegistry.{Installation, PackRegistration}
  alias Mezzanine.Execution.Repo, as: ExecutionRepo
  alias Mezzanine.LeaseInvalidation
  alias Mezzanine.Leasing

  alias Mezzanine.Pack.{
    CompiledPack,
    Compiler,
    ContextSourceSpec,
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

  test "installation suspension invalidates active installation leases in the runtime lease store" do
    registration = register_fixture_pack!()

    assert {:ok, %Installation{} = installation} =
             MezzanineConfigRegistry.create_installation(%{
               tenant_id: "tenant-suspended",
               environment: "prod",
               pack_registration_id: registration.id
             })

    assert {:ok, %Installation{} = active_installation} =
             MezzanineConfigRegistry.activate_installation(installation)

    %{read_lease: read_lease, stream_lease: stream_lease} =
      issue_installation_leases!(active_installation, "suspended")

    assert {:ok, %Installation{} = suspended_installation} =
             MezzanineConfigRegistry.suspend_installation(active_installation)

    assert suspended_installation.status == :suspended

    assert Enum.map(installation_invalidations("installation_suspended"), & &1.lease_id)
           |> Enum.sort() == Enum.sort([read_lease.lease_id, stream_lease.lease_id])
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

  test "installation activation rejects bindings whose connector capability misses required lifecycle hints" do
    registration =
      fixture_pack(required_lifecycle_hints: [:ticket_status])
      |> MezzanineConfigRegistry.register_pack!()

    assert {:ok, %Installation{} = installation} =
             MezzanineConfigRegistry.create_installation(%{
               tenant_id: "tenant-hint-contract",
               environment: "prod",
               pack_registration_id: registration.id,
               binding_config: %{
                 "execution_bindings" => %{
                   "expense_request_capture" => %{
                     "placement_ref" => "local_runner",
                     "connector_capability" => %{
                       "capability_id" => "expense.capture",
                       "version" => "2026.04",
                       "produces_lifecycle_hints" => []
                     }
                   }
                 }
               }
             })

    assert {:error, {:lifecycle_hint_contract_violation, [violation]}} =
             MezzanineConfigRegistry.activate_installation(installation)

    assert violation.recipe_ref == "expense_request_capture"
    assert violation.missing_hints == ["ticket_status"]
    assert violation.capability_id == "expense.capture"
    assert violation.capability_version == "2026.04"
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
    assert compiled.manifest.max_supersession_depth == 12
    assert recipe.workspace_policy[:reuse] == true
    assert recipe.retry_config[:rekey_on] == [:semantic_failure]
    assert recipe.required_lifecycle_hints == []
    assert compiled.context_sources_by_ref["workspace_memory"].binding_key == "shared_memory"
  end

  defp register_fixture_pack! do
    fixture_pack()
    |> MezzanineConfigRegistry.register_pack!()
  end

  defp issue_installation_leases!(installation, suffix) do
    {:ok, read_lease} =
      Leasing.issue_read_lease(
        %{
          trace_id: "trace-installation-#{suffix}",
          tenant_id: installation.tenant_id,
          installation_id: installation.id,
          installation_revision: 1,
          activation_epoch: 1,
          lease_epoch: 1,
          subject_id: Ecto.UUID.generate(),
          lineage_anchor: %{"installation_id" => installation.id},
          allowed_family: "unified_trace",
          allowed_operations: [:fetch_run],
          scope: %{}
        },
        repo: ExecutionRepo
      )

    {:ok, stream_lease} =
      Leasing.issue_stream_attach_lease(
        %{
          trace_id: "trace-installation-#{suffix}",
          tenant_id: installation.tenant_id,
          installation_id: installation.id,
          installation_revision: 1,
          activation_epoch: 1,
          lease_epoch: 1,
          subject_id: Ecto.UUID.generate(),
          lineage_anchor: %{"installation_id" => installation.id},
          allowed_family: "runtime_stream",
          scope: %{}
        },
        repo: ExecutionRepo
      )

    %{read_lease: read_lease, stream_lease: stream_lease}
  end

  defp installation_invalidations(reason) do
    ExecutionRepo.all(LeaseInvalidation)
    |> Enum.filter(&(&1.reason == reason))
    |> Enum.sort_by(& &1.sequence_number)
  end

  defp fixture_pack(opts \\ []) do
    pack_slug = Keyword.get(opts, :pack_slug, :expense_approval)
    version = Keyword.get(opts, :version, "1.0.0")
    subject_kind = Keyword.get(opts, :subject_kind, :expense_request)
    recipe_ref = Keyword.get(opts, :recipe_ref, :"#{subject_kind}_capture")
    terminal_state = Keyword.get(opts, :terminal_state, :"#{subject_kind}_done")
    projection_name = Keyword.get(opts, :projection_name, :"active_#{subject_kind}")
    required_lifecycle_hints = Keyword.get(opts, :required_lifecycle_hints, [])

    manifest = %Manifest{
      pack_slug: pack_slug,
      version: version,
      max_supersession_depth: 12,
      subject_kind_specs: [
        %SubjectKindSpec{name: subject_kind}
      ],
      context_source_specs: [
        %ContextSourceSpec{
          source_ref: :workspace_memory,
          binding_key: :shared_memory,
          usage_phase: :retrieval,
          required?: false,
          timeout_ms: 500,
          schema_ref: "context/workspace_memory",
          max_fragments: 3,
          merge_strategy: :append
        }
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
          required_lifecycle_hints: required_lifecycle_hints,
          workspace_policy: %{strategy: :per_subject, reuse: true, cleanup: :on_terminal},
          retry_config: %{
            max_attempts: 3,
            backoff: :exponential,
            rekey_on: [:semantic_failure]
          }
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
