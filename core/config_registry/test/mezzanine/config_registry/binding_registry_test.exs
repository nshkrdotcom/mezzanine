defmodule Mezzanine.ConfigRegistry.BindingRegistryTest do
  use Mezzanine.ConfigRegistry.DataCase, async: false

  alias Mezzanine.ConfigRegistry.{
    ActiveBindingSet,
    BindingManifestDependency,
    BindingSet,
    CompiledBinding,
    Installation,
    RunBindingSnapshot
  }

  alias Mezzanine.Pack.{
    CompiledPack,
    Compiler,
    ExecutionRecipeSpec,
    LifecycleSpec,
    Manifest,
    ProjectionSpec,
    RuntimeBinding,
    SourceBinding,
    SourceKindSpec,
    SubjectKindSpec
  }

  test "installation activation materializes durable active binding set and manifest dependencies" do
    installation = activate_fixture_installation!("tenant-bindings")

    assert {:ok, %ActiveBindingSet{} = active} =
             MezzanineConfigRegistry.active_binding_set(
               installation.tenant_id,
               installation.environment,
               installation.pack_slug
             )

    assert {:ok, %ActiveBindingSet{} = by_installation} =
             MezzanineConfigRegistry.active_binding_set_for_installation(installation.id)

    assert by_installation.id == active.id

    assert active.binding_epoch > 0
    assert active.compiled_pack_revision == installation.compiled_pack_revision

    assert {:ok, resolution} =
             MezzanineConfigRegistry.resolve_active_binding(
               tenant_id: installation.tenant_id,
               environment: installation.environment,
               pack_slug: installation.pack_slug,
               binding_ref: "case_source_primary",
               binding_kind: :source,
               expected_binding_epoch: active.binding_epoch
             )

    assert %BindingSet{} = resolution.binding_set
    assert %CompiledBinding{} = resolution.compiled_binding
    assert resolution.compiled_binding.connector_ref == "ticket_connector"
    assert resolution.compiled_binding.manifest_ref == "ticket_manifest_v1"

    assert resolution.compiled_binding.operation_refs == %{
             "preview" => "preview_cases",
             "read" => "search_cases"
           }

    assert resolution.compiled_binding.policy_refs == [
             "source_authority_policy",
             "ticket_projection_profile",
             "ticket_retry_policy"
           ]

    assert String.starts_with?(resolution.compiled_binding.checksum, "sha256:")
    assert String.length(resolution.compiled_binding.checksum) == 71

    assert [
             %BindingManifestDependency{} = preview_dependency,
             %BindingManifestDependency{} = dependency
           ] = resolution.manifest_dependencies

    assert preview_dependency.operation_role == "preview"
    assert preview_dependency.operation_ref == "preview_cases"
    assert dependency.operation_role == "read"
    assert dependency.operation_ref == "search_cases"
    assert dependency.operation_class == "source_read"
    assert dependency.required_scopes == ["tickets.read"]
    assert dependency.credential_scope_ref == "ticket_credentials"
  end

  test "operation-role lookup returns only the dependency targeted for dispatch" do
    installation = activate_fixture_installation!("tenant-role-lookup")

    assert {:ok, resolution} =
             MezzanineConfigRegistry.resolve_active_binding(
               tenant_id: installation.tenant_id,
               environment: installation.environment,
               pack_slug: installation.pack_slug,
               binding_ref: "case_source_primary",
               binding_kind: :source,
               operation_role: :preview
             )

    assert [%BindingManifestDependency{} = dependency] = resolution.manifest_dependencies
    assert resolution.operation_dependency.id == dependency.id
    assert dependency.operation_role == "preview"
    assert dependency.operation_ref == "preview_cases"
    assert dependency.operation_class == "source_preview"
    assert dependency.required_scopes == ["tickets.preview"]
    assert resolution.descriptor.policy_refs == resolution.compiled_binding.policy_refs
    assert resolution.descriptor.checksum == resolution.compiled_binding.checksum
  end

  test "missing operation-role lookup fails closed before dispatch" do
    installation = activate_fixture_installation!("tenant-role-missing")

    assert {:error, {:missing_binding_operation_role, missing}} =
             MezzanineConfigRegistry.resolve_active_binding(
               tenant_id: installation.tenant_id,
               environment: installation.environment,
               pack_slug: installation.pack_slug,
               binding_ref: "case_source_primary",
               binding_kind: :source,
               operation_role: :archive
             )

    assert missing.binding_ref == "case_source_primary"
    assert missing.operation_role == "archive"
  end

  test "binding updates allocate a new epoch and stale epoch lookup fails closed" do
    installation = activate_fixture_installation!("tenant-stale")

    {:ok, first_active} =
      MezzanineConfigRegistry.active_binding_set(
        installation.tenant_id,
        installation.environment,
        installation.pack_slug
      )

    assert {:ok, %Installation{} = updated_installation} =
             MezzanineConfigRegistry.update_bindings(installation, %{
               "binding_notes" => %{"reason" => "operator-selected-secondary-lane"}
             })

    {:ok, second_active} =
      MezzanineConfigRegistry.active_binding_set(
        updated_installation.tenant_id,
        updated_installation.environment,
        updated_installation.pack_slug
      )

    assert second_active.binding_epoch > first_active.binding_epoch
    assert second_active.compiled_pack_revision == updated_installation.compiled_pack_revision

    assert Ash.get!(BindingSet, first_active.binding_set_id).status == :retired

    assert {:ok, gc_status} =
             MezzanineConfigRegistry.binding_set_gc_status(first_active.binding_set_id)

    assert gc_status.status == :eligible
    assert gc_status.eligible?
    assert gc_status.snapshot_count == 0

    assert {:error, {:stale_binding_epoch, stale}} =
             MezzanineConfigRegistry.resolve_active_binding(
               tenant_id: updated_installation.tenant_id,
               environment: updated_installation.environment,
               pack_slug: updated_installation.pack_slug,
               binding_ref: "case_source_primary",
               expected_binding_epoch: first_active.binding_epoch
             )

    assert stale.current_binding_epoch == second_active.binding_epoch
    assert stale.expected_binding_epoch == first_active.binding_epoch
  end

  test "run binding snapshots stay pinned after active binding set advances" do
    installation = activate_fixture_installation!("tenant-snapshot")

    {:ok, first_active} =
      MezzanineConfigRegistry.active_binding_set(
        installation.tenant_id,
        installation.environment,
        installation.pack_slug
      )

    assert {:ok, %RunBindingSnapshot{} = snapshot} =
             MezzanineConfigRegistry.capture_run_binding_snapshot(
               tenant_id: installation.tenant_id,
               environment: installation.environment,
               pack_slug: installation.pack_slug,
               run_ref: "run://binding-snapshot/1",
               binding_ref: "case_source_primary",
               binding_kind: :source,
               expected_binding_epoch: first_active.binding_epoch
             )

    assert snapshot.binding_epoch == first_active.binding_epoch
    assert snapshot.descriptor["connector_ref"] == "ticket_connector"

    assert {:ok, %Installation{} = updated_installation} =
             MezzanineConfigRegistry.update_bindings(installation, %{
               "binding_notes" => %{"reason" => "cutover-to-new-binding-set"}
             })

    {:ok, second_active} =
      MezzanineConfigRegistry.active_binding_set(
        updated_installation.tenant_id,
        updated_installation.environment,
        updated_installation.pack_slug
      )

    assert second_active.binding_epoch > snapshot.binding_epoch
    assert Ash.get!(BindingSet, snapshot.binding_set_id).status == :retired

    assert {:ok, gc_status} =
             MezzanineConfigRegistry.binding_set_gc_status(snapshot.binding_set_id)

    assert gc_status.status == :retained_by_run_snapshots
    refute gc_status.eligible?
    assert gc_status.snapshot_count == 1

    assert {:ok, %RunBindingSnapshot{} = pinned_snapshot} =
             MezzanineConfigRegistry.resolve_run_binding_snapshot(
               tenant_id: installation.tenant_id,
               environment: installation.environment,
               run_ref: "run://binding-snapshot/1",
               binding_ref: "case_source_primary"
             )

    assert pinned_snapshot.binding_epoch == first_active.binding_epoch
    assert pinned_snapshot.binding_set_id == snapshot.binding_set_id
  end

  test "operation plan resolution uses run snapshots after active binding advances" do
    installation = activate_fixture_installation!("tenant-snapshot-plan")

    {:ok, first_active} =
      MezzanineConfigRegistry.active_binding_set(
        installation.tenant_id,
        installation.environment,
        installation.pack_slug
      )

    assert {:ok, %RunBindingSnapshot{} = snapshot} =
             MezzanineConfigRegistry.capture_run_binding_snapshot(
               tenant_id: installation.tenant_id,
               environment: installation.environment,
               pack_slug: installation.pack_slug,
               run_ref: "run://binding-plan/1",
               binding_ref: "case_source_primary",
               binding_kind: :source,
               expected_binding_epoch: first_active.binding_epoch
             )

    assert {:ok, %Installation{} = updated_installation} =
             MezzanineConfigRegistry.update_bindings(installation, %{
               "binding_notes" => %{"reason" => "advance-active-binding"}
             })

    assert {:error, {:stale_binding_epoch, _stale}} =
             MezzanineConfigRegistry.resolve_active_binding(
               tenant_id: updated_installation.tenant_id,
               environment: updated_installation.environment,
               pack_slug: updated_installation.pack_slug,
               binding_ref: "case_source_primary",
               binding_kind: :source,
               operation_role: :read,
               expected_binding_epoch: first_active.binding_epoch
             )

    assert {:ok, plan} =
             MezzanineConfigRegistry.resolve_operation_plan(
               tenant_id: updated_installation.tenant_id,
               environment: updated_installation.environment,
               pack_slug: updated_installation.pack_slug,
               run_ref: "run://binding-plan/1",
               binding_ref: "case_source_primary",
               binding_kind: :source,
               operation_role: :read,
               expected_binding_epoch: first_active.binding_epoch
             )

    assert plan.source == :run_binding_snapshot
    assert plan.run_binding_snapshot.id == snapshot.id
    assert plan.binding_epoch == first_active.binding_epoch
    assert plan.descriptor["binding_epoch"] == first_active.binding_epoch

    assert [dependency] = plan.manifest_dependencies
    assert dependency["operation_role"] == "read"
    assert dependency["operation_ref"] == "search_cases"
    assert plan.operation_dependency == dependency

    assert {:error, {:stale_binding_epoch, _stale}} =
             MezzanineConfigRegistry.resolve_operation_plan(
               tenant_id: updated_installation.tenant_id,
               environment: updated_installation.environment,
               pack_slug: updated_installation.pack_slug,
               run_ref: "run://binding-plan/missing",
               binding_ref: "case_source_primary",
               binding_kind: :source,
               operation_role: :read,
               expected_binding_epoch: first_active.binding_epoch
             )
  end

  test "missing binding refs fail closed" do
    installation = activate_fixture_installation!("tenant-missing")

    assert {:error, _not_found} =
             MezzanineConfigRegistry.resolve_active_binding(
               tenant_id: installation.tenant_id,
               environment: installation.environment,
               pack_slug: installation.pack_slug,
               binding_ref: "missing_binding"
             )
  end

  defp activate_fixture_installation!(tenant_id) do
    compiled_pack = fixture_pack!()

    registration = MezzanineConfigRegistry.register_pack!(compiled_pack)

    {:ok, installation} =
      MezzanineConfigRegistry.create_installation(%{
        tenant_id: tenant_id,
        environment: "prod",
        pack_registration_id: registration.id
      })

    {:ok, active_installation} = MezzanineConfigRegistry.activate_installation(installation)
    active_installation
  end

  defp fixture_pack! do
    case Compiler.compile(fixture_manifest()) do
      {:ok, %CompiledPack{} = compiled_pack} -> compiled_pack
      {:error, errors} -> raise "failed to compile binding registry fixture: #{inspect(errors)}"
    end
  end

  defp fixture_manifest do
    %Manifest{
      pack_slug: :binding_registry_fixture,
      version: "1.0.0",
      profile_slots: profile_slots(),
      subject_kind_specs: [
        %SubjectKindSpec{name: :case_file}
      ],
      source_kind_specs: [
        %SourceKindSpec{
          name: :case_ticket,
          subject_kind: :case_file,
          description: "Case ticket source"
        }
      ],
      binding_specs: [
        %SourceBinding{
          binding_ref: :case_source_primary,
          source_kind: :case_ticket,
          subject_kind: :case_file,
          connector_ref: :ticket_connector,
          manifest_ref: :ticket_manifest_v1,
          operation_refs: %{preview: :preview_cases, read: :search_cases},
          credential_binding_ref: :ticket_credentials,
          projection_profile_ref: :ticket_projection_profile,
          retry_policy_ref: :ticket_retry_policy,
          metadata: %{
            "operation_classes" => %{"preview" => :source_preview, "read" => :source_read},
            "required_scopes" => %{"preview" => ["tickets.preview"], "read" => ["tickets.read"]},
            policy_refs: [:source_authority_policy],
            manifest_digest: "sha256:ticket_manifest_fixture"
          }
        },
        %RuntimeBinding{
          binding_ref: :case_runtime,
          runtime_family: :session,
          connector_ref: :runtime_connector,
          manifest_ref: :runtime_manifest_v1,
          operation_refs: %{run: :start_session},
          credential_binding_ref: :runtime_credentials,
          metadata: %{
            operation_classes: %{run: :runtime_session},
            required_scopes: %{run: ["runtime.run"]}
          }
        }
      ],
      lifecycle_specs: [
        %LifecycleSpec{
          subject_kind: :case_file,
          initial_state: :submitted,
          terminal_states: [:completed],
          transitions: [
            %{from: :submitted, to: :running, trigger: {:execution_requested, :case_review}},
            %{from: :running, to: :completed, trigger: {:execution_completed, :case_review}}
          ]
        }
      ],
      execution_recipe_specs: [
        %ExecutionRecipeSpec{
          recipe_ref: :case_review,
          runtime_class: :session,
          placement_ref: :case_runtime,
          workspace_policy: %{strategy: :per_subject, root_ref: :case_workspaces},
          sandbox_policy_ref: :case_sandbox,
          prompt_refs: [:case_prompt],
          applicable_to: [:case_file]
        }
      ],
      projection_specs: [
        %ProjectionSpec{name: :active_cases, subject_kinds: [:case_file]}
      ]
    }
  end

  defp profile_slots do
    %{
      source_profile_ref: :fixture_source_v1,
      runtime_profile_ref: :fixture_runtime_v1,
      tool_scope_ref: :fixture_tools_v1,
      evidence_profile_ref: :fixture_evidence_v1,
      publication_profile_ref: :fixture_publication_v1,
      review_profile_ref: :operator_optional,
      memory_profile_ref: :none,
      projection_profile_ref: :runtime_readback_v1
    }
  end
end
