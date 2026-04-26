defmodule Mezzanine.Authoring.BundleImportTest do
  use Mezzanine.ConfigRegistry.DataCase, async: false

  alias Ash.Error.Invalid
  alias Mezzanine.Authoring.Bundle
  alias Mezzanine.ConfigRegistry.{Installation, PackRegistration}

  alias Mezzanine.Pack.{
    Compiler,
    ContextSourceSpec,
    ExecutionRecipeSpec,
    LifecycleSpec,
    Manifest,
    ProjectionSpec,
    Serializer,
    SubjectKindSpec
  }

  @signing_key "phase3-authoring-signing-key"
  @allowed_policy_refs ["policy.default"]
  @context_adapters %{"memory_adapter" => __MODULE__}

  test "imports a valid internal/operator bundle and activates it atomically" do
    attrs = signed_bundle_attrs(bundle_id: "bundle-valid", pack_slug: :phase3_valid_pack)

    assert {:ok, result} =
             MezzanineConfigRegistry.import_authoring_bundle(attrs,
               signing_key: @signing_key,
               allowed_policy_refs: @allowed_policy_refs,
               context_adapter_registry: @context_adapters
             )

    assert result.bundle.bundle_id == "bundle-valid"
    assert result.pack_registration.status == :active
    assert result.installation.status == :active
    assert result.installation.tenant_id == "tenant-authoring"
    assert result.installation.environment == "install-authoring"
    assert result.installation.metadata["bundle_id"] == "bundle-valid"
    assert result.installation.metadata["authored_by"] == "operator:phase3"

    assert get_in(result.installation.binding_config, [
             "context_bindings",
             "memory_adapter",
             "adapter_key"
           ]) == "memory_adapter"
  end

  test "rejects invalid bundle inputs before registration or activation" do
    cases = [
      {:checksum_mismatch,
       signed_bundle_attrs(bundle_id: "bundle-bad-checksum", pack_slug: :phase3_bad_checksum)
       |> Map.put("checksum", "sha256:not-the-payload")},
      {:signature_mismatch,
       signed_bundle_attrs(bundle_id: "bundle-bad-signature", pack_slug: :phase3_bad_signature)
       |> Map.put("signature", "hmac-sha256:not-the-signature")},
      {:invalid_policy_ref,
       signed_bundle_attrs(
         bundle_id: "bundle-bad-policy",
         pack_slug: :phase3_bad_policy,
         policy_refs: ["policy.missing"]
       )},
      {:pack_authored_platform_migration,
       signed_bundle_attrs(
         bundle_id: "bundle-bad-migration",
         pack_slug: :phase3_bad_migration,
         platform_migrations: [%{"table" => "subjects", "add" => ["pack_column"]}]
       )}
    ]

    Enum.each(cases, fn {expected_code, attrs} ->
      assert {:error, {:invalid_authoring_bundle, issues}} =
               MezzanineConfigRegistry.import_authoring_bundle(attrs,
                 signing_key: @signing_key,
                 allowed_policy_refs: @allowed_policy_refs,
                 context_adapter_registry: @context_adapters
               )

      pack_slug = attrs["pack_manifest"]["pack_slug"]

      assert Enum.any?(issues, &(&1.code == expected_code))
      assert {:error, %Invalid{}} = PackRegistration.by_slug_version(pack_slug, "1.0.0")
    end)
  end

  test "rejects missing lifecycle hints and unknown context adapters before activation" do
    missing_hint =
      signed_bundle_attrs(
        bundle_id: "bundle-missing-hint",
        pack_slug: :phase3_missing_hint,
        required_lifecycle_hints: [:ticket_status],
        produced_lifecycle_hints: []
      )

    unknown_adapter =
      signed_bundle_attrs(
        bundle_id: "bundle-unknown-adapter",
        pack_slug: :phase3_unknown_adapter,
        context_adapter_key: "missing_adapter"
      )

    assert {:error, {:invalid_authoring_bundle, missing_hint_issues}} =
             MezzanineConfigRegistry.import_authoring_bundle(missing_hint,
               signing_key: @signing_key,
               allowed_policy_refs: @allowed_policy_refs,
               context_adapter_registry: @context_adapters
             )

    assert Enum.any?(missing_hint_issues, &(&1.code == :missing_lifecycle_hints))

    assert {:error, {:invalid_authoring_bundle, unknown_adapter_issues}} =
             MezzanineConfigRegistry.import_authoring_bundle(unknown_adapter,
               signing_key: @signing_key,
               allowed_policy_refs: @allowed_policy_refs,
               context_adapter_registry: @context_adapters
             )

    assert Enum.any?(unknown_adapter_issues, &(&1.code == :unknown_context_adapter))
  end

  test "rejects stale installation revision before runtime activation" do
    attrs = signed_bundle_attrs(bundle_id: "bundle-stale-base", pack_slug: :phase3_stale_base)

    assert {:ok, result} =
             MezzanineConfigRegistry.import_authoring_bundle(attrs,
               signing_key: @signing_key,
               allowed_policy_refs: @allowed_policy_refs,
               context_adapter_registry: @context_adapters
             )

    stale_attrs =
      signed_bundle_attrs(
        bundle_id: "bundle-stale-update",
        installation_id: result.installation.id,
        pack_slug: :phase3_stale_base
      )

    assert {:error,
            {:stale_installation_revision,
             %{installation_id: installation_id, attempted_revision: 0, current_revision: 1}}} =
             MezzanineConfigRegistry.import_authoring_bundle(stale_attrs,
               signing_key: @signing_key,
               allowed_policy_refs: @allowed_policy_refs,
               context_adapter_registry: @context_adapters,
               expected_installation_revision: 0
             )

    assert installation_id == result.installation.id

    assert {:ok, %Installation{compiled_pack_revision: 1}} =
             Ash.get(Installation, result.installation.id)
  end

  defp signed_bundle_attrs(opts) do
    manifest = manifest(opts)
    manifest_payload = Serializer.serialize_manifest(manifest)
    context_adapter_key = Keyword.get(opts, :context_adapter_key, "memory_adapter")
    installation_id = Keyword.get(opts, :installation_id, "install-authoring")
    bundle_id = Keyword.fetch!(opts, :bundle_id)

    unsigned = %{
      "bundle_id" => bundle_id,
      "tenant_id" => "tenant-authoring",
      "installation_id" => installation_id,
      "pack_manifest" => manifest_payload,
      "lifecycle_specs" => manifest_payload["lifecycle_specs"],
      "decision_specs" => manifest_payload["decision_specs"],
      "binding_descriptors" => binding_descriptors(context_adapter_key, opts),
      "observer_descriptors" => [
        %{
          "binding_key" => "hindsight_audit",
          "subscriber_key" => "audit_export",
          "event_types" => ["run.accepted", "run.failed"]
        }
      ],
      "context_adapter_descriptors" => [
        %{
          "binding_key" => "memory_adapter",
          "adapter_key" => context_adapter_key,
          "source_ref" => "workspace_memory"
        }
      ],
      "policy_refs" => Keyword.get(opts, :policy_refs, ["policy.default"]),
      "authored_by" => "operator:phase3"
    }

    unsigned = maybe_put_platform_migrations(unsigned, opts)

    unsigned
    |> Map.put("checksum", Bundle.checksum_for(unsigned))
    |> Map.put("signature", Bundle.signature_for(unsigned, @signing_key))
  end

  defp maybe_put_platform_migrations(attrs, opts) do
    case Keyword.fetch(opts, :platform_migrations) do
      {:ok, migrations} -> Map.put(attrs, "platform_migrations", migrations)
      :error -> attrs
    end
  end

  defp binding_descriptors(context_adapter_key, opts) do
    produced_lifecycle_hints = Keyword.get(opts, :produced_lifecycle_hints, [:ticket_status])

    %{
      "execution_bindings" => %{
        "phase3_capture" => %{
          "placement_ref" => "local_runner",
          "connector_capability" => %{
            "capability_id" => "phase3.capture",
            "version" => "2026.04",
            "produces_lifecycle_hints" => Enum.map(produced_lifecycle_hints, &to_string/1)
          }
        }
      },
      "context_bindings" => %{
        "memory_adapter" => %{
          "adapter_key" => context_adapter_key,
          "timeout_ms" => 250,
          "config" => %{}
        }
      },
      "observer_bindings" => %{
        "hindsight_audit" => %{
          "subscriber_key" => "audit_export",
          "event_types" => ["run.accepted", "run.failed"]
        }
      }
    }
  end

  defp manifest(opts) do
    required_lifecycle_hints = Keyword.get(opts, :required_lifecycle_hints, [])
    pack_slug = Keyword.fetch!(opts, :pack_slug)

    %Manifest{
      pack_slug: pack_slug,
      version: "1.0.0",
      subject_kind_specs: [%SubjectKindSpec{name: :phase3_request}],
      context_source_specs: [
        %ContextSourceSpec{
          source_ref: :workspace_memory,
          binding_key: :memory_adapter,
          usage_phase: :retrieval,
          required?: false,
          timeout_ms: 250,
          schema_ref: "context/workspace_memory",
          max_fragments: 2,
          merge_strategy: :append
        }
      ],
      lifecycle_specs: [
        %LifecycleSpec{
          subject_kind: :phase3_request,
          initial_state: :submitted,
          terminal_states: [:completed],
          transitions: [
            %{
              from: :submitted,
              to: :processing,
              trigger: {:execution_requested, :phase3_capture}
            },
            %{from: :processing, to: :completed, trigger: {:execution_completed, :phase3_capture}}
          ]
        }
      ],
      execution_recipe_specs: [
        %ExecutionRecipeSpec{
          recipe_ref: :phase3_capture,
          runtime_class: :session,
          placement_ref: :local_runner,
          required_lifecycle_hints: required_lifecycle_hints,
          workspace_policy: %{strategy: :per_subject, root_ref: :phase3_workspaces},
          sandbox_policy_ref: :standard_phase3_policy,
          prompt_refs: [:phase3_prompt]
        }
      ],
      projection_specs: [
        %ProjectionSpec{name: :phase3_active, subject_kinds: [:phase3_request]}
      ]
    }
    |> compile_manifest!()
  end

  defp compile_manifest!(manifest) do
    case Compiler.compile(manifest) do
      {:ok, compiled} -> compiled.manifest
      {:error, errors} -> raise "invalid test manifest: #{inspect(errors)}"
    end
  end
end
