defmodule MezzanineConfigRegistry do
  @moduledoc """
  Durable neutral pack-registration and installation registry facade.
  """

  alias Mezzanine.Authoring.Bundle

  alias Mezzanine.ConfigRegistry.{
    Installation,
    LifecycleHintContract,
    PackRegistration,
    Policy,
    PolicyRegistry
  }

  alias Mezzanine.ConfigRegistry.Repo, as: ConfigRegistryRepo
  alias Mezzanine.Execution.Repo, as: ExecutionRepo
  alias Mezzanine.Leasing
  alias Mezzanine.Pack.{CompiledPack, Registry, Serializer}

  @spec components() :: [module()]
  def components do
    [
      Mezzanine.ConfigRegistry,
      Mezzanine.ConfigRegistry.PackRegistration,
      Mezzanine.ConfigRegistry.Installation,
      Mezzanine.ConfigRegistry.Policy,
      Mezzanine.Authoring.Bundle,
      Mezzanine.Pack.Registry,
      Mezzanine.Pack.Serializer
    ]
  end

  @spec register_pack(CompiledPack.t()) :: {:ok, PackRegistration.t()} | {:error, term()}
  def register_pack(%CompiledPack{} = compiled_pack) do
    PackRegistration.register(pack_registration_attrs(compiled_pack))
  end

  defp register_pack_with_notifications(%CompiledPack{} = compiled_pack) do
    compiled_pack
    |> pack_registration_attrs()
    |> PackRegistration.register(return_notifications?: true)
    |> action_result_with_notifications()
  end

  defp pack_registration_attrs(%CompiledPack{} = compiled_pack) do
    %{
      pack_slug: compiled_pack.pack_slug,
      version: compiled_pack.version,
      compiled_manifest: Serializer.serialize_compiled(compiled_pack),
      canonical_subject_kinds: Map.keys(compiled_pack.subject_kinds),
      migration_strategy: to_string(compiled_pack.manifest.migration_strategy)
    }
  end

  @spec register_pack!(CompiledPack.t()) :: PackRegistration.t()
  def register_pack!(%CompiledPack{} = compiled_pack) do
    case register_pack(compiled_pack) do
      {:ok, registration} -> registration
      {:error, error} -> raise "failed to register pack: #{inspect(error)}"
    end
  end

  @spec register_policy(struct(), keyword()) :: {:ok, Policy.t()} | {:error, term()}
  def register_policy(policy_contract, opts \\ []) when is_struct(policy_contract) do
    PolicyRegistry.register(policy_contract, opts)
  end

  @spec resolve_policy(atom() | String.t(), map(), keyword()) ::
          {:ok, Policy.t()} | {:error, :not_found}
  def resolve_policy(kind, context, opts \\ []) when is_map(context) do
    PolicyRegistry.resolve(kind, context, opts)
  end

  @spec resolve_policy_for_snapshot(atom() | String.t(), map(), term(), keyword()) ::
          {:ok, Policy.t()}
          | {:error,
             :not_found | {:invalid_snapshot_epoch, term()} | {:snapshot_epoch_mismatch, map()}}
  def resolve_policy_for_snapshot(kind, context, snapshot_epoch, opts \\ [])
      when is_map(context) do
    PolicyRegistry.resolve_for_snapshot(kind, context, snapshot_epoch, opts)
  end

  @spec create_installation(map()) :: {:ok, Installation.t()} | {:error, term()}
  def create_installation(attrs) when is_map(attrs) do
    with {:ok, %PackRegistration{} = registration} <-
           Ash.get(PackRegistration, Map.fetch!(attrs, :pack_registration_id)) do
      attrs
      |> Map.put(:pack_slug, registration.pack_slug)
      |> Installation.create_installation()
    end
  end

  @spec activate_installation(Installation.t(), keyword()) ::
          {:ok, Installation.t()} | {:error, term()}
  def activate_installation(%Installation{} = installation, opts \\ []) do
    with :ok <- LifecycleHintContract.validate(installation),
         {:ok, updated_installation} <- Installation.activate_installation(installation),
         :ok <- maybe_write_installation_graph(updated_installation, opts) do
      :ok =
        Registry.reload_installation(
          updated_installation.id,
          updated_installation.compiled_pack_revision
        )

      {:ok, updated_installation}
    end
  end

  @spec suspend_installation(Installation.t()) :: {:ok, Installation.t()} | {:error, term()}
  def suspend_installation(%Installation{} = installation) do
    with {:ok, updated_installation} <- Installation.suspend_installation(installation) do
      {:ok, _invalidations} =
        Leasing.invalidate_installation_leases(
          updated_installation.id,
          "installation_suspended",
          now: DateTime.utc_now() |> DateTime.truncate(:microsecond),
          repo: ExecutionRepo,
          trace_id: "installation-suspended:#{updated_installation.id}"
        )

      :ok = Registry.forget_installation(updated_installation.id)
      {:ok, updated_installation}
    end
  end

  @spec reactivate_installation(Installation.t()) :: {:ok, Installation.t()} | {:error, term()}
  def reactivate_installation(%Installation{} = installation) do
    with :ok <- LifecycleHintContract.validate(installation),
         {:ok, updated_installation} <- Installation.reactivate_installation(installation) do
      :ok =
        Registry.reload_installation(
          updated_installation.id,
          updated_installation.compiled_pack_revision
        )

      {:ok, updated_installation}
    end
  end

  @spec update_bindings(Installation.t(), map(), keyword()) ::
          {:ok, Installation.t()} | {:error, term()}
  def update_bindings(%Installation{} = installation, binding_config, opts \\ [])
      when is_map(binding_config) do
    with :ok <- LifecycleHintContract.validate(installation, binding_config),
         {:ok, updated_installation} <-
           Installation.update_bindings(installation, %{binding_config: binding_config}),
         :ok <- maybe_advance_policy_epoch(updated_installation, opts) do
      :ok =
        Registry.reload_installation(
          updated_installation.id,
          updated_installation.compiled_pack_revision
        )

      {:ok, updated_installation}
    end
  end

  @spec import_authoring_bundle(map(), keyword()) :: {:ok, map()} | {:error, term()}
  def import_authoring_bundle(attrs, opts \\ []) when is_map(attrs) do
    with {:ok, %Bundle{} = bundle} <- Bundle.new(attrs, opts),
         :ok <- assert_expected_installation_revision(bundle, opts) do
      bundle
      |> import_authoring_bundle_with_transaction()
      |> finalize_authoring_bundle_import()
    end
  end

  defp import_authoring_bundle_with_transaction(%Bundle{} = bundle) do
    ConfigRegistryRepo.transaction(fn ->
      case import_authoring_bundle_transaction(bundle) do
        {:ok, result} -> result
        {:error, error} -> ConfigRegistryRepo.rollback(error)
      end
    end)
  end

  defp finalize_authoring_bundle_import({:ok, result}) do
    notify_import(result)
    result |> Map.delete(:notifications) |> reload_imported_installation()
  end

  defp finalize_authoring_bundle_import({:error, error}), do: {:error, error}

  defp import_authoring_bundle_transaction(%Bundle{} = bundle) do
    with {:ok, %PackRegistration{} = registration, registration_notifications} <-
           ensure_active_registration(bundle),
         {:ok, %Installation{} = installation, installation_notifications} <-
           upsert_installation(bundle, registration) do
      {:ok,
       %{
         bundle: bundle,
         pack_registration: registration,
         installation: installation,
         notifications: registration_notifications ++ installation_notifications
       }}
    end
  end

  defp ensure_active_registration(%Bundle{compiled_pack: %CompiledPack{} = compiled_pack}) do
    case PackRegistration.by_slug_version(compiled_pack.pack_slug, compiled_pack.version) do
      {:ok, %PackRegistration{status: :active} = registration} ->
        {:ok, registration, []}

      {:ok, %PackRegistration{status: :registered} = registration} ->
        activate_registration_with_notifications(registration)

      {:ok, %PackRegistration{status: :deprecated} = registration} ->
        {:error, {:deprecated_pack_registration, %{pack_registration_id: registration.id}}}

      {:error, _not_found} ->
        with {:ok, %PackRegistration{} = registration, register_notifications} <-
               register_pack_with_notifications(compiled_pack),
             {:ok, %PackRegistration{} = active_registration, activate_notifications} <-
               activate_registration_with_notifications(registration) do
          {:ok, active_registration, register_notifications ++ activate_notifications}
        end
    end
  end

  defp upsert_installation(%Bundle{} = bundle, %PackRegistration{} = registration) do
    case existing_installation(bundle.installation_id) do
      {:ok, %Installation{} = installation} ->
        update_existing_installation(bundle, installation)

      :not_found ->
        create_and_activate_installation(bundle, registration)
    end
  end

  defp update_existing_installation(%Bundle{} = bundle, %Installation{} = installation) do
    cond do
      installation.tenant_id != bundle.tenant_id ->
        {:error,
         {:installation_tenant_mismatch,
          %{
            installation_id: installation.id,
            bundle_tenant_id: bundle.tenant_id,
            installation_tenant_id: installation.tenant_id
          }}}

      installation.pack_slug != bundle.compiled_pack.pack_slug ->
        {:error,
         {:installation_pack_mismatch,
          %{
            installation_id: installation.id,
            bundle_pack_slug: bundle.compiled_pack.pack_slug,
            installation_pack_slug: installation.pack_slug
          }}}

      true ->
        with {:ok, updated_installation, update_notifications} <-
               update_bindings_for_bundle(installation, bundle),
             {:ok, active_installation, activation_notifications} <-
               ensure_installation_active(updated_installation) do
          {:ok, active_installation, update_notifications ++ activation_notifications}
        end
    end
  end

  defp update_bindings_for_bundle(%Installation{} = installation, %Bundle{} = bundle) do
    if installation.binding_config == bundle.binding_descriptors do
      {:ok, installation, []}
    else
      installation
      |> Installation.update_bindings(
        %{
          binding_config: bundle.binding_descriptors
        },
        return_notifications?: true
      )
      |> action_result_with_notifications()
    end
  end

  defp create_and_activate_installation(%Bundle{} = bundle, %PackRegistration{} = registration) do
    attrs = %{
      tenant_id: bundle.tenant_id,
      environment: bundle.installation_id,
      pack_slug: registration.pack_slug,
      pack_registration_id: registration.id,
      binding_config: bundle.binding_descriptors,
      metadata: bundle_metadata(bundle)
    }

    with {:ok, %Installation{} = installation, create_notifications} <-
           attrs
           |> Installation.create_installation(return_notifications?: true)
           |> action_result_with_notifications(),
         {:ok, %Installation{} = active_installation, activate_notifications} <-
           activate_installation_with_notifications(installation) do
      {:ok, active_installation, create_notifications ++ activate_notifications}
    end
  end

  defp ensure_installation_active(%Installation{status: :active} = installation),
    do: {:ok, installation, []}

  defp ensure_installation_active(%Installation{status: :suspended} = installation),
    do:
      installation
      |> Installation.reactivate_installation(return_notifications?: true)
      |> action_result_with_notifications()

  defp ensure_installation_active(%Installation{} = installation),
    do: activate_installation_with_notifications(installation)

  defp activate_registration_with_notifications(%PackRegistration{} = registration) do
    registration
    |> PackRegistration.activate(return_notifications?: true)
    |> action_result_with_notifications()
  end

  defp activate_installation_with_notifications(%Installation{} = installation) do
    installation
    |> Installation.activate_installation(return_notifications?: true)
    |> action_result_with_notifications()
  end

  defp maybe_write_installation_graph(%Installation{} = installation, opts) do
    installation
    |> installation_graph_edges(opts)
    |> write_installation_graph(installation, opts, access_graph_store(opts))
  end

  defp write_installation_graph(_edges, _installation, _opts, nil), do: :ok

  defp write_installation_graph([], _installation, _opts, _store), do: :ok

  defp write_installation_graph(edges, %Installation{} = installation, opts, store) do
    case store.insert_edges(
           installation.tenant_id,
           edges,
           access_graph_opts(installation, opts, "installation_activation")
         ) do
      {:ok, _result} -> :ok
      {:error, reason} -> {:error, reason}
    end
  end

  defp maybe_advance_policy_epoch(%Installation{} = installation, opts) do
    case access_graph_store(opts) do
      nil ->
        :ok

      store ->
        case store.advance_epoch(
               installation.tenant_id,
               access_graph_opts(installation, opts, "policy_compilation_change")
             ) do
          {:ok, _result} -> :ok
          {:error, reason} -> {:error, reason}
        end
    end
  end

  defp access_graph_store(opts) do
    Keyword.get(opts, :access_graph_store) ||
      Application.get_env(:mezzanine_config_registry, :access_graph_store)
  end

  defp installation_graph_edges(%Installation{} = installation, opts) do
    graph = graph_config(installation.binding_config)

    user_refs = string_list(graph, :user_refs)
    scope_refs = string_list(graph, :scope_refs)
    resource_refs = string_list(graph, :resource_refs)
    policy_refs = string_list(graph, :policy_refs)

    authority_ref =
      graph_value(graph, :granting_authority_ref) || Keyword.get(opts, :granting_authority_ref)

    evidence_refs = list_value(graph, :evidence_refs)

    metadata = %{
      "installation_ref" => installation.id,
      "compiled_pack_revision" => installation.compiled_pack_revision
    }

    us_edges =
      for user_ref <- user_refs,
          scope_ref <- scope_refs,
          do:
            graph_edge(
              :us,
              user_ref,
              scope_ref,
              authority_ref,
              evidence_refs,
              policy_refs,
              metadata
            )

    sr_edges =
      for scope_ref <- scope_refs,
          resource_ref <- resource_refs,
          do:
            graph_edge(
              :sr,
              scope_ref,
              resource_ref,
              authority_ref,
              evidence_refs,
              policy_refs,
              metadata
            )

    up_edges =
      for user_ref <- user_refs,
          policy_ref <- policy_refs,
          do:
            graph_edge(
              :up,
              user_ref,
              policy_ref,
              authority_ref,
              evidence_refs,
              policy_refs,
              metadata
            )

    us_edges ++ sr_edges ++ up_edges
  end

  defp graph_edge(
         edge_type,
         head_ref,
         tail_ref,
         authority_ref,
         evidence_refs,
         policy_refs,
         metadata
       ) do
    %{
      edge_type: edge_type,
      head_ref: head_ref,
      tail_ref: tail_ref,
      granting_authority_ref: authority_ref,
      evidence_refs: evidence_refs,
      policy_refs: policy_refs,
      metadata: metadata
    }
  end

  defp access_graph_opts(%Installation{} = installation, opts, cause) do
    graph = graph_config(installation.binding_config)

    [
      cause: cause,
      trace_id:
        Keyword.get(opts, :trace_id) ||
          graph_value(graph, :trace_id) ||
          "#{cause}:#{installation.id}:#{installation.compiled_pack_revision}",
      source_node_ref:
        Keyword.get(opts, :source_node_ref) || graph_value(graph, :source_node_ref),
      commit_hlc: Keyword.get(opts, :commit_hlc),
      metadata: %{
        "installation_ref" => installation.id,
        "compiled_pack_revision" => installation.compiled_pack_revision
      }
    ]
    |> maybe_put_test_pid(opts)
    |> Enum.reject(fn {_key, value} -> is_nil(value) end)
  end

  defp maybe_put_test_pid(graph_opts, opts) do
    case Keyword.get(opts, :access_graph_test_pid) do
      nil -> graph_opts
      pid -> Keyword.put(graph_opts, :access_graph_test_pid, pid)
    end
  end

  defp graph_config(config) when is_map(config) do
    case Map.get(config, "access_graph") || Map.get(config, :access_graph) do
      graph when is_map(graph) -> graph
      _other -> %{}
    end
  end

  defp graph_config(_config), do: %{}

  defp string_list(map, key) do
    map
    |> graph_value(key)
    |> list_value()
    |> Enum.flat_map(fn
      value when is_binary(value) and value != "" -> [value]
      value when is_atom(value) -> [Atom.to_string(value)]
      _other -> []
    end)
    |> Enum.uniq()
  end

  defp list_value(map, key), do: map |> graph_value(key) |> list_value()

  defp list_value(nil), do: []
  defp list_value(values) when is_list(values), do: values
  defp list_value(value), do: [value]

  defp graph_value(map, key) when is_map(map) do
    Map.get(map, key) || Map.get(map, Atom.to_string(key))
  end

  defp action_result_with_notifications({:ok, record, notifications}),
    do: {:ok, record, notifications}

  defp action_result_with_notifications({:ok, record}), do: {:ok, record, []}
  defp action_result_with_notifications({:error, reason}), do: {:error, reason}

  defp notify_import(%{notifications: notifications}) do
    notifications
    |> List.wrap()
    |> case do
      [] -> :ok
      notifications -> Ash.Notifier.notify(notifications)
    end
  end

  defp reload_imported_installation(%{installation: %Installation{} = installation} = result) do
    :ok = Registry.reload_installation(installation.id, installation.compiled_pack_revision)
    {:ok, result}
  end

  defp assert_expected_installation_revision(%Bundle{} = bundle, opts) do
    expected_revision =
      Keyword.get(opts, :expected_installation_revision, bundle.expected_installation_revision)

    case {expected_revision, existing_installation(bundle.installation_id)} do
      {nil, _result} ->
        :ok

      {_revision, :not_found} ->
        :ok

      {revision, {:ok, %Installation{} = installation}}
      when revision == installation.compiled_pack_revision ->
        :ok

      {revision, {:ok, %Installation{} = installation}} ->
        {:error,
         {:stale_installation_revision,
          %{
            installation_id: installation.id,
            attempted_revision: revision,
            current_revision: installation.compiled_pack_revision
          }}}
    end
  end

  defp existing_installation(installation_id) do
    with {:ok, uuid} <- Ecto.UUID.cast(installation_id),
         {:ok, %Installation{} = installation} <- Ash.get(Installation, uuid) do
      {:ok, installation}
    else
      _other -> :not_found
    end
  end

  defp bundle_metadata(%Bundle{} = bundle) do
    %{
      "bundle_id" => bundle.bundle_id,
      "authored_by" => bundle.authored_by,
      "checksum" => bundle.checksum,
      "signature" => bundle.signature,
      "policy_refs" => bundle.policy_refs
    }
  end
end
