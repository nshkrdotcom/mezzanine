defmodule Mezzanine.ConfigRegistry.BindingRegistry do
  @moduledoc """
  Durable resolver for generic pack bindings.

  The registry stores activation-scoped binding sets in Postgres. Runtime caches
  may sit above this module, but authoritative lookup and run snapshots resolve
  from these durable rows so stale cache state fails closed.
  """

  alias Mezzanine.ConfigRegistry.{
    ActiveBindingSet,
    BindingManifestDependency,
    BindingSet,
    CompiledBinding,
    Installation,
    PackRegistration,
    Repo,
    RunBindingSnapshot
  }

  alias Mezzanine.Pack.{CompiledPack, Serializer}

  @type activation_result :: %{
          binding_set: BindingSet.t(),
          active_binding_set: ActiveBindingSet.t(),
          compiled_bindings: [CompiledBinding.t()],
          manifest_dependencies: [BindingManifestDependency.t()]
        }

  @spec activate_for_installation(Installation.t(), keyword()) ::
          {:ok, activation_result()} | {:error, term()}
  def activate_for_installation(%Installation{} = installation, opts \\ []) when is_list(opts) do
    Repo.transaction(fn ->
      case activate_for_installation_transaction(installation, opts) do
        {:ok, result} -> result
        {:error, reason} -> Repo.rollback(reason)
      end
    end)
  end

  @spec active_binding_set(String.t(), String.t(), String.t()) ::
          {:ok, ActiveBindingSet.t()} | {:error, term()}
  def active_binding_set(tenant_id, environment, pack_slug) do
    ActiveBindingSet.by_scope(tenant_id, environment, pack_slug)
  end

  @spec active_binding_set_for_installation(String.t()) ::
          {:ok, ActiveBindingSet.t()} | {:error, term()}
  def active_binding_set_for_installation(installation_id) when is_binary(installation_id) do
    ActiveBindingSet.by_installation(installation_id)
  end

  @spec resolve_active_binding(keyword() | map()) :: {:ok, map()} | {:error, term()}
  def resolve_active_binding(attrs) when is_list(attrs) or is_map(attrs) do
    with {:ok, request} <- resolve_request(attrs),
         {:ok, active} <-
           ActiveBindingSet.by_scope(request.tenant_id, request.environment, request.pack_slug),
         :ok <- ensure_expected_epoch(active, request.expected_binding_epoch),
         {:ok, binding_set} <- Ash.get(BindingSet, active.binding_set_id),
         {:ok, compiled_binding} <-
           CompiledBinding.by_set_ref(active.binding_set_id, request.binding_ref),
         :ok <- ensure_expected_kind(compiled_binding, request.binding_kind),
         {:ok, dependencies, operation_dependency} <-
           fetch_dependencies(compiled_binding, request) do
      {:ok, resolution(active, binding_set, compiled_binding, dependencies, operation_dependency)}
    end
  end

  @spec capture_run_binding_snapshot(keyword() | map()) ::
          {:ok, RunBindingSnapshot.t()} | {:error, term()}
  def capture_run_binding_snapshot(attrs) when is_list(attrs) or is_map(attrs) do
    with {:ok, request} <- snapshot_request(attrs) do
      case RunBindingSnapshot.by_run_binding(
             request.tenant_id,
             request.environment,
             request.run_ref,
             request.binding_ref
           ) do
        {:ok, %RunBindingSnapshot{} = snapshot} ->
          {:ok, snapshot}

        {:error, _not_found} ->
          capture_new_run_binding_snapshot(request)
      end
    end
  end

  @spec resolve_run_binding_snapshot(keyword() | map()) ::
          {:ok, RunBindingSnapshot.t()} | {:error, term()}
  def resolve_run_binding_snapshot(attrs) when is_list(attrs) or is_map(attrs) do
    with {:ok, request} <- snapshot_lookup_request(attrs) do
      RunBindingSnapshot.by_run_binding(
        request.tenant_id,
        request.environment,
        request.run_ref,
        request.binding_ref
      )
    end
  end

  @spec binding_set_gc_status(String.t()) :: {:ok, map()} | {:error, term()}
  def binding_set_gc_status(binding_set_id) when is_binary(binding_set_id) do
    with {:ok, %BindingSet{} = binding_set} <- Ash.get(BindingSet, binding_set_id),
         {:ok, snapshots} <- RunBindingSnapshot.by_binding_set(binding_set_id),
         {:ok, active?} <- active_binding_set?(binding_set) do
      snapshot_count = length(snapshots)
      status = gc_status(binding_set, active?, snapshot_count)

      {:ok,
       %{
         binding_set_id: binding_set.id,
         binding_set_status: binding_set.status,
         active?: active?,
         snapshot_count: snapshot_count,
         status: status,
         eligible?: status == :eligible
       }}
    end
  end

  defp activate_for_installation_transaction(%Installation{} = installation, opts) do
    with {:ok, %PackRegistration{} = registration} <-
           Ash.get(PackRegistration, installation.pack_registration_id),
         {:ok, %CompiledPack{} = compiled_pack} <-
           Serializer.deserialize_compiled(registration.compiled_manifest),
         {:ok, binding_epoch} <- next_binding_epoch(),
         {:ok, binding_set} <-
           create_binding_set(installation, registration, binding_epoch, opts),
         {:ok, compiled_bindings, manifest_dependencies} <-
           create_compiled_bindings(binding_set, compiled_pack),
         {:ok, active_binding_set} <-
           upsert_active_binding_set(installation, binding_set, opts) do
      {:ok,
       %{
         binding_set: binding_set,
         active_binding_set: active_binding_set,
         compiled_bindings: compiled_bindings,
         manifest_dependencies: manifest_dependencies
       }}
    end
  end

  defp create_binding_set(
         %Installation{} = installation,
         %PackRegistration{} = registration,
         epoch,
         opts
       ) do
    %{
      tenant_id: installation.tenant_id,
      environment: installation.environment,
      pack_slug: installation.pack_slug,
      installation_id: installation.id,
      pack_registration_id: registration.id,
      compiled_pack_revision: installation.compiled_pack_revision,
      binding_epoch: epoch,
      status: :active,
      binding_config: installation.binding_config,
      metadata:
        %{
          "activation_reason" => activation_reason(opts),
          "registration_version" => registration.version
        }
        |> maybe_put_metadata("trace_id", Keyword.get(opts, :trace_id))
    }
    |> BindingSet.register(return_notifications?: true)
    |> action_result()
  end

  defp create_compiled_bindings(%BindingSet{} = binding_set, %CompiledPack{} = compiled_pack) do
    compiled_pack.bindings_by_ref
    |> Map.values()
    |> Enum.sort_by(&to_string(&1.binding_ref))
    |> Enum.reduce_while({:ok, [], []}, fn binding, {:ok, bindings, dependencies} ->
      case create_compiled_binding(binding_set, binding) do
        {:ok, compiled_binding, binding_dependencies} ->
          {:cont, {:ok, [compiled_binding | bindings], binding_dependencies ++ dependencies}}

        {:error, reason} ->
          {:halt, {:error, reason}}
      end
    end)
    |> case do
      {:ok, bindings, dependencies} -> {:ok, Enum.reverse(bindings), Enum.reverse(dependencies)}
      {:error, reason} -> {:error, reason}
    end
  end

  defp create_compiled_binding(%BindingSet{} = binding_set, binding) do
    with {:ok, attrs} <- compiled_binding_attrs(binding_set, binding),
         {:ok, %CompiledBinding{} = compiled_binding} <-
           attrs |> CompiledBinding.register(return_notifications?: true) |> action_result(),
         {:ok, dependencies} <- create_manifest_dependencies(compiled_binding, binding) do
      {:ok, compiled_binding, dependencies}
    end
  end

  defp create_manifest_dependencies(%CompiledBinding{} = compiled_binding, binding) do
    binding
    |> operation_refs()
    |> Enum.sort_by(fn {role, operation_ref} -> {role, operation_ref} end)
    |> Enum.reduce_while({:ok, []}, fn {role, operation_ref}, {:ok, dependencies} ->
      attrs = manifest_dependency_attrs(compiled_binding, binding, role, operation_ref)

      case attrs
           |> BindingManifestDependency.register(return_notifications?: true)
           |> action_result() do
        {:ok, dependency} -> {:cont, {:ok, [dependency | dependencies]}}
        {:error, reason} -> {:halt, {:error, reason}}
      end
    end)
    |> case do
      {:ok, dependencies} -> {:ok, Enum.reverse(dependencies)}
      {:error, reason} -> {:error, reason}
    end
  end

  defp upsert_active_binding_set(
         %Installation{} = installation,
         %BindingSet{} = binding_set,
         opts
       ) do
    attrs = %{
      tenant_id: installation.tenant_id,
      environment: installation.environment,
      pack_slug: installation.pack_slug,
      installation_id: installation.id,
      binding_set_id: binding_set.id,
      binding_epoch: binding_set.binding_epoch,
      compiled_pack_revision: installation.compiled_pack_revision,
      metadata:
        %{}
        |> maybe_put_metadata("trace_id", Keyword.get(opts, :trace_id))
    }

    case ActiveBindingSet.by_scope(
           installation.tenant_id,
           installation.environment,
           installation.pack_slug
         ) do
      {:ok, %ActiveBindingSet{} = active} ->
        with :ok <- retire_superseded_binding_set(active, binding_set) do
          active
          |> ActiveBindingSet.replace_binding_set(
            Map.drop(attrs, [:tenant_id, :environment, :pack_slug]),
            return_notifications?: true
          )
          |> action_result()
        end

      {:error, _not_found} ->
        attrs |> ActiveBindingSet.activate(return_notifications?: true) |> action_result()
    end
  end

  defp retire_superseded_binding_set(
         %ActiveBindingSet{binding_set_id: binding_set_id},
         %BindingSet{id: binding_set_id}
       ),
       do: :ok

  defp retire_superseded_binding_set(%ActiveBindingSet{} = active, %BindingSet{} = _binding_set) do
    with {:ok, %BindingSet{} = old_binding_set} <- Ash.get(BindingSet, active.binding_set_id),
         {:ok, _retired_binding_set} <-
           old_binding_set |> BindingSet.retire(return_notifications?: true) |> action_result() do
      :ok
    end
  end

  defp capture_new_run_binding_snapshot(request) do
    with {:ok, resolution} <-
           resolve_active_binding(
             tenant_id: request.tenant_id,
             environment: request.environment,
             pack_slug: request.pack_slug,
             binding_ref: request.binding_ref,
             binding_kind: request.binding_kind,
             expected_binding_epoch: request.expected_binding_epoch
           ) do
      RunBindingSnapshot.capture(%{
        snapshot_ref:
          snapshot_ref(request.run_ref, request.binding_ref, resolution.binding_epoch),
        tenant_id: request.tenant_id,
        environment: request.environment,
        pack_slug: request.pack_slug,
        run_ref: request.run_ref,
        binding_ref: resolution.compiled_binding.binding_ref,
        binding_kind: resolution.compiled_binding.binding_kind,
        binding_set_id: resolution.binding_set.id,
        compiled_binding_id: resolution.compiled_binding.id,
        binding_epoch: resolution.binding_epoch,
        compiled_pack_revision: resolution.binding_set.compiled_pack_revision,
        descriptor: store_descriptor(resolution.descriptor),
        manifest_dependencies: store_dependencies(resolution.manifest_dependencies),
        metadata: %{}
      })
    end
  end

  defp next_binding_epoch do
    %{rows: [[epoch]]} = Repo.query!("SELECT nextval('binding_registry_epoch_seq')", [])
    {:ok, epoch}
  rescue
    error -> {:error, {:binding_epoch_allocation_failed, Exception.message(error)}}
  end

  defp compiled_binding_attrs(%BindingSet{} = binding_set, binding) do
    {:ok,
     %{
       binding_set_id: binding_set.id,
       binding_ref: identifier!(binding.binding_ref, :binding_ref),
       binding_kind: binding_kind(binding),
       connector_ref: identifier!(binding.connector_ref, :connector_ref),
       manifest_ref: identifier!(binding.manifest_ref, :manifest_ref),
       credential_binding_ref:
         identifier!(binding.credential_binding_ref, :credential_binding_ref),
       runtime_family: runtime_family(binding),
       operation_refs: operation_refs(binding),
       policy_refs: policy_refs(binding),
       checksum: binding_checksum(binding),
       binding_payload: dump_struct(binding),
       metadata: dump_value(Map.get(binding, :metadata, %{}))
     }}
  rescue
    error in [ArgumentError, KeyError] ->
      {:error, {:invalid_binding_record, Exception.message(error)}}
  end

  defp manifest_dependency_attrs(
         %CompiledBinding{} = compiled_binding,
         binding,
         role,
         operation_ref
       ) do
    metadata = Map.get(binding, :metadata, %{})

    %{
      binding_set_id: compiled_binding.binding_set_id,
      compiled_binding_id: compiled_binding.id,
      binding_ref: compiled_binding.binding_ref,
      binding_kind: compiled_binding.binding_kind,
      connector_ref: compiled_binding.connector_ref,
      manifest_ref: compiled_binding.manifest_ref,
      operation_role: role,
      operation_ref: operation_ref,
      operation_class: metadata_value(metadata, "operation_classes", role) || role,
      side_effect_class: metadata_value(metadata, "side_effect_classes", role),
      credential_scope_ref: compiled_binding.credential_binding_ref,
      required_runtime_family: compiled_binding.runtime_family,
      manifest_digest: metadata_scalar(metadata, "manifest_digest"),
      required_scopes: required_scopes(metadata, role),
      metadata: %{}
    }
  end

  defp resolution(
         %ActiveBindingSet{} = active,
         %BindingSet{} = binding_set,
         %CompiledBinding{} = compiled_binding,
         dependencies,
         operation_dependency
       ) do
    descriptor = %{
      binding_ref: compiled_binding.binding_ref,
      binding_kind: compiled_binding.binding_kind,
      connector_ref: compiled_binding.connector_ref,
      manifest_ref: compiled_binding.manifest_ref,
      operation_refs: compiled_binding.operation_refs,
      policy_refs: compiled_binding.policy_refs,
      checksum: compiled_binding.checksum,
      credential_binding_ref: compiled_binding.credential_binding_ref,
      runtime_family: compiled_binding.runtime_family,
      binding_epoch: active.binding_epoch,
      binding_set_id: active.binding_set_id,
      compiled_binding_id: compiled_binding.id
    }

    %{
      active_binding_set: active,
      binding_set: binding_set,
      compiled_binding: compiled_binding,
      manifest_dependencies: Enum.sort_by(dependencies, & &1.operation_role),
      operation_dependency: operation_dependency,
      descriptor: descriptor,
      binding_epoch: active.binding_epoch
    }
  end

  defp fetch_dependencies(%CompiledBinding{} = compiled_binding, %{operation_role: nil}) do
    with {:ok, dependencies} <- BindingManifestDependency.by_binding(compiled_binding.id) do
      {:ok, dependencies, nil}
    end
  end

  defp fetch_dependencies(%CompiledBinding{} = compiled_binding, %{operation_role: operation_role}) do
    case BindingManifestDependency.by_binding_role(compiled_binding.id, operation_role) do
      {:ok, dependency} ->
        {:ok, [dependency], dependency}

      {:error, _not_found} ->
        {:error,
         {:missing_binding_operation_role,
          %{binding_ref: compiled_binding.binding_ref, operation_role: operation_role}}}
    end
  end

  defp active_binding_set?(%BindingSet{} = binding_set) do
    case ActiveBindingSet.by_scope(
           binding_set.tenant_id,
           binding_set.environment,
           binding_set.pack_slug
         ) do
      {:ok, %ActiveBindingSet{} = active} -> {:ok, active.binding_set_id == binding_set.id}
      {:error, _not_found} -> {:ok, false}
    end
  end

  defp gc_status(_binding_set, true, _snapshot_count), do: :active

  defp gc_status(_binding_set, false, snapshot_count) when snapshot_count > 0,
    do: :retained_by_run_snapshots

  defp gc_status(%BindingSet{status: :retired}, false, 0), do: :eligible
  defp gc_status(_binding_set, false, 0), do: :not_retired

  defp ensure_expected_epoch(_active, nil), do: :ok

  defp ensure_expected_epoch(%ActiveBindingSet{} = active, expected_epoch) do
    if active.binding_epoch == expected_epoch do
      :ok
    else
      {:error,
       {:stale_binding_epoch,
        %{
          tenant_id: active.tenant_id,
          environment: active.environment,
          pack_slug: active.pack_slug,
          expected_binding_epoch: expected_epoch,
          current_binding_epoch: active.binding_epoch
        }}}
    end
  end

  defp ensure_expected_kind(_compiled_binding, nil), do: :ok

  defp ensure_expected_kind(%CompiledBinding{} = compiled_binding, expected_kind) do
    if compiled_binding.binding_kind == expected_kind do
      :ok
    else
      {:error,
       {:binding_kind_mismatch,
        %{
          binding_ref: compiled_binding.binding_ref,
          expected_binding_kind: expected_kind,
          actual_binding_kind: compiled_binding.binding_kind
        }}}
    end
  end

  defp resolve_request(attrs) do
    with {:ok, tenant_id} <- required_attr(attrs, :tenant_id),
         {:ok, environment} <- required_attr(attrs, :environment),
         {:ok, pack_slug} <- required_attr(attrs, :pack_slug),
         {:ok, binding_ref} <- required_attr(attrs, :binding_ref),
         {:ok, binding_kind} <- optional_binding_kind(attr(attrs, :binding_kind)),
         {:ok, expected_epoch} <- optional_positive_integer(attr(attrs, :expected_binding_epoch)),
         {:ok, operation_role} <-
           optional_identifier(attr(attrs, :operation_role), :operation_role) do
      {:ok,
       %{
         tenant_id: tenant_id,
         environment: environment,
         pack_slug: pack_slug,
         binding_ref: binding_ref,
         binding_kind: binding_kind,
         expected_binding_epoch: expected_epoch,
         operation_role: operation_role
       }}
    end
  end

  defp snapshot_request(attrs) do
    with {:ok, request} <- resolve_request(attrs),
         {:ok, run_ref} <- required_attr(attrs, :run_ref) do
      {:ok, Map.put(request, :run_ref, run_ref)}
    end
  end

  defp snapshot_lookup_request(attrs) do
    with {:ok, tenant_id} <- required_attr(attrs, :tenant_id),
         {:ok, environment} <- required_attr(attrs, :environment),
         {:ok, run_ref} <- required_attr(attrs, :run_ref),
         {:ok, binding_ref} <- required_attr(attrs, :binding_ref) do
      {:ok,
       %{
         tenant_id: tenant_id,
         environment: environment,
         run_ref: run_ref,
         binding_ref: binding_ref
       }}
    end
  end

  defp required_attr(attrs, key) do
    case attr(attrs, key) do
      nil -> {:error, {:missing_required_binding_registry_attr, key}}
      value -> {:ok, identifier!(value, key)}
    end
  rescue
    error in ArgumentError ->
      {:error, {:invalid_binding_registry_attr, key, Exception.message(error)}}
  end

  defp attr(attrs, key) when is_list(attrs), do: Keyword.get(attrs, key)

  defp attr(attrs, key) when is_map(attrs) do
    Map.get(attrs, key) || Map.get(attrs, Atom.to_string(key))
  end

  defp optional_positive_integer(nil), do: {:ok, nil}
  defp optional_positive_integer(value) when is_integer(value) and value > 0, do: {:ok, value}
  defp optional_positive_integer(value), do: {:error, {:invalid_expected_binding_epoch, value}}

  defp optional_binding_kind(nil), do: {:ok, nil}

  defp optional_binding_kind(value)
       when value in [
              :source,
              :source_publication,
              :runtime,
              :runtime_tool,
              :evidence,
              :resource_effect
            ],
       do: {:ok, value}

  defp optional_binding_kind(value) when is_binary(value) do
    value
    |> case do
      "source" -> {:ok, :source}
      "source_publication" -> {:ok, :source_publication}
      "runtime" -> {:ok, :runtime}
      "runtime_tool" -> {:ok, :runtime_tool}
      "evidence" -> {:ok, :evidence}
      "resource_effect" -> {:ok, :resource_effect}
      _other -> {:error, {:invalid_binding_kind, value}}
    end
  end

  defp optional_binding_kind(value), do: {:error, {:invalid_binding_kind, value}}

  defp optional_identifier(nil, _key), do: {:ok, nil}

  defp optional_identifier(value, key) do
    {:ok, identifier!(value, key)}
  rescue
    error in ArgumentError ->
      {:error, {:invalid_binding_registry_attr, key, Exception.message(error)}}
  end

  defp identifier!(value, _key) when is_atom(value), do: Atom.to_string(value)

  defp identifier!(value, key) when is_binary(value) do
    if String.trim(value) == "" do
      raise ArgumentError, "#{key} must be non-empty"
    else
      value
    end
  end

  defp identifier!(value, key),
    do: raise(ArgumentError, "#{key} must be an atom or string, got #{inspect(value)}")

  defp operation_refs(binding) do
    binding.operation_refs
    |> Map.new(fn {role, operation_ref} ->
      {identifier!(role, :operation_role), identifier!(operation_ref, :operation_ref)}
    end)
  end

  defp policy_refs(binding) do
    binding
    |> Map.from_struct()
    |> Map.drop([:__struct__, :metadata])
    |> Enum.reduce(metadata_policy_refs(Map.get(binding, :metadata, %{})), fn {field, value},
                                                                              refs ->
      if policy_ref_field?(field) do
        refs ++ List.wrap(value)
      else
        refs
      end
    end)
    |> Enum.reject(&is_nil/1)
    |> Enum.map(&identifier!(&1, :policy_ref))
    |> Enum.uniq()
    |> Enum.sort()
  end

  defp policy_ref_field?(field) do
    field_name = Atom.to_string(field)
    String.ends_with?(field_name, "_policy_ref") or String.ends_with?(field_name, "_profile_ref")
  end

  defp metadata_policy_refs(metadata) when is_map(metadata) do
    metadata
    |> Map.get(:policy_refs, Map.get(metadata, "policy_refs", []))
    |> List.wrap()
  end

  defp metadata_policy_refs(_metadata), do: []

  defp binding_checksum(binding) do
    payload = dump_struct(binding)
    digest = :crypto.hash(:sha256, :erlang.term_to_binary(payload))

    "sha256:" <> Base.encode16(digest, case: :lower)
  end

  defp runtime_family(%{runtime_family: value}) when not is_nil(value),
    do: identifier!(value, :runtime_family)

  defp runtime_family(_binding), do: nil

  defp binding_kind(%{__struct__: Mezzanine.Pack.SourceBinding}), do: :source

  defp binding_kind(%{__struct__: Mezzanine.Pack.SourcePublicationBinding}),
    do: :source_publication

  defp binding_kind(%{__struct__: Mezzanine.Pack.RuntimeBinding}), do: :runtime
  defp binding_kind(%{__struct__: Mezzanine.Pack.ToolBinding}), do: :runtime_tool
  defp binding_kind(%{__struct__: Mezzanine.Pack.EvidenceBinding}), do: :evidence
  defp binding_kind(%{__struct__: Mezzanine.Pack.ResourceEffectBinding}), do: :resource_effect

  defp required_scopes(metadata, role) do
    metadata
    |> metadata_value("required_scopes", role)
    |> List.wrap()
    |> Enum.map(&identifier!(&1, :required_scope))
  end

  defp metadata_value(metadata, key, role) when is_map(metadata) do
    case Map.get(metadata, key) || Map.get(metadata, metadata_atom_key(key)) do
      value when is_map(value) -> Map.get(value, role) || Map.get(value, metadata_atom_key(role))
      value -> value
    end
  end

  defp metadata_value(_metadata, _key, _role), do: nil

  defp metadata_scalar(metadata, key) when is_map(metadata) do
    case Map.get(metadata, key) || Map.get(metadata, metadata_atom_key(key)) do
      value when is_binary(value) -> value
      nil -> nil
      value when is_atom(value) -> Atom.to_string(value)
      _other -> nil
    end
  end

  defp metadata_scalar(_metadata, _key), do: nil

  defp metadata_atom_key("manifest_digest"), do: :manifest_digest
  defp metadata_atom_key("operation_classes"), do: :operation_classes
  defp metadata_atom_key("required_scopes"), do: :required_scopes
  defp metadata_atom_key("side_effect_classes"), do: :side_effect_classes
  defp metadata_atom_key("read"), do: :read
  defp metadata_atom_key("write"), do: :write
  defp metadata_atom_key("run"), do: :run
  defp metadata_atom_key("collect"), do: :collect
  defp metadata_atom_key(value), do: value

  defp dump_struct(binding) do
    binding
    |> Map.from_struct()
    |> Map.new(fn {key, value} -> {Atom.to_string(key), dump_value(value)} end)
  end

  defp dump_value(value) when is_map(value) do
    Map.new(value, fn {key, nested_value} -> {identifier_key(key), dump_value(nested_value)} end)
  end

  defp dump_value(value) when is_list(value), do: Enum.map(value, &dump_value/1)
  defp dump_value(value) when is_boolean(value) or is_nil(value), do: value
  defp dump_value(value) when is_atom(value), do: Atom.to_string(value)
  defp dump_value(value), do: value

  defp identifier_key(key) when is_atom(key), do: Atom.to_string(key)
  defp identifier_key(key), do: key

  defp store_descriptor(descriptor),
    do: Map.new(descriptor, fn {key, value} -> {to_string(key), dump_value(value)} end)

  defp store_dependencies(dependencies) do
    %{
      "items" =>
        Enum.map(dependencies, fn dependency ->
          %{
            "operation_role" => dependency.operation_role,
            "operation_ref" => dependency.operation_ref,
            "operation_class" => dependency.operation_class,
            "connector_ref" => dependency.connector_ref,
            "manifest_ref" => dependency.manifest_ref,
            "credential_scope_ref" => dependency.credential_scope_ref,
            "required_runtime_family" => dependency.required_runtime_family,
            "manifest_digest" => dependency.manifest_digest,
            "required_scopes" => dependency.required_scopes
          }
        end)
    }
  end

  defp snapshot_ref(run_ref, binding_ref, binding_epoch),
    do: "binding-snapshot://#{run_ref}/#{binding_ref}/#{binding_epoch}"

  defp activation_reason(opts),
    do: Keyword.get(opts, :activation_reason, "installation_activation")

  defp maybe_put_metadata(metadata, _key, nil), do: metadata
  defp maybe_put_metadata(metadata, key, value), do: Map.put(metadata, key, value)

  defp action_result({:ok, record, _notifications}), do: {:ok, record}
  defp action_result({:ok, record}), do: {:ok, record}
  defp action_result({:error, reason}), do: {:error, reason}
end
