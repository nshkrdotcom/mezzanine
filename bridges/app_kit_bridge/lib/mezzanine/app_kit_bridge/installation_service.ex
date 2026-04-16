defmodule Mezzanine.AppKitBridge.InstallationService do
  @moduledoc """
  Backend-oriented tenant installation lifecycle for AppKit consumers.

  Installation semantics stay strictly installation-scoped: the service binds a
  tenant/environment to an already activated pack registration and refuses to
  absorb deployment or pack-registration responsibilities by stealth.
  """

  require Ash.Query

  alias Mezzanine.ConfigRegistry.{Installation, PackRegistration}

  @deployment_keys [
    :compiled_manifest,
    :serializer_version,
    :migration_strategy,
    :canonical_subject_kinds,
    :register_pack,
    :activate_pack,
    :deployment
  ]

  @spec create_installation(map(), keyword()) :: {:ok, map()} | {:error, term()}
  def create_installation(attrs, opts \\ []) when is_map(attrs) and is_list(opts) do
    attrs = Map.new(attrs)

    with :ok <- reject_deployment_fields(attrs),
         {:ok, tenant_id} <- fetch_string(attrs, opts, :tenant_id),
         {:ok, pack_slug} <- fetch_string(attrs, opts, :pack_slug),
         {:ok, pack_version} <- fetch_string(attrs, opts, :pack_version),
         environment <- optional_string(attrs, opts, :environment, "default"),
         {:ok, pack_registration} <- fetch_active_pack_registration(pack_slug, pack_version),
         {:ok, existing_installation} <- find_installation(tenant_id, environment, pack_slug) do
      binding_config = binding_config(attrs)
      metadata = installation_metadata(attrs)

      case existing_installation do
        nil ->
          create_active_installation(
            tenant_id,
            environment,
            pack_registration,
            binding_config,
            metadata
          )

        %Installation{} = installation ->
          reuse_or_update_installation(installation, pack_registration, binding_config)
      end
    end
  end

  @spec get_installation(Ecto.UUID.t(), keyword()) :: {:ok, map()} | {:error, term()}
  def get_installation(installation_id, _opts \\ []) when is_binary(installation_id) do
    with {:ok, installation} <- fetch_installation(installation_id) do
      installation_detail(installation)
    end
  end

  @spec list_installations(String.t(), map(), keyword()) :: {:ok, [map()]} | {:error, term()}
  def list_installations(tenant_id, filters \\ %{}, _opts \\ [])
      when is_binary(tenant_id) and is_map(filters) do
    with {:ok, installations} <- list_tenant_installations(tenant_id) do
      {:ok,
       installations
       |> Enum.filter(&matches_filters?(&1, filters))
       |> Enum.map(&installation_detail!/1)
       |> Enum.sort_by(&{&1.environment, &1.installation_ref.id})}
    end
  end

  @spec update_bindings(Ecto.UUID.t(), map(), keyword()) :: {:ok, map()} | {:error, term()}
  def update_bindings(installation_id, binding_config, _opts \\ [])
      when is_binary(installation_id) and is_map(binding_config) do
    with {:ok, installation} <- fetch_installation(installation_id),
         {:ok, updated_installation} <-
           MezzanineConfigRegistry.update_bindings(installation, binding_config),
         {:ok, detail} <- installation_detail(updated_installation) do
      {:ok, action_result(detail, :update_bindings, "Bindings updated")}
    end
  end

  @spec suspend_installation(Ecto.UUID.t(), keyword()) :: {:ok, map()} | {:error, term()}
  def suspend_installation(installation_id, _opts \\ []) when is_binary(installation_id) do
    with {:ok, installation} <- fetch_installation(installation_id),
         {:ok, suspended_installation} <-
           MezzanineConfigRegistry.suspend_installation(installation),
         {:ok, detail} <- installation_detail(suspended_installation) do
      {:ok, action_result(detail, :suspend_installation, "Installation suspended")}
    end
  end

  @spec reactivate_installation(Ecto.UUID.t(), keyword()) :: {:ok, map()} | {:error, term()}
  def reactivate_installation(installation_id, _opts \\ []) when is_binary(installation_id) do
    with {:ok, installation} <- fetch_installation(installation_id),
         {:ok, active_installation} <- ensure_active(installation),
         {:ok, detail} <- installation_detail(active_installation) do
      {:ok, action_result(detail, :reactivate_installation, "Installation active")}
    end
  end

  defp create_active_installation(
         tenant_id,
         environment,
         pack_registration,
         binding_config,
         metadata
       ) do
    with {:ok, installation} <-
           MezzanineConfigRegistry.create_installation(%{
             tenant_id: tenant_id,
             environment: environment,
             pack_registration_id: pack_registration.id,
             binding_config: binding_config,
             metadata: metadata
           }),
         {:ok, active_installation} <- MezzanineConfigRegistry.activate_installation(installation),
         {:ok, detail} <- installation_detail(active_installation) do
      {:ok,
       %{
         installation_ref: detail.installation_ref,
         status: :created,
         message: "Installation created",
         metadata: %{installation: detail}
       }}
    end
  end

  defp reuse_or_update_installation(installation, pack_registration, binding_config) do
    if installation.pack_registration_id != pack_registration.id do
      {:error, :installation_pack_conflict}
    else
      with {:ok, installation} <- maybe_update_bindings(installation, binding_config),
           {:ok, active_installation} <- ensure_active(installation),
           {:ok, detail} <- installation_detail(active_installation) do
        status = result_status(installation, active_installation, binding_config)

        {:ok,
         %{
           installation_ref: detail.installation_ref,
           status: status,
           message: reuse_message(status),
           metadata: %{installation: detail}
         }}
      end
    end
  end

  defp maybe_update_bindings(%Installation{} = installation, binding_config) do
    if installation.binding_config == binding_config do
      {:ok, installation}
    else
      MezzanineConfigRegistry.update_bindings(installation, binding_config)
    end
  end

  defp ensure_active(%Installation{status: :active} = installation), do: {:ok, installation}

  defp ensure_active(%Installation{status: :inactive} = installation),
    do: MezzanineConfigRegistry.activate_installation(installation)

  defp ensure_active(%Installation{} = installation),
    do: MezzanineConfigRegistry.reactivate_installation(installation)

  defp result_status(original_installation, active_installation, desired_bindings) do
    if original_installation.status == :active and
         original_installation.binding_config == desired_bindings and
         active_installation.compiled_pack_revision ==
           original_installation.compiled_pack_revision do
      :reused
    else
      :updated
    end
  end

  defp reuse_message(:reused), do: "Installation already active"
  defp reuse_message(:updated), do: "Installation updated"

  defp fetch_installation(installation_id) do
    case Ash.get(Installation, installation_id) do
      {:ok, %Installation{} = installation} -> load_installation(installation)
      {:error, %Ash.Error.Invalid{}} -> {:error, :not_found}
      {:error, reason} -> {:error, reason}
    end
  end

  defp list_tenant_installations(tenant_id) do
    Installation
    |> Ash.Query.filter(tenant_id == ^tenant_id)
    |> Ash.read(domain: Mezzanine.ConfigRegistry)
    |> case do
      {:ok, installations} -> {:ok, Enum.map(installations, &load_installation!/1)}
      {:error, reason} -> {:error, reason}
    end
  end

  defp find_installation(tenant_id, environment, pack_slug) do
    Installation
    |> Ash.Query.filter(
      tenant_id == ^tenant_id and environment == ^environment and pack_slug == ^pack_slug
    )
    |> Ash.read(domain: Mezzanine.ConfigRegistry)
    |> case do
      {:ok, [installation | _]} -> {:ok, load_installation!(installation)}
      {:ok, []} -> {:ok, nil}
      {:error, reason} -> {:error, reason}
    end
  end

  defp fetch_active_pack_registration(pack_slug, pack_version) do
    case PackRegistration.by_slug_version(pack_slug, pack_version) do
      {:ok, %PackRegistration{status: :active} = registration} -> {:ok, registration}
      {:ok, %PackRegistration{}} -> {:error, :pack_registration_not_active}
      {:error, _reason} -> {:error, :pack_registration_not_found}
    end
  end

  defp installation_detail(%Installation{} = installation) do
    {:ok, installation_detail!(installation)}
  end

  defp installation_detail!(%Installation{} = installation) do
    installation = load_installation!(installation)

    %{
      installation_ref: installation_ref(installation),
      tenant_id: installation.tenant_id,
      environment: installation.environment,
      bindings: installation.binding_config,
      metadata: installation.metadata,
      pack_registration_id: installation.pack_registration_id
    }
  end

  defp installation_ref(%Installation{} = installation) do
    %{
      id: installation.id,
      pack_slug: installation.pack_slug,
      pack_version: installation.pack_registration.version,
      compiled_pack_revision: installation.compiled_pack_revision,
      status: installation.status
    }
  end

  defp action_result(detail, action, message) do
    %{
      status: :completed,
      action_ref: %{
        id: "#{detail.installation_ref.id}:#{action}",
        action_kind: Atom.to_string(action),
        installation_ref: detail.installation_ref
      },
      message: message,
      metadata: %{installation: detail}
    }
  end

  defp matches_filters?(installation, filters) do
    status = map_value(filters, :status)
    environment = map_value(filters, :environment)
    pack_slug = map_value(filters, :pack_slug)

    (is_nil(status) or installation.installation_ref.status == normalize_status(status)) and
      (is_nil(environment) or installation.environment == environment) and
      (is_nil(pack_slug) or installation.installation_ref.pack_slug == pack_slug)
  end

  defp load_installation(%Installation{} = installation) do
    installation
    |> Ash.load([:pack_registration], domain: Mezzanine.ConfigRegistry)
    |> case do
      {:ok, loaded_installation} -> {:ok, loaded_installation}
      {:error, reason} -> {:error, reason}
    end
  end

  defp load_installation!(%Installation{} = installation) do
    case load_installation(installation) do
      {:ok, loaded_installation} -> loaded_installation
      {:error, reason} -> raise "failed to load installation: #{inspect(reason)}"
    end
  end

  defp binding_config(attrs),
    do: map_value(attrs, :default_bindings) || map_value(attrs, :bindings) || %{}

  defp installation_metadata(attrs) do
    metadata = map_value(attrs, :metadata) || %{}
    template_key = map_value(attrs, :template_key)

    if is_binary(template_key) do
      Map.put(metadata, "template_key", template_key)
    else
      metadata
    end
  end

  defp reject_deployment_fields(attrs) do
    if Enum.any?(@deployment_keys, &Map.has_key?(attrs, &1)) or
         Enum.any?(@deployment_keys, &Map.has_key?(attrs, Atom.to_string(&1))) do
      {:error, :installation_payload_contains_deployment_fields}
    else
      :ok
    end
  end

  defp fetch_string(attrs, opts, key) do
    case Keyword.get(opts, key) || map_value(attrs, key) do
      value when is_binary(value) and value != "" -> {:ok, value}
      _ -> {:error, {:missing_required_field, key}}
    end
  end

  defp optional_string(attrs, opts, key, default) do
    case Keyword.get(opts, key) || map_value(attrs, key) do
      value when is_binary(value) and value != "" -> value
      _ -> default
    end
  end

  defp map_value(attrs, key), do: Map.get(attrs, key) || Map.get(attrs, Atom.to_string(key))

  defp normalize_status(status) when status in [:inactive, :active, :suspended, :degraded],
    do: status

  defp normalize_status(status) when is_binary(status) do
    case status do
      "inactive" -> :inactive
      "active" -> :active
      "suspended" -> :suspended
      "degraded" -> :degraded
      _ -> status
    end
  end
end
