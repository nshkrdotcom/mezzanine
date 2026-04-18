defmodule MezzanineConfigRegistry do
  @moduledoc """
  Durable neutral pack-registration and installation registry facade.
  """

  alias Mezzanine.ConfigRegistry.{Installation, LifecycleHintContract, PackRegistration}
  alias Mezzanine.Execution.Repo, as: ExecutionRepo
  alias Mezzanine.Leasing
  alias Mezzanine.Pack.{CompiledPack, Registry, Serializer}

  @spec components() :: [module()]
  def components do
    [
      Mezzanine.ConfigRegistry,
      Mezzanine.ConfigRegistry.PackRegistration,
      Mezzanine.ConfigRegistry.Installation,
      Mezzanine.Pack.Registry,
      Mezzanine.Pack.Serializer
    ]
  end

  @spec register_pack(CompiledPack.t()) :: {:ok, PackRegistration.t()} | {:error, term()}
  def register_pack(%CompiledPack{} = compiled_pack) do
    PackRegistration.register(%{
      pack_slug: compiled_pack.pack_slug,
      version: compiled_pack.version,
      compiled_manifest: Serializer.serialize_compiled(compiled_pack),
      canonical_subject_kinds: Map.keys(compiled_pack.subject_kinds),
      migration_strategy: to_string(compiled_pack.manifest.migration_strategy)
    })
  end

  @spec register_pack!(CompiledPack.t()) :: PackRegistration.t()
  def register_pack!(%CompiledPack{} = compiled_pack) do
    case register_pack(compiled_pack) do
      {:ok, registration} -> registration
      {:error, error} -> raise "failed to register pack: #{inspect(error)}"
    end
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

  @spec activate_installation(Installation.t()) :: {:ok, Installation.t()} | {:error, term()}
  def activate_installation(%Installation{} = installation) do
    with :ok <- LifecycleHintContract.validate(installation),
         {:ok, updated_installation} <- Installation.activate_installation(installation) do
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

  @spec update_bindings(Installation.t(), map()) :: {:ok, Installation.t()} | {:error, term()}
  def update_bindings(%Installation{} = installation, binding_config)
      when is_map(binding_config) do
    with :ok <- LifecycleHintContract.validate(installation, binding_config),
         {:ok, updated_installation} <-
           Installation.update_bindings(installation, %{binding_config: binding_config}) do
      :ok =
        Registry.reload_installation(
          updated_installation.id,
          updated_installation.compiled_pack_revision
        )

      {:ok, updated_installation}
    end
  end
end
