defmodule Mezzanine.Persistence do
  @moduledoc """
  Mezzanine persistence profile helpers backed by GroundPlane policy data.

  This module is pure coordination logic. It does not read environment state,
  start substrates, or inspect durable backends.
  """

  alias GroundPlane.PersistencePolicy
  alias GroundPlane.PersistencePolicy.StoreCapability

  @restart_safe_claims [:restart_safe, :durable, :durable_workflow, :durable_artifact]

  @type profile :: PersistencePolicy.Profile.t()
  @type store_capability :: StoreCapability.t()

  @spec resolve(keyword() | map()) :: {:ok, PersistencePolicy.Profile.t()} | {:error, term()}
  def resolve(attrs), do: PersistencePolicy.resolve(attrs)

  @spec resolve!(keyword() | map()) :: PersistencePolicy.Profile.t()
  def resolve!(attrs), do: PersistencePolicy.resolve!(attrs)

  @spec restart_safe?(PersistencePolicy.Profile.t()) :: boolean()
  def restart_safe?(%PersistencePolicy.Profile{metadata: metadata}) do
    Map.get(metadata, :restart_claim) in @restart_safe_claims
  end

  @spec postgres_capability(atom(), [atom()]) :: StoreCapability.t()
  def postgres_capability(component, data_classes \\ [:runtime_state]) when is_atom(component) do
    capability!(
      store_ref: component,
      tier: :postgres_shared,
      data_classes: data_classes,
      adapter: :ash_postgres,
      restart_safe?: true
    )
  end

  @spec capability!(keyword() | map()) :: StoreCapability.t()
  def capability!(attrs) do
    case StoreCapability.new(attrs) do
      {:ok, capability} -> capability
      {:error, reason} -> raise ArgumentError, message: inspect(reason)
    end
  end

  @spec preflight(keyword() | map(), [StoreCapability.t()]) :: :ok | {:error, term()}
  def preflight(attrs, capabilities) do
    profile = resolve!(attrs)
    PersistencePolicy.preflight(profile, capabilities, fn _capability -> :ok end)
  end

  @spec postgres_preflight(atom(), keyword() | map()) :: :ok | {:error, term()}
  def postgres_preflight(component, attrs) when is_atom(component) do
    profile = resolve!(attrs)

    if profile.default_tier == :postgres_shared,
      do: require_migration_proof(component, attrs),
      else: {:error, {:unsupported_persistence_tier, component, profile.default_tier}}
  end

  defp require_migration_proof(component, attrs) do
    attrs = Map.new(attrs)

    case Map.get(attrs, :migration_proof) || Map.get(attrs, "migration_proof") do
      :present -> :ok
      true -> :ok
      paths when is_list(paths) and paths != [] -> :ok
      _missing -> {:error, {:missing_migration_proof, component}}
    end
  end
end
