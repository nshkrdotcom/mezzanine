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
end
