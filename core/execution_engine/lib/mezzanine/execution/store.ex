defmodule Mezzanine.Execution.Store do
  @moduledoc """
  Persistence facade for execution runtime state.
  """

  @callback capabilities() :: Mezzanine.Persistence.store_capability()
  @callback preflight(keyword() | map()) :: :ok | {:error, term()}
  @callback health(keyword()) :: {:ok, map()} | {:error, term()}
  @callback resource_modules() :: [module()]

  @durable_adapter Mezzanine.Execution.Store.AshPostgres
  @profile_keys [
    :profile,
    :persistence_profile,
    :restart_profile,
    :session_profile,
    :authority_profile,
    :tenant_policy_profile,
    :host_profile,
    :release_profile,
    :package_profile,
    :global_profile
  ]
  @forbidden_profiles [:mickey_mouse, :memory_debug, "mickey_mouse", "memory_debug"]

  def adapter(opts \\ []) do
    reject_memory_profile!(opts)
    @durable_adapter
  end

  def capabilities, do: @durable_adapter.capabilities()
  def resource_modules, do: @durable_adapter.resource_modules()
  def preflight(opts \\ []), do: adapter(opts).preflight(opts)
  def health(opts \\ []), do: adapter(opts).health(opts)

  defp reject_memory_profile!(opts) when is_list(opts) or is_map(opts) do
    attrs = Map.new(opts)

    selected =
      Enum.find_value(@profile_keys, fn key ->
        Map.get(attrs, key) || Map.get(attrs, Atom.to_string(key))
      end)

    if selected in @forbidden_profiles do
      raise ArgumentError, "production execution store cannot select #{inspect(selected)}"
    end
  end
end
