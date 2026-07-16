defmodule Mezzanine.Projections.Store do
  @moduledoc "Persistence facade for projection state."

  alias Mezzanine.Projections.Store.AshPostgres

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

  @callback capabilities() :: Mezzanine.Persistence.store_capability()
  @callback preflight(keyword() | map()) :: :ok | {:error, term()}
  @callback put_record(map(), keyword()) :: {:ok, map()} | {:error, term()}
  @callback fetch_record(term(), keyword()) :: {:ok, map()} | {:error, term()}
  @callback update_record(term(), map(), keyword()) :: {:ok, map()} | {:error, term()}
  @callback append_event(term(), map(), keyword()) :: {:ok, map()} | {:error, term()}
  @callback health(keyword()) :: {:ok, map()} | {:error, term()}
  @callback resource_modules() :: [module()]

  def adapter(opts \\ []) do
    reject_memory_profile!(opts)
    AshPostgres
  end

  def capabilities, do: AshPostgres.capabilities()
  def resource_modules, do: AshPostgres.resource_modules()
  def preflight(opts \\ []), do: adapter(opts).preflight(opts)
  def put_record(attrs, opts \\ []), do: adapter(opts).put_record(attrs, opts)
  def fetch_record(id, opts \\ []), do: adapter(opts).fetch_record(id, opts)
  def update_record(id, attrs, opts \\ []), do: adapter(opts).update_record(id, attrs, opts)
  def append_event(id, event, opts \\ []), do: adapter(opts).append_event(id, event, opts)
  def health(opts \\ []), do: adapter(opts).health(opts)

  defp reject_memory_profile!(opts) when is_list(opts) or is_map(opts) do
    attrs = Map.new(opts)

    selected =
      Enum.find_value(@profile_keys, fn key ->
        Map.get(attrs, key) || Map.get(attrs, Atom.to_string(key))
      end)

    if selected in @forbidden_profiles do
      raise ArgumentError, "production projection store cannot select #{inspect(selected)}"
    end
  end
end
