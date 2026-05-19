defmodule Mezzanine.RuntimeProfile do
  @moduledoc """
  Boot-loaded runtime configuration profile for Mezzanine subsystems.

  Application environment is an input to application boot only. Runtime code
  consumes this profile explicitly, either by receiving it as a value or by
  asking the supervised `Mezzanine.RuntimeProfileStore` owner for the current
  boot snapshot.
  """

  @enforce_keys [:configs]
  defstruct @enforce_keys

  @known_configs %{
    mezzanine_archival_engine: [:cold_store, :scheduler],
    mezzanine_config_registry: [
      :access_graph_store,
      :authoring_signing_key,
      :cluster_invalidation_publisher
    ],
    mezzanine_core: [:workflow_runtime_impl],
    mezzanine_execution_engine: [:lower_gateway_impl],
    mezzanine_leasing: [
      :access_graph_store,
      :default_poll_interval_ms,
      :default_read_ttl_ms,
      :default_stream_ttl_ms
    ],
    mezzanine_workflow_runtime: [
      :integration_bridge,
      :outbox_persistence,
      :receipt_reducer,
      :temporal,
      :temporalex_boundary
    ]
  }

  @known_apps Map.keys(@known_configs)

  @type t :: %__MODULE__{configs: %{optional(atom()) => keyword()}}

  @doc "Returns an empty profile with no configured runtime overrides."
  @spec empty() :: t()
  def empty, do: %__MODULE__{configs: %{}}

  @doc "Builds a profile from the configured applications' boot env snapshot."
  @spec from_env() :: t()
  def from_env, do: from_env_snapshot(env_snapshot())

  @doc "Captures known Mezzanine runtime configuration keys from application env."
  @spec env_snapshot() :: %{optional(atom()) => keyword()}
  def env_snapshot do
    Map.new(@known_apps, fn app -> {app, Application.get_all_env(app)} end)
  end

  @doc "Builds a profile from a caller-supplied environment snapshot."
  @spec from_env_snapshot(map() | keyword()) :: t()
  def from_env_snapshot(snapshot) when is_map(snapshot) or is_list(snapshot) do
    snapshot
    |> Map.new()
    |> Enum.reduce(empty(), fn {app, config}, profile ->
      put_config(profile, app, config)
    end)
  end

  @doc "Returns a profile with `key` set for `app` when the pair is known."
  @spec put(t(), atom(), atom(), term()) :: t()
  def put(%__MODULE__{} = profile, app, key, value)
      when is_atom(app) and is_atom(key) do
    ensure_known_pair!(app, key)

    update_in(profile.configs, fn configs ->
      config =
        configs
        |> Map.get(app, [])
        |> Keyword.put(key, value)

      Map.put(configs, app, config)
    end)
  end

  @doc "Returns a profile with all known keys from `config` set for `app`."
  @spec put_config(t(), atom(), map() | keyword() | nil) :: t()
  def put_config(%__MODULE__{} = profile, app, config) when is_atom(app) do
    case Map.fetch(@known_configs, app) do
      {:ok, known_keys} ->
        filtered =
          config
          |> normalize_config()
          |> Keyword.take(known_keys)

        if filtered == [] do
          profile
        else
          update_in(profile.configs, &Map.put(&1, app, filtered))
        end

      :error ->
        profile
    end
  end

  @doc "Fetches a configured value, returning `default` when absent."
  @spec config(t(), atom(), atom(), term()) :: term()
  def config(%__MODULE__{} = profile, app, key, default \\ nil)
      when is_atom(app) and is_atom(key) do
    ensure_known_pair!(app, key)

    profile.configs
    |> Map.get(app, [])
    |> Keyword.get(key, default)
  end

  @doc "Fetches a configured keyword list, returning `default` when absent."
  @spec keyword_config(t(), atom(), atom(), keyword()) :: keyword()
  def keyword_config(%__MODULE__{} = profile, app, key, default \\ [])
      when is_atom(app) and is_atom(key) and is_list(default) do
    case config(profile, app, key, default) do
      value when is_list(value) -> value
      _other -> default
    end
  end

  @doc "Fetches a configured module, returning `default` when absent or invalid."
  @spec module(t(), atom(), atom(), module()) :: module()
  def module(%__MODULE__{} = profile, app, key, default)
      when is_atom(app) and is_atom(key) and is_atom(default) do
    case config(profile, app, key, default) do
      value when is_atom(value) -> value
      _other -> default
    end
  end

  @doc "Returns the known runtime configuration pairs captured by profiles."
  @spec known_configs() :: %{optional(atom()) => [atom()]}
  def known_configs, do: @known_configs

  defp normalize_config(nil), do: []
  defp normalize_config(config) when is_list(config), do: config
  defp normalize_config(config) when is_map(config), do: Map.to_list(config)
  defp normalize_config(_config), do: []

  defp ensure_known_pair!(app, key) do
    case Map.fetch(@known_configs, app) do
      {:ok, keys} ->
        if key in keys do
          :ok
        else
          raise ArgumentError, "unknown Mezzanine runtime profile key #{inspect({app, key})}"
        end

      :error ->
        raise ArgumentError, "unknown Mezzanine runtime profile app #{inspect(app)}"
    end
  end
end
