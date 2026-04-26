defmodule Mezzanine.Pack.Registry do
  @moduledoc """
  ETS-backed runtime cache for installation-scoped compiled packs.
  """

  use GenServer

  alias Mezzanine.ConfigRegistry.{ClusterInvalidation, Installation}
  alias Mezzanine.Pack.Serializer
  require Logger

  @table :mezzanine_pack_registry
  @cluster_invalidation_publish_event [:mezzanine, :cluster_invalidation, :publish]
  @cluster_invalidation_handler_id {__MODULE__, :cluster_invalidation_publish}

  @spec start_link(keyword()) :: GenServer.on_start()
  def start_link(opts \\ []) do
    GenServer.start_link(__MODULE__, opts, name: __MODULE__)
  end

  @spec get_compiled_pack(String.t(), non_neg_integer() | nil) ::
          {:ok, Mezzanine.Pack.CompiledPack.t()} | {:error, term()}
  def get_compiled_pack(installation_id, expected_revision \\ nil) do
    case :ets.lookup(@table, {:installation, installation_id}) do
      [{_, %{revision: revision, compiled: compiled}}]
      when is_nil(expected_revision) or expected_revision == revision ->
        {:ok, compiled}

      _other ->
        GenServer.call(__MODULE__, {:reload_installation, installation_id, expected_revision})
    end
  end

  @spec get_compiled_pack!(String.t(), non_neg_integer() | nil) :: Mezzanine.Pack.CompiledPack.t()
  def get_compiled_pack!(installation_id, expected_revision \\ nil) do
    case get_compiled_pack(installation_id, expected_revision) do
      {:ok, compiled_pack} ->
        compiled_pack

      {:error, error} ->
        raise "failed to load compiled pack for #{installation_id}: #{inspect(error)}"
    end
  end

  @spec reload_installation(String.t(), non_neg_integer() | nil) :: :ok
  def reload_installation(installation_id, expected_revision \\ nil) do
    case GenServer.call(__MODULE__, {:reload_installation, installation_id, expected_revision}) do
      {:ok, _compiled_pack} ->
        :ok

      {:error, :inactive} ->
        :ok

      {:error, error} ->
        raise "failed to reload installation #{installation_id}: #{inspect(error)}"
    end
  end

  @spec forget_installation(String.t()) :: :ok
  def forget_installation(installation_id) do
    GenServer.call(__MODULE__, {:forget_installation, installation_id})
  end

  @impl true
  def init(_opts) do
    Process.flag(:trap_exit, true)

    table =
      case :ets.whereis(@table) do
        :undefined -> :ets.new(@table, [:named_table, :protected, :set, read_concurrency: true])
        table -> table
      end

    :ok = attach_cluster_invalidation_handler()
    :ok = maybe_subscribe_cache_fanout()
    :ok = load_all_active_installations(table)
    {:ok, %{table: table}}
  end

  @impl true
  def handle_call({:reload_installation, installation_id, expected_revision}, _from, state) do
    {:reply, load_installation(installation_id, expected_revision, state.table), state}
  end

  def handle_call({:forget_installation, installation_id}, _from, state) do
    :ets.delete(state.table, {:installation, installation_id})
    {:reply, :ok, state}
  end

  def handle_call({:cluster_invalidation, %ClusterInvalidation{} = message}, _from, state) do
    evict_cache_for_invalidation(message, state.table)
    {:reply, :ok, state}
  end

  @impl true
  def handle_info({:cluster_invalidation, %ClusterInvalidation{} = message}, state) do
    evict_cache_for_invalidation(message, state.table)
    {:noreply, state}
  end

  @impl true
  def terminate(_reason, _state) do
    detach_cluster_invalidation_handler()
  end

  @doc false
  def handle_cluster_invalidation_publish(
        _event,
        _measurements,
        %{message: %ClusterInvalidation{} = message},
        registry
      )
      when is_pid(registry) do
    evict_cache_from_publisher(registry, message)
    :ok
  end

  def handle_cluster_invalidation_publish(_event, _measurements, _metadata, _registry), do: :ok

  defp evict_cache_from_publisher(registry, %ClusterInvalidation{} = message) do
    cond do
      registry == self() ->
        send(registry, {:cluster_invalidation, message})
        :ok

      Process.alive?(registry) ->
        try do
          GenServer.call(registry, {:cluster_invalidation, message})
        catch
          :exit, _reason -> :ok
        end

      true ->
        :ok
    end
  end

  defp attach_cluster_invalidation_handler do
    :ok = detach_cluster_invalidation_handler()

    :telemetry.attach(
      @cluster_invalidation_handler_id,
      @cluster_invalidation_publish_event,
      &__MODULE__.handle_cluster_invalidation_publish/4,
      self()
    )
  end

  defp detach_cluster_invalidation_handler do
    case :telemetry.detach(@cluster_invalidation_handler_id) do
      :ok -> :ok
      {:error, :not_found} -> :ok
    end
  end

  defp maybe_subscribe_cache_fanout do
    case Application.get_env(:mezzanine_config_registry, :cluster_invalidation_publisher) do
      {:phoenix_pubsub, pubsub_name} ->
        Phoenix.PubSub.subscribe(pubsub_name, ClusterInvalidation.cache_fanout_topic())

      _other ->
        :ok
    end
  end

  defp evict_cache_for_invalidation(
         %ClusterInvalidation{topic: "memory.policy." <> _rest, metadata: metadata},
         table
       ) do
    case invalidated_installation_ref(metadata) do
      {:installation, installation_ref} -> :ets.delete(table, {:installation, installation_ref})
      :all -> :ets.delete_all_objects(table)
    end

    :ok
  end

  defp evict_cache_for_invalidation(%ClusterInvalidation{}, _table), do: :ok

  defp invalidated_installation_ref(metadata) do
    case Map.get(metadata, "installation_ref") || Map.get(metadata, :installation_ref) do
      nil -> :all
      "" -> :all
      "installation://global" -> :all
      installation_ref when is_binary(installation_ref) -> {:installation, installation_ref}
      _other -> :all
    end
  end

  defp load_all_active_installations(table) do
    {:ok, installations} = Installation.active_installations()

    Enum.each(installations, fn installation ->
      case load_installation(installation.id, installation.compiled_pack_revision, table) do
        {:ok, _compiled_pack} ->
          :ok

        {:error, reason} ->
          :ets.delete(table, {:installation, installation.id})

          Logger.warning(
            "skipping invalid active pack installation #{installation.id}: #{inspect(reason)}"
          )
      end
    end)

    :ok
  end

  defp load_installation(installation_id, expected_revision, table) do
    with {:ok, installation} <- Ash.get(Installation, installation_id),
         {:ok, installation} <- ensure_active_installation(installation),
         :ok <- ensure_expected_revision(installation, expected_revision),
         {:ok, installation} <- Ash.load(installation, [:pack_registration]),
         {:ok, compiled_pack} <-
           Serializer.deserialize_compiled(installation.pack_registration.compiled_manifest) do
      :ets.insert(
        table,
        {{:installation, installation.id},
         %{revision: installation.compiled_pack_revision, compiled: compiled_pack}}
      )

      {:ok, compiled_pack}
    else
      {:error, :inactive} = inactive_error ->
        :ets.delete(table, {:installation, installation_id})
        inactive_error

      {:error, _reason} = error ->
        error
    end
  end

  defp ensure_active_installation(%Installation{status: :active} = installation),
    do: {:ok, installation}

  defp ensure_active_installation(%Installation{}), do: {:error, :inactive}

  defp ensure_expected_revision(_installation, nil), do: :ok

  defp ensure_expected_revision(%Installation{} = installation, expected_revision) do
    if installation.compiled_pack_revision == expected_revision do
      :ok
    else
      {:error, :stale_revision}
    end
  end
end
