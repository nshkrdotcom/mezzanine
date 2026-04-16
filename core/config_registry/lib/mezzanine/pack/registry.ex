defmodule Mezzanine.Pack.Registry do
  @moduledoc """
  ETS-backed runtime cache for installation-scoped compiled packs.
  """

  use GenServer

  alias Mezzanine.ConfigRegistry.Installation
  alias Mezzanine.Pack.Serializer

  @table :mezzanine_pack_registry

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
    table =
      case :ets.whereis(@table) do
        :undefined -> :ets.new(@table, [:named_table, :protected, :set, read_concurrency: true])
        table -> table
      end

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

  defp load_all_active_installations(table) do
    {:ok, installations} = Installation.active_installations()

    Enum.each(installations, fn installation ->
      {:ok, _compiled_pack} =
        load_installation(installation.id, installation.compiled_pack_revision, table)
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
