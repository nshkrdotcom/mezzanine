defmodule Mezzanine.Persistence.MemoryStore do
  @moduledoc """
  VM-local record store for memory-only Mezzanine persistence adapters.

  Records are intentionally process-lifetime state. They carry no restart
  durability claim and use no external substrate.
  """

  use GenServer

  @type namespace :: term()
  @type record_id :: term()
  @type records :: %{optional(record_id()) => map()}
  @type state :: %{optional(namespace()) => records()}

  @spec start_link(keyword()) :: GenServer.on_start()
  def start_link(opts \\ []) do
    GenServer.start_link(__MODULE__, opts, name: __MODULE__)
  end

  @spec put(term(), map(), keyword()) :: {:ok, map()}
  def put(namespace, attrs, opts \\ []) when is_map(attrs) and is_list(opts) do
    id = record_id(namespace, attrs, opts)
    attrs = normalize_keys(attrs)

    GenServer.call(__MODULE__, {:put, namespace, id, attrs})
  end

  @spec fetch(term(), term()) :: {:ok, map()} | {:error, :not_found}
  def fetch(namespace, id) do
    GenServer.call(__MODULE__, {:fetch, namespace, id})
  end

  @spec update(term(), term(), map()) :: {:ok, map()} | {:error, :not_found}
  def update(namespace, id, attrs) when is_map(attrs) do
    GenServer.call(__MODULE__, {:update, namespace, id, normalize_keys(attrs)})
  end

  @spec append_event(term(), term(), map()) :: {:ok, map()} | {:error, :not_found}
  def append_event(namespace, id, event) when is_map(event) do
    GenServer.call(__MODULE__, {:append_event, namespace, id, normalize_keys(event)})
  end

  @spec all(term()) :: [map()]
  def all(namespace), do: GenServer.call(__MODULE__, {:all, namespace})

  @spec reset!(term()) :: :ok
  def reset!(namespace), do: GenServer.call(__MODULE__, {:reset, namespace})

  @impl true
  def init(_opts), do: {:ok, %{}}

  @impl true
  def handle_call({:put, namespace, id, attrs}, _from, state) do
    record = attrs |> Map.put(:id, id) |> Map.put_new(:events, [])
    rows = state |> records(namespace) |> Map.put(id, record)

    {:reply, {:ok, record}, Map.put(state, namespace, rows)}
  end

  def handle_call({:fetch, namespace, id}, _from, state) do
    reply =
      case Map.fetch(records(state, namespace), id) do
        {:ok, record} -> {:ok, record}
        :error -> {:error, :not_found}
      end

    {:reply, reply, state}
  end

  def handle_call({:update, namespace, id, attrs}, _from, state) do
    rows = records(state, namespace)

    case Map.fetch(rows, id) do
      {:ok, record} ->
        updated = record |> Map.merge(attrs) |> Map.put(:id, id)
        {:reply, {:ok, updated}, Map.put(state, namespace, Map.put(rows, id, updated))}

      :error ->
        {:reply, {:error, :not_found}, state}
    end
  end

  def handle_call({:append_event, namespace, id, event}, _from, state) do
    rows = records(state, namespace)

    case Map.fetch(rows, id) do
      {:ok, record} ->
        events = Map.get(record, :events, [])
        sequence = length(events) + 1
        event = event |> Map.put(:sequence, sequence) |> Map.put_new(:record_id, id)
        updated = Map.put(record, :events, events ++ [event])

        {:reply, {:ok, event}, Map.put(state, namespace, Map.put(rows, id, updated))}

      :error ->
        {:reply, {:error, :not_found}, state}
    end
  end

  def handle_call({:all, namespace}, _from, state) do
    {:reply, state |> records(namespace) |> Map.values(), state}
  end

  def handle_call({:reset, namespace}, _from, state) do
    {:reply, :ok, Map.put(state, namespace, %{})}
  end

  defp records(state, namespace), do: Map.get(state, namespace, %{})

  defp record_id(namespace, attrs, opts) do
    Map.get(attrs, :id) ||
      Map.get(attrs, "id") ||
      Keyword.get(opts, :id) ||
      "#{inspect(namespace)}:#{System.unique_integer([:positive, :monotonic])}"
  end

  defp normalize_keys(attrs) do
    Map.new(attrs, fn
      {key, value} when is_binary(key) -> {string_key(key), value}
      {key, value} -> {key, value}
    end)
  end

  defp string_key("events"), do: :events
  defp string_key("id"), do: :id
  defp string_key("record_id"), do: :record_id
  defp string_key("sequence"), do: :sequence
  defp string_key(key), do: key
end
