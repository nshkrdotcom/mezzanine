defmodule Mezzanine.Persistence.MemoryStore do
  @moduledoc """
  VM-local record store for memory-only Mezzanine persistence adapters.

  Records are intentionally process-lifetime state. They carry no restart
  durability claim and use no external substrate.
  """

  @spec put(term(), map(), keyword()) :: {:ok, map()}
  def put(namespace, attrs, opts \\ []) when is_map(attrs) and is_list(opts) do
    id = record_id(namespace, attrs, opts)
    record = attrs |> normalize_keys() |> Map.put(:id, id) |> Map.put_new(:events, [])
    put_records(namespace, Map.put(records(namespace), id, record))
    {:ok, record}
  end

  @spec fetch(term(), term()) :: {:ok, map()} | {:error, :not_found}
  def fetch(namespace, id) do
    case Map.fetch(records(namespace), id) do
      {:ok, record} -> {:ok, record}
      :error -> {:error, :not_found}
    end
  end

  @spec update(term(), term(), map()) :: {:ok, map()} | {:error, :not_found}
  def update(namespace, id, attrs) when is_map(attrs) do
    case fetch(namespace, id) do
      {:ok, record} ->
        updated =
          record
          |> Map.merge(normalize_keys(attrs))
          |> Map.put(:id, id)

        put_records(namespace, Map.put(records(namespace), id, updated))
        {:ok, updated}

      {:error, :not_found} ->
        {:error, :not_found}
    end
  end

  @spec append_event(term(), term(), map()) :: {:ok, map()} | {:error, :not_found}
  def append_event(namespace, id, event) when is_map(event) do
    case fetch(namespace, id) do
      {:ok, record} ->
        events = Map.get(record, :events, [])
        sequence = length(events) + 1

        event =
          event
          |> normalize_keys()
          |> Map.put(:sequence, sequence)
          |> Map.put_new(:record_id, id)

        updated = Map.put(record, :events, events ++ [event])
        put_records(namespace, Map.put(records(namespace), id, updated))
        {:ok, event}

      {:error, :not_found} ->
        {:error, :not_found}
    end
  end

  @spec all(term()) :: [map()]
  def all(namespace), do: namespace |> records() |> Map.values()

  @spec reset!(term()) :: :ok
  def reset!(namespace) do
    put_records(namespace, %{})
    :ok
  end

  defp records(namespace), do: :persistent_term.get(storage_key(namespace), %{})
  defp put_records(namespace, rows), do: :persistent_term.put(storage_key(namespace), rows)
  defp storage_key(namespace), do: {__MODULE__, namespace}

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
