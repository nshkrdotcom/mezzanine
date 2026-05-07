defmodule Mezzanine.Execution.Store.Memory do
  @moduledoc "Memory-only execution store adapter."
  @behaviour Mezzanine.Execution.Store

  alias Mezzanine.Persistence.MemoryStore

  @namespace :mezzanine_execution_store

  @impl true
  def capabilities, do: Mezzanine.Persistence.memory_capability(:execution, [:execution])

  @impl true
  def preflight(opts), do: Mezzanine.Persistence.preflight(opts, [capabilities()])

  @impl true
  def put_record(attrs, opts), do: MemoryStore.put(@namespace, attrs, opts)

  @impl true
  def fetch_record(id, _opts), do: MemoryStore.fetch(@namespace, id)

  @impl true
  def update_record(id, attrs, _opts), do: MemoryStore.update(@namespace, id, attrs)

  @impl true
  def append_event(id, event, _opts), do: MemoryStore.append_event(@namespace, id, event)

  @impl true
  def health(_opts), do: {:ok, %{adapter: :memory, tier: :memory_ephemeral}}

  def reset!, do: MemoryStore.reset!(@namespace)
end
