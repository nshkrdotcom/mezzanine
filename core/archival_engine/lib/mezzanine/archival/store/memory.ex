defmodule Mezzanine.Archival.Store.Memory do
  @moduledoc "Memory-only archival store adapter."
  @behaviour Mezzanine.Archival.Store

  alias Mezzanine.Persistence.MemoryStore

  @namespace :mezzanine_archival_store

  def capabilities, do: Mezzanine.Persistence.memory_capability(:archival, [:archival])
  def preflight(opts), do: Mezzanine.Persistence.preflight(opts, [capabilities()])
  def put_record(attrs, opts), do: MemoryStore.put(@namespace, attrs, opts)
  def fetch_record(id, _opts), do: MemoryStore.fetch(@namespace, id)
  def update_record(id, attrs, _opts), do: MemoryStore.update(@namespace, id, attrs)
  def append_event(id, event, _opts), do: MemoryStore.append_event(@namespace, id, event)
  def health(_opts), do: {:ok, %{adapter: :memory, tier: :memory_ephemeral}}
  def reset!, do: MemoryStore.reset!(@namespace)
end
