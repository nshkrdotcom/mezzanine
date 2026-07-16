defmodule Mezzanine.Decisions.Store.Memory do
  @moduledoc "Deterministic decision adapter compiled only for tests."
  @behaviour Mezzanine.Decisions.Store

  alias Mezzanine.Persistence.MemoryStore

  @namespace :mezzanine_decisions_store

  def capabilities do
    Mezzanine.Persistence.capability!(
      store_ref: :decisions,
      tier: :memory_ephemeral,
      data_classes: [:decisions],
      adapter: :memory,
      restart_safe?: false
    )
  end
  def resource_modules, do: []
  def preflight(_opts), do: :ok
  def put_record(attrs, opts), do: MemoryStore.put(@namespace, attrs, opts)
  def fetch_record(id, _opts), do: MemoryStore.fetch(@namespace, id)
  def update_record(id, attrs, _opts), do: MemoryStore.update(@namespace, id, attrs)
  def append_event(id, event, _opts), do: MemoryStore.append_event(@namespace, id, event)
  def health(_opts), do: {:ok, %{adapter: :memory, tier: :memory_ephemeral}}
  def reset!, do: MemoryStore.reset!(@namespace)
end
