defmodule Mezzanine.Execution.Store.Memory do
  @moduledoc "Deterministic execution adapter compiled only for tests."
  @behaviour Mezzanine.Execution.Store

  alias Mezzanine.Persistence.MemoryStore

  @namespace :mezzanine_execution_store

  @impl true
  def capabilities do
    Mezzanine.Persistence.capability!(
      store_ref: :execution,
      tier: :memory_ephemeral,
      data_classes: [:execution],
      adapter: :memory,
      restart_safe?: false
    )
  end

  @impl true
  def resource_modules, do: []

  @impl true
  def preflight(_opts), do: :ok

  def put_record(attrs, opts), do: MemoryStore.put(@namespace, attrs, opts)

  def fetch_record(id, _opts), do: MemoryStore.fetch(@namespace, id)

  def update_record(id, attrs, _opts), do: MemoryStore.update(@namespace, id, attrs)

  def append_event(id, event, _opts), do: MemoryStore.append_event(@namespace, id, event)

  @impl true
  def health(_opts), do: {:ok, %{adapter: :memory, tier: :memory_ephemeral}}

  def reset!, do: MemoryStore.reset!(@namespace)
end
