defmodule Mezzanine.Objects.Store do
  @moduledoc "Persistence facade for object lifecycle state."

  alias Mezzanine.Objects.Store.AshPostgres
  alias Mezzanine.Objects.Store.Memory

  @callback capabilities() :: Mezzanine.Persistence.store_capability()
  @callback preflight(keyword() | map()) :: :ok | {:error, term()}
  @callback put_record(map(), keyword()) :: {:ok, map()} | {:error, term()}
  @callback fetch_record(term(), keyword()) :: {:ok, map()} | {:error, term()}
  @callback update_record(term(), map(), keyword()) :: {:ok, map()} | {:error, term()}
  @callback append_event(term(), map(), keyword()) :: {:ok, map()} | {:error, term()}
  @callback health(keyword()) :: {:ok, map()} | {:error, term()}

  def adapter(opts \\ []), do: Mezzanine.Persistence.adapter_for(opts, Memory, AshPostgres)

  def capabilities, do: Memory.capabilities()
  def preflight(opts \\ []), do: adapter(opts).preflight(opts)
  def put_record(attrs, opts \\ []), do: adapter(opts).put_record(attrs, opts)
  def fetch_record(id, opts \\ []), do: adapter(opts).fetch_record(id, opts)
  def update_record(id, attrs, opts \\ []), do: adapter(opts).update_record(id, attrs, opts)
  def append_event(id, event, opts \\ []), do: adapter(opts).append_event(id, event, opts)
  def health(opts \\ []), do: adapter(opts).health(opts)
end
