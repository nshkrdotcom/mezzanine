defmodule Mezzanine.Archival.Store.AshPostgres do
  @moduledoc "Adapter-local descriptor for archival AshPostgres state."
  @behaviour Mezzanine.Archival.Store

  def capabilities, do: Mezzanine.Persistence.postgres_capability(:archival, [:archival])
  def preflight(opts), do: Mezzanine.Persistence.postgres_preflight(:archival, opts)
  def repo, do: Mezzanine.Archival.Repo
  def resource_modules, do: [Mezzanine.Archival.ArchivalManifest]
  def health(opts), do: with(:ok <- preflight(opts), do: {:ok, %{adapter: :ash_postgres}})
  def put_record(_attrs, opts), do: durable_mutation(:put_record, opts)
  def fetch_record(_id, opts), do: durable_mutation(:fetch_record, opts)
  def update_record(_id, _attrs, opts), do: durable_mutation(:update_record, opts)
  def append_event(_id, _event, opts), do: durable_mutation(:append_event, opts)

  defp durable_mutation(operation, opts) do
    with :ok <- preflight(opts) do
      {:error, {:adapter_local_implementation_required, :archival, operation}}
    end
  end
end
