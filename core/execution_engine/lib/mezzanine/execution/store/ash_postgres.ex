defmodule Mezzanine.Execution.Store.AshPostgres do
  @moduledoc "Adapter-local descriptor for execution AshPostgres state."
  @behaviour Mezzanine.Execution.Store

  @impl true
  def capabilities, do: Mezzanine.Persistence.postgres_capability(:execution, [:execution])

  @impl true
  def preflight(opts), do: Mezzanine.Persistence.postgres_preflight(:execution, opts)

  @impl true
  def put_record(_attrs, opts), do: durable_mutation(:put_record, opts)

  @impl true
  def fetch_record(_id, opts), do: durable_mutation(:fetch_record, opts)

  @impl true
  def update_record(_id, _attrs, opts), do: durable_mutation(:update_record, opts)

  @impl true
  def append_event(_id, _event, opts), do: durable_mutation(:append_event, opts)

  @impl true
  def health(opts) do
    with :ok <- preflight(opts) do
      {:ok, %{adapter: :ash_postgres, tier: :postgres_shared, repo: repo()}}
    end
  end

  def repo, do: Mezzanine.Execution.Repo
  def resource_modules, do: [Mezzanine.Execution.ExecutionRecord]

  defp durable_mutation(operation, opts) do
    with :ok <- preflight(opts) do
      {:error, {:adapter_local_implementation_required, :execution, operation}}
    end
  end
end
