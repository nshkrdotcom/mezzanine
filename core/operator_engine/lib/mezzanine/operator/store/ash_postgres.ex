defmodule Mezzanine.Operator.Store.AshPostgres do
  @moduledoc "Adapter-local descriptor for operator Postgres state."
  @behaviour Mezzanine.Operator.Store

  def capabilities, do: Mezzanine.Persistence.postgres_capability(:operator, [:operator])
  def preflight(opts), do: Mezzanine.Persistence.postgres_preflight(:operator, opts)
  def repo, do: Mezzanine.Execution.Repo
  def resource_modules, do: [Mezzanine.OperatorCommands]
  def health(opts), do: with(:ok <- preflight(opts), do: {:ok, %{adapter: :ash_postgres}})
  def put_record(_attrs, opts), do: durable_mutation(:put_record, opts)
  def fetch_record(_id, opts), do: durable_mutation(:fetch_record, opts)
  def update_record(_id, _attrs, opts), do: durable_mutation(:update_record, opts)
  def append_event(_id, _event, opts), do: durable_mutation(:append_event, opts)

  defp durable_mutation(operation, opts) do
    with :ok <- preflight(opts) do
      {:error, {:adapter_local_implementation_required, :operator, operation}}
    end
  end
end
