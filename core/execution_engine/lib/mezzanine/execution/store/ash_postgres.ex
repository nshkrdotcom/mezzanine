defmodule Mezzanine.Execution.Store.AshPostgres do
  @moduledoc "Durable execution owner profile backed by AshPostgres."
  @behaviour Mezzanine.Execution.Store

  alias Ecto.Adapters.SQL

  @migration_version 20_260_428_114_100

  @impl true
  def capabilities, do: Mezzanine.Persistence.postgres_capability(:execution, [:execution])

  @impl true
  def preflight(opts) do
    selected_repo = configured_repo(opts)

    with {:ok, %{rows: [[1]]}} <- SQL.query(selected_repo, "SELECT 1", []),
         {:ok, %{rows: [[true]]}} <-
           SQL.query(
             selected_repo,
             "SELECT EXISTS(SELECT 1 FROM schema_migrations WHERE version = $1)",
             [@migration_version]
           ) do
      :ok
    else
      {:ok, %{rows: [[false]]}} -> {:error, {:required_migration_missing, @migration_version}}
      {:error, reason} -> {:error, {:postgres_unavailable, reason}}
      other -> {:error, {:postgres_preflight_failed, other}}
    end
  end

  @impl true
  def health(opts) do
    with :ok <- preflight(opts) do
      {:ok,
       %{
         adapter: :ash_postgres,
         capability: capabilities(),
         migration_version: @migration_version,
         repo: configured_repo(opts),
         restart_safe?: true,
         tier: :postgres_shared
       }}
    end
  end

  def repo, do: Mezzanine.Execution.Repo
  @impl true
  def resource_modules, do: [Mezzanine.Execution.ExecutionRecord]

  defp configured_repo(opts) when is_list(opts), do: Keyword.get(opts, :repo, repo())
  defp configured_repo(opts) when is_map(opts), do: Map.get(opts, :repo, repo())
end
