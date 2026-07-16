defmodule Mezzanine.Archival.Store.AshPostgres do
  @moduledoc "Durable archival owner profile backed by AshPostgres."
  @behaviour Mezzanine.Archival.Store

  alias Ecto.Adapters.SQL

  @migration_version 20_260_416_103_000

  def capabilities, do: Mezzanine.Persistence.postgres_capability(:archival, [:archival])

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

  def repo, do: Mezzanine.Archival.Repo
  def resource_modules, do: [Mezzanine.Archival.ArchivalManifest]

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

  defp configured_repo(opts) when is_list(opts), do: Keyword.get(opts, :repo, repo())
  defp configured_repo(opts) when is_map(opts), do: Map.get(opts, :repo, repo())
end
