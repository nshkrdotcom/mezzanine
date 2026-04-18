defmodule Mezzanine.Archival.Query do
  @moduledoc """
  Operator-only archived-query helpers over manifests and cold snapshots.
  """

  alias Ecto.Adapters.SQL
  alias Mezzanine.Archival.{ArchivalManifest, BundleChecksum, ColdStore, Repo}

  @spec archived_subject_manifest(String.t(), Ecto.UUID.t()) ::
          {:ok, ArchivalManifest.t()} | {:error, :not_found | term()}
  def archived_subject_manifest(installation_id, subject_id)
      when is_binary(installation_id) and is_binary(subject_id) do
    with {:ok, manifests} <- ArchivalManifest.for_subject(installation_id, subject_id),
         %ArchivalManifest{} = manifest <-
           Enum.find(manifests, &(&1.status == "archived")) do
      {:ok, manifest}
    else
      nil -> {:error, :not_found}
      {:error, reason} -> {:error, reason}
    end
  end

  @spec archived_trace_sources(String.t(), String.t(), keyword()) ::
          {:ok, %{manifest: ArchivalManifest.t(), sources: map()}} | {:error, :not_found | term()}
  def archived_trace_sources(installation_id, trace_id, opts \\ [])
      when is_binary(installation_id) and is_binary(trace_id) do
    with {:ok, manifest} <- archived_manifest_by_trace(installation_id, trace_id),
         {:ok, bundle} <- fetch_bundle(manifest, opts),
         sources <- trace_sources_from_bundle(bundle, trace_id),
         true <- has_trace_sources?(sources) do
      {:ok, %{manifest: manifest, sources: sources}}
    else
      false -> {:error, :not_found}
      {:error, reason} -> {:error, reason}
    end
  end

  @spec fetch_bundle(ArchivalManifest.t() | String.t(), keyword()) ::
          {:ok, map()} | {:error, term()}
  def fetch_bundle(%ArchivalManifest{} = manifest, opts) do
    with {:ok, bundle} <- ColdStore.read_bundle(manifest.storage_uri, opts),
         checksum when is_binary(checksum) <- bundle_checksum(bundle),
         true <- checksum == manifest.checksum do
      {:ok, bundle}
    else
      false -> {:error, :bundle_checksum_mismatch}
      {:error, reason} -> {:error, reason}
    end
  end

  def fetch_bundle(manifest_ref, opts) when is_binary(manifest_ref) do
    with {:ok, manifest} <- ArchivalManifest.by_manifest_ref(manifest_ref) do
      fetch_bundle(manifest, opts)
    end
  end

  @spec build_manifest_ref(String.t(), Ecto.UUID.t(), DateTime.t()) :: String.t()
  def build_manifest_ref(installation_id, subject_id, %DateTime{} = terminal_at)
      when is_binary(installation_id) and is_binary(subject_id) do
    terminal_at_us = DateTime.to_unix(terminal_at, :microsecond)
    "archive/#{installation_id}/#{subject_id}/#{terminal_at_us}"
  end

  defp archived_manifest_by_trace(installation_id, trace_id) do
    sql = """
    SELECT manifest_ref
    FROM archival_manifests
    WHERE installation_id = $1
      AND status = 'archived'
      AND $2 = ANY(trace_ids)
    ORDER BY terminal_at DESC, inserted_at DESC
    LIMIT 1
    """

    with {:ok, result} <- SQL.query(Repo, sql, [installation_id, trace_id]) do
      case result.rows do
        [[manifest_ref]] -> ArchivalManifest.by_manifest_ref(manifest_ref)
        _ -> {:error, :not_found}
      end
    end
  end

  defp trace_sources_from_bundle(bundle, trace_id) do
    view = get_in(bundle, ["trace_views", trace_id]) || %{}

    %{
      audit_facts: atomize_rows(Map.get(view, "audit_facts", [])),
      executions: atomize_rows(Map.get(view, "executions", [])),
      decisions: atomize_rows(Map.get(view, "decisions", [])),
      evidence: atomize_rows(Map.get(view, "evidence", []))
    }
  end

  defp has_trace_sources?(sources) when is_map(sources) do
    Enum.any?(Map.values(sources), &(is_list(&1) and &1 != []))
  end

  defp bundle_checksum(bundle) when is_map(bundle) do
    BundleChecksum.generate(bundle)
  end

  defp atomize_rows(rows) when is_list(rows), do: Enum.map(rows, &deep_atomize/1)

  defp deep_atomize(%{} = value) do
    value
    |> Enum.map(fn {key, nested} -> {atomize_key(key), deep_atomize(nested)} end)
    |> Map.new()
  end

  defp deep_atomize(value) when is_list(value), do: Enum.map(value, &deep_atomize/1)
  defp deep_atomize(value), do: value

  defp atomize_key(key) when is_atom(key), do: key
  defp atomize_key(key) when is_binary(key), do: String.to_atom(key)
end
