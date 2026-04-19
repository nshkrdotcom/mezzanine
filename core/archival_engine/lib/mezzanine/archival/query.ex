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

  @spec archived_trace_sources_by_pivot(String.t(), atom(), String.t(), keyword()) ::
          {:ok, %{manifest: ArchivalManifest.t(), sources: map(), trace_id: String.t()}}
          | {:error, :not_found | :unsupported_pivot | term()}
  def archived_trace_sources_by_pivot(installation_id, pivot, value, opts \\ [])
      when is_binary(installation_id) and is_atom(pivot) and is_binary(value) do
    with {:ok, manifest, trace_id} <-
           archived_manifest_by_pivot(installation_id, pivot, value, opts),
         {:ok, bundle} <- fetch_bundle(manifest, opts),
         trace_id <- trace_id || trace_id_for_pivot(bundle, pivot, value),
         true <- is_binary(trace_id),
         sources <- trace_sources_from_bundle(bundle, trace_id),
         true <- has_trace_sources?(sources) do
      {:ok, %{manifest: manifest, sources: sources, trace_id: trace_id}}
    else
      false -> {:error, :not_found}
      nil -> {:error, :not_found}
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

  defp archived_manifest_by_pivot(installation_id, :trace_id, trace_id, _opts),
    do:
      with(
        {:ok, manifest} <- archived_manifest_by_trace(installation_id, trace_id),
        do: {:ok, manifest, trace_id}
      )

  defp archived_manifest_by_pivot(installation_id, :subject_id, subject_id, _opts) do
    with {:ok, manifest} <- archived_manifest_by_subject(installation_id, subject_id) do
      {:ok, manifest, nil}
    end
  end

  defp archived_manifest_by_pivot(installation_id, :execution_id, execution_id, _opts) do
    archived_manifest_by_array_member(installation_id, "execution_ids", execution_id)
  end

  defp archived_manifest_by_pivot(installation_id, :decision_id, decision_id, _opts) do
    archived_manifest_by_array_member(installation_id, "decision_ids", decision_id)
  end

  defp archived_manifest_by_pivot(installation_id, pivot, value, opts)
       when pivot in [:run_id, :lower_run_id, :attempt_id, :lower_attempt_id, :artifact_id] do
    with {:ok, manifests} <- archived_manifests_for_installation(installation_id) do
      find_manifest_by_bundle_pivot(manifests, pivot, value, opts)
    end
  end

  defp archived_manifest_by_pivot(_installation_id, :manifest_ref, manifest_ref, _opts) do
    with {:ok, manifest} <- ArchivalManifest.by_manifest_ref(manifest_ref) do
      {:ok, manifest, nil}
    end
  end

  defp archived_manifest_by_pivot(_installation_id, _pivot, _value, _opts),
    do: {:error, :unsupported_pivot}

  defp archived_manifest_by_subject(installation_id, subject_id) do
    with {:ok, manifests} <- ArchivalManifest.for_subject(installation_id, subject_id),
         %ArchivalManifest{} = manifest <- Enum.find(manifests, &(&1.status == "archived")) do
      {:ok, manifest}
    else
      nil -> {:error, :not_found}
      {:error, reason} -> {:error, reason}
    end
  end

  defp archived_manifest_by_array_member(installation_id, column, value) do
    sql = """
    SELECT manifest_ref
    FROM archival_manifests
    WHERE installation_id = $1
      AND status = 'archived'
      AND $2::uuid = ANY(#{column})
    ORDER BY terminal_at DESC, inserted_at DESC
    LIMIT 1
    """

    with {:ok, result} <- SQL.query(Repo, sql, [installation_id, dump_uuid!(value)]) do
      archived_manifest_result_for_rows(result.rows, nil)
    end
  end

  defp find_manifest_by_bundle_pivot(manifests, pivot, value, opts) do
    manifests
    |> Enum.find_value(&manifest_for_bundle_pivot(&1, pivot, value, opts))
    |> case do
      nil -> {:error, :not_found}
      result -> result
    end
  end

  defp manifest_for_bundle_pivot(manifest, pivot, value, opts) do
    with {:ok, bundle} <- fetch_bundle(manifest, opts),
         trace_id when is_binary(trace_id) <- trace_id_for_pivot(bundle, pivot, value) do
      {:ok, manifest, trace_id}
    else
      _other -> nil
    end
  end

  defp archived_manifest_result_for_rows([[manifest_ref]], trace_id),
    do: manifest_tuple(manifest_ref, trace_id)

  defp archived_manifest_result_for_rows(_rows, _trace_id), do: {:error, :not_found}

  defp manifest_tuple(manifest_ref, trace_id) do
    case ArchivalManifest.by_manifest_ref(manifest_ref) do
      {:ok, manifest} -> {:ok, manifest, trace_id}
      {:error, reason} -> {:error, reason}
    end
  end

  defp archived_manifests_for_installation(installation_id) do
    sql = """
    SELECT manifest_ref
    FROM archival_manifests
    WHERE installation_id = $1
      AND status = 'archived'
    ORDER BY terminal_at DESC, inserted_at DESC
    """

    with {:ok, result} <- SQL.query(Repo, sql, [installation_id]) do
      result.rows
      |> Enum.map(fn [manifest_ref] -> ArchivalManifest.by_manifest_ref(manifest_ref) end)
      |> Enum.reduce_while({:ok, []}, fn
        {:ok, manifest}, {:ok, acc} -> {:cont, {:ok, [manifest | acc]}}
        {:error, reason}, _acc -> {:halt, {:error, reason}}
      end)
      |> case do
        {:ok, manifests} -> {:ok, Enum.reverse(manifests)}
        {:error, reason} -> {:error, reason}
      end
    end
  end

  defp trace_sources_from_bundle(bundle, trace_id) do
    view = get_in(bundle, ["trace_views", trace_id]) || %{}

    %{
      audit_facts: archived_rows(Map.get(view, "audit_facts", [])),
      executions: archived_rows(Map.get(view, "executions", [])),
      decisions: archived_rows(Map.get(view, "decisions", [])),
      evidence: archived_rows(Map.get(view, "evidence", []))
    }
  end

  defp has_trace_sources?(sources) when is_map(sources) do
    Enum.any?(Map.values(sources), &(is_list(&1) and &1 != []))
  end

  defp trace_id_for_pivot(_bundle, :trace_id, trace_id), do: trace_id

  defp trace_id_for_pivot(bundle, :subject_id, subject_id) do
    if get_in(bundle, ["subject", "id"]) == subject_id do
      first_trace_id(bundle)
    end
  end

  defp trace_id_for_pivot(bundle, :manifest_ref, manifest_ref) do
    if Map.get(bundle, "manifest_ref") == manifest_ref do
      first_trace_id(bundle)
    end
  end

  defp trace_id_for_pivot(bundle, :execution_id, execution_id) do
    bundle
    |> rows_for_family("executions")
    |> Enum.find_value(&row_trace_id(&1, "id", execution_id))
  end

  defp trace_id_for_pivot(bundle, :decision_id, decision_id) do
    bundle
    |> rows_for_family("decisions")
    |> Enum.find_value(&row_trace_id(&1, "id", decision_id))
  end

  defp trace_id_for_pivot(bundle, :run_id, run_id), do: nested_execution_trace_id(bundle, run_id)

  defp trace_id_for_pivot(bundle, :lower_run_id, run_id),
    do: nested_execution_trace_id(bundle, run_id)

  defp trace_id_for_pivot(bundle, :attempt_id, attempt_id),
    do: nested_execution_trace_id(bundle, attempt_id)

  defp trace_id_for_pivot(bundle, :lower_attempt_id, attempt_id),
    do: nested_execution_trace_id(bundle, attempt_id)

  defp trace_id_for_pivot(bundle, :artifact_id, artifact_id),
    do:
      nested_execution_trace_id(bundle, artifact_id) ||
        nested_row_trace_id(bundle, "evidence", artifact_id)

  defp trace_id_for_pivot(_bundle, _pivot, _value), do: nil

  defp first_trace_id(bundle) do
    bundle
    |> Map.get("trace_views", %{})
    |> Map.keys()
    |> Enum.sort()
    |> List.first()
  end

  defp row_trace_id(row, field, value) when is_map(row) do
    if Map.get(row, field) == value, do: Map.get(row, "trace_id")
  end

  defp nested_execution_trace_id(bundle, value) do
    nested_row_trace_id(bundle, "executions", value)
  end

  defp nested_row_trace_id(bundle, family, value) do
    bundle
    |> rows_for_family(family)
    |> Enum.find_value(fn row ->
      if nested_value_present?(row, value), do: Map.get(row, "trace_id")
    end)
  end

  defp rows_for_family(bundle, family) when is_map(bundle) and is_binary(family) do
    top_level_rows = Map.get(bundle, family, [])

    trace_view_rows =
      bundle
      |> Map.get("trace_views", %{})
      |> Map.values()
      |> Enum.flat_map(&Map.get(&1, family, []))

    top_level_rows ++ trace_view_rows
  end

  defp nested_value_present?(%{} = map, value) do
    Enum.any?(map, fn
      {_key, ^value} -> true
      {_key, nested} -> nested_value_present?(nested, value)
    end)
  end

  defp nested_value_present?(list, value) when is_list(list) do
    Enum.any?(list, &nested_value_present?(&1, value))
  end

  defp nested_value_present?(encoded, value) when is_binary(encoded) and is_binary(value) do
    encoded == value or decoded_nested_value_present?(encoded, value)
  end

  defp nested_value_present?(value, value), do: true
  defp nested_value_present?(_other, _value), do: false

  defp decoded_nested_value_present?(encoded, value) do
    case Jason.decode(encoded) do
      {:ok, decoded} -> nested_value_present?(decoded, value)
      {:error, _reason} -> false
    end
  end

  defp bundle_checksum(bundle) when is_map(bundle) do
    BundleChecksum.generate(bundle)
  end

  defp atomize_rows(rows) when is_list(rows), do: Enum.map(rows, &deep_atomize/1)

  defp archived_rows(rows) when is_list(rows) do
    rows
    |> atomize_rows()
    |> Enum.map(&Map.put(&1, :staleness_class, :authoritative_archived))
  end

  defp deep_atomize(%{} = value) do
    value
    |> Enum.map(fn {key, nested} -> {atomize_key(key), deep_atomize(nested)} end)
    |> Map.new()
  end

  defp deep_atomize(value) when is_list(value), do: Enum.map(value, &deep_atomize/1)
  defp deep_atomize(value), do: value

  defp atomize_key(key) when is_atom(key), do: key
  defp atomize_key(key) when is_binary(key), do: String.to_atom(key)

  defp dump_uuid!(value) when is_binary(value), do: Ecto.UUID.dump!(value)
end
