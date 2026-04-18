defmodule Mezzanine.Archival.Scheduler do
  @moduledoc false

  use GenServer

  alias Ash.Changeset
  alias Ecto.Adapters.SQL
  alias Mezzanine.Archival.{ArchivalManifest, ColdStore, Repo, Snapshot}
  alias Mezzanine.Telemetry

  @default_retention_days 30

  @spec start_link(keyword()) :: GenServer.on_start()
  def start_link(opts \\ []) do
    GenServer.start_link(__MODULE__, opts, name: __MODULE__)
  end

  @spec run_once(keyword()) :: {:ok, [map()]} | {:error, term()}
  def run_once(opts \\ []) do
    now = Keyword.get(opts, :now, DateTime.utc_now())

    installations()
    |> Enum.map(&run_installation(&1.id, Keyword.put(opts, :now, now)))
    |> then(&{:ok, &1})
  end

  @spec run_installation(String.t(), keyword()) :: map()
  def run_installation(installation_id, opts \\ []) when is_binary(installation_id) do
    now = Keyword.get(opts, :now, DateTime.utc_now())
    installation = installation!(installation_id)
    eligible_subject_ids = eligible_subject_ids(installation, now)

    %{
      installation_id: installation_id,
      subject_count: length(eligible_subject_ids),
      archived:
        Enum.map(eligible_subject_ids, fn subject_id ->
          archive_subject(installation_id, subject_id, opts)
        end)
    }
  end

  @spec archive_subject(String.t(), Ecto.UUID.t(), keyword()) :: {:ok, map()} | {:error, term()}
  def archive_subject(installation_id, subject_id, opts \\ [])
      when is_binary(installation_id) and is_binary(subject_id) do
    now = Keyword.get(opts, :now, DateTime.utc_now())
    installation = installation!(installation_id)

    with {:ok, snapshot} <- Snapshot.build(installation_id, subject_id, now: now),
         :ok <- ensure_due(snapshot, installation, now) do
      archive_from_snapshot(snapshot, installation, now, opts)
    end
  end

  @impl true
  def init(opts) do
    state = %{
      enabled?: opts[:enabled?] || scheduler_config(:enabled?, false),
      interval_ms: opts[:interval_ms] || scheduler_config(:interval_ms, :timer.minutes(5))
    }

    if state.enabled?, do: schedule_next(state.interval_ms)
    {:ok, state}
  end

  @impl true
  def handle_info(:run, state) do
    _ = run_once()
    schedule_next(state.interval_ms)
    {:noreply, state}
  end

  defp archive_from_snapshot(snapshot, installation, now, opts) do
    existing_manifest = latest_manifest(snapshot.installation_id, snapshot.subject_id)
    trace_id = List.first(snapshot.trace_ids)

    case existing_manifest && existing_manifest.status do
      "archived" ->
        {:ok, %{manifest_ref: existing_manifest.manifest_ref, status: :already_archived}}

      "verified" ->
        finalize_verified_manifest(existing_manifest, snapshot, installation, trace_id, now)

      _other ->
        with {:ok, manifest} <- stage_manifest(snapshot, installation, now),
             :ok <-
               emit_archival_event([:archival, :run], snapshot, installation, trace_id, %{
                 count: 1
               }),
             {:ok, cold_result} <-
               ColdStore.write_bundle(manifest.manifest_ref, snapshot.bundle, opts),
             {:ok, verified_manifest} <-
               ArchivalManifest.mark_verified(manifest, %{
                 storage_uri: cold_result.storage_uri,
                 checksum: cold_result.checksum,
                 verified_at: now,
                 metadata:
                   Map.merge(manifest.metadata || %{}, %{
                     "cold_store_checksum" => cold_result.checksum,
                     "cold_store_uri" => cold_result.storage_uri
                   })
               }),
             :ok <-
               emit_archival_event(
                 [:archival, :verified],
                 snapshot,
                 installation,
                 trace_id,
                 %{count: 1}
               ),
             {:ok, finalized} <-
               finalize_verified_manifest(
                 verified_manifest,
                 snapshot,
                 installation,
                 trace_id,
                 now
               ) do
          {:ok, finalized}
        else
          {:error, reason} ->
            fail_manifest(
              latest_manifest(snapshot.installation_id, snapshot.subject_id) || existing_manifest,
              reason
            )

            emit_failure(snapshot, installation, trace_id, reason)
            {:error, reason}
        end
    end
  end

  defp finalize_verified_manifest(manifest, snapshot, installation, trace_id, now) do
    case Repo.transaction(fn ->
           removed = Snapshot.delete_hot_rows!(snapshot)
           finalize_archival_transaction(manifest, removed, now)
         end) do
      {:ok, {archived_manifest, removed, notifications}} ->
        finalize_archival_success(
          archived_manifest,
          removed,
          notifications,
          snapshot,
          installation,
          trace_id
        )

      {:error, reason} ->
        fail_manifest(manifest, reason)
        emit_failure(snapshot, installation, trace_id, reason)
        {:error, reason}
    end
  end

  defp finalize_archival_transaction(manifest, removed, now) do
    case mark_archived_with_notifications(manifest, now) do
      {:ok, archived_manifest, notifications} ->
        {archived_manifest, removed, notifications}

      {:error, reason} ->
        Repo.rollback(reason)
    end
  end

  defp finalize_archival_success(
         archived_manifest,
         removed,
         notifications,
         snapshot,
         installation,
         trace_id
       ) do
    Ash.Notifier.notify(notifications)

    total_removed =
      Enum.reduce(removed, 0, fn {_key, count}, acc -> acc + count end)

    emit_archival_event(
      [:archival, :rows_removed],
      snapshot,
      installation,
      trace_id,
      %{count: total_removed}
    )

    {:ok,
     %{
       manifest_ref: archived_manifest.manifest_ref,
       status: :archived,
       removed: removed,
       trace_id: trace_id
     }}
  end

  defp mark_archived_with_notifications(manifest, now) do
    manifest
    |> Changeset.for_update(:mark_archived, %{archived_at: now})
    |> Ash.update(
      authorize?: false,
      domain: Mezzanine.Archival,
      return_notifications?: true
    )
  end

  defp latest_manifest(installation_id, subject_id) do
    case ArchivalManifest.for_subject(installation_id, subject_id) do
      {:ok, [manifest | _rest]} -> manifest
      _ -> nil
    end
  end

  defp stage_manifest(snapshot, installation, now) do
    due_at = DateTime.add(snapshot.terminal_at, installation.retention_seconds, :second)

    ArchivalManifest.stage(%{
      manifest_ref: snapshot.manifest_ref,
      installation_id: snapshot.installation_id,
      subject_id: snapshot.subject_id,
      subject_state: snapshot.subject_state,
      execution_states: snapshot.execution_states,
      trace_ids: snapshot.trace_ids,
      execution_ids: snapshot.execution_ids,
      decision_ids: snapshot.decision_ids,
      evidence_ids: snapshot.evidence_ids,
      audit_fact_ids: snapshot.audit_fact_ids,
      projection_names: snapshot.projection_names,
      terminal_at: snapshot.terminal_at,
      due_at: due_at,
      retention_seconds: installation.retention_seconds,
      storage_kind: "filesystem",
      metadata: %{
        "captured_at" => DateTime.to_iso8601(now),
        "tenant_id" => installation.tenant_id
      }
    })
  end

  defp ensure_due(snapshot, installation, now) do
    due_at = DateTime.add(snapshot.terminal_at, installation.retention_seconds, :second)

    if DateTime.compare(now, due_at) in [:eq, :gt] do
      :ok
    else
      {:error, :not_due}
    end
  end

  defp installation!(installation_id) do
    sql = """
    SELECT id::text, tenant_id, metadata
    FROM installations
    WHERE id = $1::uuid
    LIMIT 1
    """

    case SQL.query(Repo, sql, [dump_uuid!(installation_id)]) do
      {:ok, %{rows: [[id, tenant_id, metadata]]}} ->
        retention_days =
          metadata
          |> hot_retention_days()

        %{
          id: id,
          tenant_id: tenant_id,
          metadata: metadata || %{},
          retention_days: retention_days,
          retention_seconds: retention_days * 86_400
        }

      {:ok, _result} ->
        %{
          id: installation_id,
          tenant_id: nil,
          metadata: %{},
          retention_days: @default_retention_days,
          retention_seconds: @default_retention_days * 86_400
        }

      {:error, error} ->
        raise error
    end
  end

  defp installations do
    sql = "SELECT id::text FROM installations ORDER BY inserted_at ASC, id ASC"

    case SQL.query(Repo, sql, []) do
      {:ok, %{rows: rows}} -> Enum.map(rows, fn [id] -> %{id: id} end)
      {:error, error} -> raise error
    end
  end

  defp eligible_subject_ids(installation, now) do
    cutoff = DateTime.add(now, -installation.retention_seconds, :second)

    sql = """
    SELECT id::text
    FROM subject_records
    WHERE installation_id = $1
      AND terminal_at IS NOT NULL
      AND terminal_at <= $2
    ORDER BY terminal_at ASC, id ASC
    """

    case SQL.query(Repo, sql, [installation.id, cutoff]) do
      {:ok, %{rows: rows}} -> Enum.map(rows, fn [id] -> id end)
      {:error, error} -> raise error
    end
  end

  defp emit_archival_event(event, snapshot, installation, trace_id, measurements) do
    Telemetry.emit(
      event,
      measurements,
      %{
        trace_id: trace_id,
        subject_id: snapshot.subject_id,
        installation_id: snapshot.installation_id,
        tenant_id: installation.tenant_id,
        manifest_ref: snapshot.manifest_ref
      }
    )

    :ok
  end

  defp emit_failure(snapshot, installation, trace_id, reason) do
    _ =
      emit_archival_event(
        [:archival, :failed],
        snapshot,
        installation,
        trace_id,
        %{count: 1}
      )

    {:error, reason}
  end

  defp fail_manifest(nil, _reason), do: :ok

  defp fail_manifest(manifest, reason) do
    _ =
      ArchivalManifest.mark_failed(manifest, %{
        reason: inspect(reason),
        metadata: Map.merge(manifest.metadata || %{}, %{"failure" => inspect(reason)})
      })

    :ok
  end

  defp hot_retention_days(metadata) when is_map(metadata) do
    case Map.get(metadata, "hot_retention_days") || Map.get(metadata, :hot_retention_days) do
      value when is_integer(value) and value > 0 -> value
      _ -> @default_retention_days
    end
  end

  defp hot_retention_days(_metadata), do: @default_retention_days

  defp schedule_next(interval_ms) when is_integer(interval_ms) and interval_ms > 0 do
    Process.send_after(self(), :run, interval_ms)
  end

  defp scheduler_config(key, default) do
    Application.fetch_env!(:mezzanine_archival_engine, :scheduler)
    |> Keyword.get(key, default)
  end

  defp dump_uuid!(value) when is_binary(value), do: Ecto.UUID.dump!(value)
end
