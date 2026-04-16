defmodule Mezzanine.WorkAudit do
  @moduledoc """
  Durable audit and evidence services above the persistent evidence domain.
  """

  require Ash.Query

  alias Mezzanine.Audit.{EvidenceManifest, TimelineAssembler}
  alias Mezzanine.Evidence.{AuditEvent, EvidenceBundle, TimelineProjection}

  @spec record_event(String.t(), map()) :: {:ok, struct()} | {:error, term()}
  def record_event(tenant_id, attrs) when is_binary(tenant_id) and is_map(attrs) do
    attrs = Map.put_new(attrs, :occurred_at, DateTime.utc_now())

    AuditEvent
    |> Ash.Changeset.for_create(:record, attrs)
    |> Ash.Changeset.set_tenant(tenant_id)
    |> Ash.create(actor: actor(tenant_id), authorize?: false, domain: Mezzanine.Evidence)
  end

  @spec timeline_for_work(String.t(), Ecto.UUID.t()) :: {:ok, struct()} | {:error, term()}
  def timeline_for_work(tenant_id, work_object_id)
      when is_binary(tenant_id) and is_binary(work_object_id) do
    case projection_for_work(tenant_id, work_object_id) do
      {:ok, projection} -> {:ok, normalize_projection(projection)}
      {:error, :not_found} -> refresh_timeline(tenant_id, work_object_id)
      error -> error
    end
  end

  @spec refresh_timeline(String.t(), Ecto.UUID.t(), DateTime.t()) ::
          {:ok, struct()} | {:error, term()}
  def refresh_timeline(tenant_id, work_object_id, now \\ DateTime.utc_now())
      when is_binary(tenant_id) and is_binary(work_object_id) do
    with {:ok, events} <- list_events_for_work(tenant_id, work_object_id) do
      timeline = TimelineAssembler.project(events)

      last_event_at =
        List.last(timeline)
        |> case do
          nil -> nil
          item -> item.occurred_at
        end

      attrs = %{
        work_object_id: work_object_id,
        timeline: Enum.map(timeline, &Map.from_struct/1),
        last_event_at: last_event_at,
        projected_at: now
      }

      case projection_for_work(tenant_id, work_object_id) do
        {:ok, projection} ->
          projection
          |> Ash.Changeset.for_update(:refresh, Map.delete(attrs, :work_object_id))
          |> Ash.Changeset.set_tenant(tenant_id)
          |> Ash.update(actor: actor(tenant_id), authorize?: false, domain: Mezzanine.Evidence)
          |> normalize_projection_result()

        {:error, :not_found} ->
          TimelineProjection
          |> Ash.Changeset.for_create(:project, attrs)
          |> Ash.Changeset.set_tenant(tenant_id)
          |> Ash.create(actor: actor(tenant_id), authorize?: false, domain: Mezzanine.Evidence)
          |> normalize_projection_result()

        {:error, reason} ->
          {:error, reason}
      end
    end
  end

  @spec assemble_bundle(String.t(), map(), DateTime.t()) :: {:ok, struct()} | {:error, term()}
  def assemble_bundle(tenant_id, attrs, now \\ DateTime.utc_now())
      when is_binary(tenant_id) and is_map(attrs) do
    attrs = Map.new(attrs)

    with work_object_id when is_binary(work_object_id) <- Map.get(attrs, :work_object_id),
         program_id when is_binary(program_id) <- Map.get(attrs, :program_id),
         {:ok, events} <- list_events_for_work(tenant_id, work_object_id),
         manifest <- EvidenceManifest.build(events, []),
         {:ok, bundle} <-
           create_bundle(
             tenant_id,
             %{
               program_id: program_id,
               work_object_id: work_object_id,
               run_id: Map.get(attrs, :run_id),
               summary: Map.get(attrs, :summary, manifest.summary),
               evidence_manifest: manifest.evidence_manifest,
               completeness_status: manifest.completeness_status,
               assembled_at: now
             }
           ) do
      bundle
      |> Ash.Changeset.for_update(:mark_ready, %{
        summary: manifest.summary,
        evidence_manifest: manifest.evidence_manifest,
        completeness_status: manifest.completeness_status,
        assembled_at: now
      })
      |> Ash.Changeset.set_tenant(tenant_id)
      |> Ash.update(actor: actor(tenant_id), authorize?: false, domain: Mezzanine.Evidence)
      |> normalize_bundle_result()
    else
      nil -> {:error, :invalid_bundle_attrs}
      {:error, reason} -> {:error, reason}
      _ -> {:error, :invalid_bundle_attrs}
    end
  end

  @spec work_report(String.t(), Ecto.UUID.t()) :: {:ok, map()} | {:error, term()}
  def work_report(tenant_id, work_object_id)
      when is_binary(tenant_id) and is_binary(work_object_id) do
    with {:ok, projection} <- timeline_for_work(tenant_id, work_object_id),
         {:ok, events} <- list_events_for_work(tenant_id, work_object_id),
         {:ok, bundles} <- list_bundles_for_work(tenant_id, work_object_id) do
      {:ok,
       %{
         work_object_id: work_object_id,
         timeline: projection.timeline,
         audit_events: events,
         evidence_bundles: bundles
       }}
    end
  end

  defp create_bundle(tenant_id, attrs) do
    EvidenceBundle
    |> Ash.Changeset.for_create(:assemble, attrs)
    |> Ash.Changeset.set_tenant(tenant_id)
    |> Ash.create(actor: actor(tenant_id), authorize?: false, domain: Mezzanine.Evidence)
  end

  defp projection_for_work(tenant_id, work_object_id) do
    TimelineProjection
    |> Ash.Query.set_tenant(tenant_id)
    |> Ash.Query.filter(work_object_id == ^work_object_id)
    |> Ash.read(actor: actor(tenant_id), domain: Mezzanine.Evidence)
    |> case do
      {:ok, [projection | _]} -> {:ok, projection}
      {:ok, []} -> {:error, :not_found}
      {:error, error} -> {:error, error}
    end
  end

  defp list_events_for_work(tenant_id, work_object_id) do
    AuditEvent
    |> Ash.Query.set_tenant(tenant_id)
    |> Ash.Query.filter(work_object_id == ^work_object_id)
    |> Ash.read(actor: actor(tenant_id), domain: Mezzanine.Evidence)
    |> case do
      {:ok, events} ->
        {:ok, Enum.sort_by(events, &{&1.occurred_at, &1.id})}

      {:error, error} ->
        {:error, error}
    end
  end

  defp list_bundles_for_work(tenant_id, work_object_id) do
    EvidenceBundle
    |> Ash.Query.set_tenant(tenant_id)
    |> Ash.Query.filter(work_object_id == ^work_object_id)
    |> Ash.read(actor: actor(tenant_id), domain: Mezzanine.Evidence)
  end

  defp actor(tenant_id), do: %{tenant_id: tenant_id}

  defp normalize_projection_result({:ok, projection}), do: {:ok, normalize_projection(projection)}
  defp normalize_projection_result(error), do: error

  defp normalize_bundle_result({:ok, bundle}), do: {:ok, normalize_bundle(bundle)}
  defp normalize_bundle_result(error), do: error

  defp normalize_projection(projection) do
    %{projection | timeline: Enum.map(projection.timeline, &normalize_timeline_row/1)}
  end

  defp normalize_timeline_row(%TimelineAssembler{} = row), do: row

  defp normalize_timeline_row(row) when is_map(row) do
    %TimelineAssembler{
      event_id: row_value(row, :event_id),
      event_kind: row_value(row, :event_kind),
      occurred_at: row_value(row, :occurred_at),
      payload: row_value(row, :payload) || %{},
      actor_kind: row_value(row, :actor_kind),
      actor_ref: row_value(row, :actor_ref),
      run_id: row_value(row, :run_id),
      review_unit_id: row_value(row, :review_unit_id)
    }
  end

  defp normalize_bundle(bundle) do
    %{
      bundle
      | evidence_manifest: normalize_evidence_manifest(bundle.evidence_manifest),
        completeness_status: normalize_completeness_status(bundle.completeness_status)
    }
  end

  defp normalize_evidence_manifest(manifest) when is_map(manifest) do
    %{
      audit_event_count:
        Map.get(manifest, :audit_event_count) || Map.get(manifest, "audit_event_count"),
      evidence_item_count:
        Map.get(manifest, :evidence_item_count) || Map.get(manifest, "evidence_item_count"),
      event_counts: Map.get(manifest, :event_counts) || Map.get(manifest, "event_counts") || %{},
      evidence_item_counts:
        Map.get(manifest, :evidence_item_counts) ||
          Map.get(manifest, "evidence_item_counts") || %{},
      last_event_kind:
        Map.get(manifest, :last_event_kind) || Map.get(manifest, "last_event_kind"),
      last_event_at: Map.get(manifest, :last_event_at) || Map.get(manifest, "last_event_at")
    }
  end

  defp normalize_completeness_status(status) when is_map(status) do
    %{
      audit_events:
        normalize_presence(Map.get(status, :audit_events) || Map.get(status, "audit_events")),
      evidence_items:
        normalize_presence(Map.get(status, :evidence_items) || Map.get(status, "evidence_items")),
      verified_evidence:
        normalize_presence(
          Map.get(status, :verified_evidence) || Map.get(status, "verified_evidence")
        )
    }
  end

  defp normalize_presence(value) when is_atom(value), do: value
  defp normalize_presence(value) when is_binary(value), do: String.to_existing_atom(value)
  defp normalize_presence(value), do: value

  defp row_value(row, key), do: Map.get(row, key) || Map.get(row, Atom.to_string(key))
end
