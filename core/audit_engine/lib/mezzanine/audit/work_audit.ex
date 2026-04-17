defmodule Mezzanine.Audit.WorkAudit do
  @moduledoc """
  Neutral audit timeline and evidence-bundle services for bounded northbound
  consumers.
  """

  require Ash.Query

  alias Mezzanine.Evidence.{AuditEvent, EvidenceBundle, TimelineProjection}

  @type timeline_row :: %{
          event_id: String.t(),
          event_kind: String.t(),
          occurred_at: DateTime.t(),
          payload: map(),
          actor_kind: String.t() | nil,
          actor_ref: String.t() | nil,
          run_id: String.t() | nil,
          review_unit_id: String.t() | nil
        }

  @type audit_event_record :: struct()
  @type evidence_bundle_record :: struct()
  @type timeline_projection :: struct()

  @type report :: %{
          work_object_id: Ecto.UUID.t(),
          timeline: [timeline_row()],
          audit_events: [audit_event_record()],
          evidence_bundles: [evidence_bundle_record()]
        }

  @spec record_event(String.t(), map()) :: {:ok, audit_event_record()} | {:error, term()}
  def record_event(tenant_id, attrs) when is_binary(tenant_id) and is_map(attrs) do
    attrs = Map.put_new(attrs, :occurred_at, DateTime.utc_now())

    AuditEvent
    |> Ash.Changeset.for_create(:record, attrs)
    |> Ash.Changeset.set_tenant(tenant_id)
    |> Ash.create(actor: actor(tenant_id), authorize?: false, domain: Mezzanine.Evidence)
  end

  @spec timeline_for_work(String.t(), Ecto.UUID.t()) ::
          {:ok, timeline_projection()} | {:error, term()}
  def timeline_for_work(tenant_id, work_object_id)
      when is_binary(tenant_id) and is_binary(work_object_id) do
    case projection_for_work(tenant_id, work_object_id) do
      {:ok, projection} -> {:ok, normalize_projection(projection)}
      {:error, :not_found} -> refresh_timeline(tenant_id, work_object_id)
      error -> error
    end
  end

  @spec refresh_timeline(String.t(), Ecto.UUID.t(), DateTime.t()) ::
          {:ok, timeline_projection()} | {:error, term()}
  def refresh_timeline(tenant_id, work_object_id, now \\ DateTime.utc_now())
      when is_binary(tenant_id) and is_binary(work_object_id) do
    with {:ok, events} <- list_events_for_work(tenant_id, work_object_id) do
      timeline = project_timeline(events)
      last_event_at = timeline |> List.last() |> value_or_nil(:occurred_at)

      attrs = %{
        work_object_id: work_object_id,
        timeline: timeline,
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

  @spec assemble_bundle(String.t(), map(), DateTime.t()) ::
          {:ok, evidence_bundle_record()} | {:error, term()}
  def assemble_bundle(tenant_id, attrs, now \\ DateTime.utc_now())
      when is_binary(tenant_id) and is_map(attrs) do
    attrs = Map.new(attrs)

    with work_object_id when is_binary(work_object_id) <- Map.get(attrs, :work_object_id),
         program_id when is_binary(program_id) <- Map.get(attrs, :program_id),
         {:ok, events} <- list_events_for_work(tenant_id, work_object_id),
         manifest <- build_manifest(events, []),
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
      _other -> {:error, :invalid_bundle_attrs}
    end
  end

  @spec work_report(String.t(), Ecto.UUID.t()) :: {:ok, report()} | {:error, term()}
  def work_report(tenant_id, work_object_id)
      when is_binary(tenant_id) and is_binary(work_object_id) do
    case timeline_for_work(tenant_id, work_object_id) do
      {:ok, projection} ->
        assemble_report(tenant_id, work_object_id, projection)

      {:error, reason} ->
        {:error, reason}
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

  @spec list_bundles_for_work(String.t(), Ecto.UUID.t()) ::
          {:ok, [evidence_bundle_record()]} | {:error, term()}
  defp list_bundles_for_work(tenant_id, work_object_id) do
    EvidenceBundle
    |> Ash.Query.set_tenant(tenant_id)
    |> Ash.Query.filter(work_object_id == ^work_object_id)
    |> Ash.read(actor: actor(tenant_id), domain: Mezzanine.Evidence)
    |> case do
      {:ok, bundles} ->
        {:ok, Enum.map(bundles, &normalize_bundle/1)}

      {:error, error} ->
        {:error, error}
    end
  end

  defp assemble_report(tenant_id, work_object_id, projection) do
    with {:ok, events} <- list_events_for_work(tenant_id, work_object_id),
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

  defp project_timeline(events) when is_list(events) do
    events
    |> Enum.sort_by(&{&1.occurred_at, &1.id})
    |> Enum.map(fn event ->
      %{
        event_id: event.id,
        event_kind: Atom.to_string(event.event_kind),
        occurred_at: event.occurred_at,
        payload: event.payload,
        actor_kind: maybe_stringify(event.actor_kind),
        actor_ref: event.actor_ref,
        run_id: event.run_id,
        review_unit_id: event.review_unit_id
      }
    end)
  end

  defp build_manifest(events, evidence_items) when is_list(events) and is_list(evidence_items) do
    event_counts =
      events
      |> Enum.frequencies_by(&Atom.to_string(&1.event_kind))

    item_counts =
      evidence_items
      |> Enum.frequencies_by(fn item -> "#{item.kind}:#{item.status}" end)

    last_event = List.last(Enum.sort_by(events, &{&1.occurred_at, &1.id}))

    %{
      summary: manifest_summary(last_event, events, evidence_items),
      evidence_manifest: %{
        audit_event_count: length(events),
        evidence_item_count: length(evidence_items),
        event_counts: event_counts,
        evidence_item_counts: item_counts,
        last_event_kind: last_event && Atom.to_string(last_event.event_kind),
        last_event_at: last_event && last_event.occurred_at
      },
      completeness_status: %{
        audit_events: presence(length(events)),
        evidence_items: presence(length(evidence_items)),
        verified_evidence: presence(Enum.count(evidence_items, &match?(%{status: :verified}, &1)))
      }
    }
  end

  defp manifest_summary(nil, _events, evidence_items) do
    "No audit events recorded; #{length(evidence_items)} evidence items attached"
  end

  defp manifest_summary(last_event, events, evidence_items) do
    "#{length(events)} audit events, #{length(evidence_items)} evidence items, last event #{Atom.to_string(last_event.event_kind)}"
  end

  defp presence(0), do: :missing
  defp presence(_count), do: :present

  defp actor(tenant_id), do: %{tenant_id: tenant_id}

  defp normalize_projection_result({:ok, projection}), do: {:ok, normalize_projection(projection)}
  defp normalize_projection_result(error), do: error

  defp normalize_bundle_result({:ok, bundle}), do: {:ok, normalize_bundle(bundle)}
  defp normalize_bundle_result(error), do: error

  defp normalize_projection(projection) do
    %{projection | timeline: Enum.map(projection.timeline, &normalize_timeline_row/1)}
  end

  defp normalize_timeline_row(row) when is_map(row) do
    %{
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

  defp maybe_stringify(nil), do: nil
  defp maybe_stringify(value) when is_atom(value), do: Atom.to_string(value)
  defp maybe_stringify(value), do: to_string(value)

  defp row_value(row, key), do: Map.get(row, key) || Map.get(row, Atom.to_string(key))

  defp value_or_nil(nil, _key), do: nil
  defp value_or_nil(map, key) when is_map(map), do: Map.get(map, key)
end
