defmodule Mezzanine.Surfaces.WorkSurface do
  @moduledoc """
  Reusable typed intake, detail, and queue surface for governed work.
  """

  require Ash.Query

  alias Mezzanine.Assurance
  alias Mezzanine.Audit
  alias Mezzanine.Control.ControlSession
  alias Mezzanine.Review.{Escalation, ReviewUnit}
  alias Mezzanine.Runs.{Run, RunSeries}
  alias Mezzanine.Surfaces.{WorkDetail, WorkQueueStats, WorkStatusProjection}
  alias Mezzanine.Work.{WorkObject, WorkPlan}

  @active_statuses [:pending, :planning, :planned, :running, :awaiting_review, :blocked]
  @opaque work_resource :: struct()

  @spec ingest_work(map(), keyword()) :: {:ok, work_resource()} | {:error, term()}
  def ingest_work(attrs, opts \\ []) when is_map(attrs) and is_list(opts) do
    attrs = Map.new(attrs)

    with {:ok, tenant_id} <- fetch_string(attrs, opts, :tenant_id),
         {:ok, program_id} <- fetch_string(attrs, opts, :program_id),
         {:ok, work_class_id} <- fetch_string(attrs, opts, :work_class_id),
         {:ok, external_ref} <- fetch_string(attrs, opts, :external_ref),
         {:ok, work_object} <-
           upsert_work_object(tenant_id, program_id, work_class_id, external_ref, attrs) do
      refresh_plan(work_object, tenant_id, attrs)
    end
  end

  @spec list_active_work(String.t(), Ecto.UUID.t(), map()) ::
          {:ok, [work_resource()]} | {:error, term()}
  def list_active_work(tenant_id, program_id, filters \\ %{})
      when is_binary(tenant_id) and is_binary(program_id) and is_map(filters) do
    with {:ok, work_objects} <-
           WorkObject.list_for_program(program_id, actor: actor(tenant_id), tenant: tenant_id) do
      {:ok, Enum.filter(work_objects, &active_work_object?(&1, filters))}
    end
  end

  @spec get_work_detail(String.t(), Ecto.UUID.t()) :: {:ok, WorkDetail.t()} | {:error, term()}
  def get_work_detail(tenant_id, work_object_id)
      when is_binary(tenant_id) and is_binary(work_object_id) do
    with {:ok, work_object} <- fetch_work_object(tenant_id, work_object_id),
         {:ok, current_plan} <- fetch_current_plan(tenant_id, work_object.current_plan_id),
         {:ok, run_series} <- list_run_series(tenant_id, work_object.id),
         {:ok, active_run} <- fetch_active_run(tenant_id, run_series),
         {:ok, pending_reviews} <- list_pending_reviews_for_work(tenant_id, work_object.id),
         {:ok, control_session} <- fetch_control_session(tenant_id, work_object.id),
         {:ok, audit_report} <- Audit.work_report(tenant_id, work_object.id),
         {:ok, gate_status} <- Assurance.gate_status(tenant_id, work_object.id) do
      {:ok,
       %WorkDetail{
         work_object: work_object,
         current_plan: current_plan,
         active_run: active_run,
         run_series: run_series,
         obligations: obligation_ids(current_plan),
         pending_reviews: pending_reviews,
         evidence_bundle: List.first(Enum.reverse(audit_report.evidence_bundles)),
         control_session: control_session,
         timeline_projection: %{
           work_object_id: work_object.id,
           timeline: audit_report.timeline,
           audit_events: audit_report.audit_events
         },
         gate_status: gate_status
       }}
    end
  end

  @spec work_queue_stats(String.t(), Ecto.UUID.t()) ::
          {:ok, WorkQueueStats.t()} | {:error, term()}
  def work_queue_stats(tenant_id, program_id)
      when is_binary(tenant_id) and is_binary(program_id) do
    with {:ok, work_objects} <- list_active_work(tenant_id, program_id),
         {:ok, open_escalation_count} <- open_escalation_count(tenant_id, work_objects),
         {:ok, stalled_count} <- stalled_count(tenant_id, work_objects) do
      counts_by_status = Enum.frequencies_by(work_objects, & &1.status)

      {:ok,
       %WorkQueueStats{
         program_id: program_id,
         active_count: length(work_objects),
         queued_count:
           Map.get(counts_by_status, :pending, 0) + Map.get(counts_by_status, :planned, 0),
         running_count: Map.get(counts_by_status, :running, 0),
         awaiting_review_count: Map.get(counts_by_status, :awaiting_review, 0),
         blocked_count: Map.get(counts_by_status, :blocked, 0),
         stalled_count: stalled_count,
         open_escalation_count: open_escalation_count,
         counts_by_status: counts_by_status
       }}
    end
  end

  @spec work_status_projection(String.t(), Ecto.UUID.t()) ::
          {:ok, WorkStatusProjection.t()} | {:error, term()}
  def work_status_projection(tenant_id, work_object_id)
      when is_binary(tenant_id) and is_binary(work_object_id) do
    with {:ok, work_object} <- fetch_work_object(tenant_id, work_object_id),
         {:ok, current_plan} <- fetch_current_plan(tenant_id, work_object.current_plan_id),
         {:ok, run_series} <- list_run_series(tenant_id, work_object.id),
         {:ok, active_run} <- fetch_active_run(tenant_id, run_series),
         {:ok, control_session} <- fetch_control_session(tenant_id, work_object.id),
         {:ok, gate_status} <- Assurance.gate_status(tenant_id, work_object.id),
         {:ok, timeline_projection} <- Audit.timeline_for_work(tenant_id, work_object.id) do
      {:ok,
       %WorkStatusProjection{
         work_object_id: work_object.id,
         work_status: work_object.status,
         plan_status: if(current_plan, do: current_plan.status),
         run_status: if(active_run, do: active_run.status),
         control_mode: if(control_session, do: control_session.current_mode),
         review_status: gate_status.status,
         release_ready?: gate_status.release_ready?,
         last_event_at: timeline_projection.last_event_at
       }}
    end
  end

  defp upsert_work_object(tenant_id, program_id, work_class_id, external_ref, attrs) do
    case find_work_object_by_external_ref(tenant_id, program_id, external_ref) do
      {:ok, nil} ->
        WorkObject.ingest(
          intake_attrs(attrs, program_id, work_class_id, external_ref),
          actor: actor(tenant_id),
          tenant: tenant_id
        )

      {:ok, %WorkObject{} = work_object} ->
        WorkObject.refresh_intake(
          work_object,
          refresh_intake_attrs(attrs, work_class_id, external_ref),
          actor: actor(tenant_id),
          tenant: tenant_id
        )

      {:error, reason} ->
        {:error, reason}
    end
  end

  defp find_work_object_by_external_ref(tenant_id, program_id, external_ref) do
    WorkObject
    |> Ash.Query.set_tenant(tenant_id)
    |> Ash.Query.filter(program_id == ^program_id and external_ref == ^external_ref)
    |> Ash.read(actor: actor(tenant_id), domain: Mezzanine.Work)
    |> case do
      {:ok, [work_object | _]} -> {:ok, work_object}
      {:ok, []} -> {:ok, nil}
      {:error, reason} -> {:error, reason}
    end
  end

  defp refresh_plan(work_object, tenant_id, attrs) do
    with {:ok, _prior_plan} <- maybe_supersede_plan(tenant_id, work_object.current_plan_id) do
      compile_attrs =
        case map_value(attrs, :policy_bundle_id) do
          nil -> %{}
          policy_bundle_id -> %{policy_bundle_id: policy_bundle_id}
        end

      WorkObject.compile_plan(work_object, compile_attrs,
        actor: actor(tenant_id),
        tenant: tenant_id
      )
    end
  end

  defp intake_attrs(attrs, program_id, work_class_id, external_ref) do
    %{
      program_id: program_id,
      work_class_id: work_class_id,
      external_ref: external_ref,
      title: map_value(attrs, :title) || external_ref,
      description: map_value(attrs, :description),
      priority: map_value(attrs, :priority) || 50,
      source_kind: map_value(attrs, :source_kind) || "external",
      payload:
        map_value(attrs, :payload) || Map.drop(attrs, [:tenant_id, :program_id, :work_class_id]),
      normalized_payload:
        map_value(attrs, :normalized_payload) ||
          map_value(attrs, :payload) ||
          Map.drop(attrs, [:tenant_id, :program_id, :work_class_id])
    }
  end

  defp refresh_intake_attrs(attrs, work_class_id, external_ref) do
    %{
      work_class_id: work_class_id,
      external_ref: external_ref,
      title: map_value(attrs, :title) || external_ref,
      description: map_value(attrs, :description),
      priority: map_value(attrs, :priority) || 50,
      source_kind: map_value(attrs, :source_kind) || "external",
      payload:
        map_value(attrs, :payload) || Map.drop(attrs, [:tenant_id, :program_id, :work_class_id]),
      normalized_payload:
        map_value(attrs, :normalized_payload) ||
          map_value(attrs, :payload) ||
          Map.drop(attrs, [:tenant_id, :program_id, :work_class_id])
    }
  end

  defp active_work_object?(work_object, filters) do
    work_object.status in @active_statuses and
      match_filter(work_object.status, Map.get(filters, :statuses)) and
      match_filter(work_object.source_kind, map_value(filters, :source_kind)) and
      match_filter(work_object.work_class_id, map_value(filters, :work_class_id))
  end

  defp match_filter(_value, nil), do: true
  defp match_filter(value, values) when is_list(values), do: value in values
  defp match_filter(value, expected), do: value == expected

  defp fetch_work_object(tenant_id, work_object_id) do
    WorkObject
    |> Ash.Query.set_tenant(tenant_id)
    |> Ash.Query.filter(id == ^work_object_id)
    |> Ash.read(actor: actor(tenant_id), domain: Mezzanine.Work)
    |> case do
      {:ok, [work_object]} -> {:ok, work_object}
      {:ok, []} -> {:error, :not_found}
      {:error, reason} -> {:error, reason}
    end
  end

  defp fetch_current_plan(_tenant_id, nil), do: {:ok, nil}

  defp fetch_current_plan(tenant_id, plan_id) do
    WorkPlan
    |> Ash.Query.set_tenant(tenant_id)
    |> Ash.Query.filter(id == ^plan_id)
    |> Ash.read(actor: actor(tenant_id), domain: Mezzanine.Work)
    |> case do
      {:ok, [plan]} -> {:ok, plan}
      {:ok, []} -> {:error, :not_found}
      {:error, reason} -> {:error, reason}
    end
  end

  defp maybe_supersede_plan(_tenant_id, nil), do: {:ok, nil}

  defp maybe_supersede_plan(tenant_id, plan_id) do
    with {:ok, %WorkPlan{} = plan} <- fetch_current_plan(tenant_id, plan_id) do
      WorkPlan.supersede(plan, actor: actor(tenant_id), tenant: tenant_id)
    end
  end

  defp list_run_series(tenant_id, work_object_id) do
    RunSeries.list_for_work_object(work_object_id, actor: actor(tenant_id), tenant: tenant_id)
  end

  defp fetch_active_run(_tenant_id, []), do: {:ok, nil}

  defp fetch_active_run(tenant_id, [series | _]) do
    case series.current_run_id do
      nil -> {:ok, nil}
      run_id -> fetch_run(tenant_id, run_id)
    end
  end

  defp fetch_run(tenant_id, run_id) do
    Run
    |> Ash.Query.set_tenant(tenant_id)
    |> Ash.Query.filter(id == ^run_id)
    |> Ash.read(actor: actor(tenant_id), domain: Mezzanine.Runs)
    |> case do
      {:ok, [run]} -> {:ok, run}
      {:ok, []} -> {:error, :not_found}
      {:error, reason} -> {:error, reason}
    end
  end

  defp list_pending_reviews_for_work(tenant_id, work_object_id) do
    ReviewUnit.list_for_work_object(work_object_id, actor: actor(tenant_id), tenant: tenant_id)
    |> case do
      {:ok, review_units} ->
        {:ok, Enum.filter(review_units, &(&1.status in [:pending, :in_review, :escalated]))}

      {:error, reason} ->
        {:error, reason}
    end
  end

  defp fetch_control_session(tenant_id, work_object_id) do
    ControlSession
    |> Ash.Query.set_tenant(tenant_id)
    |> Ash.Query.filter(work_object_id == ^work_object_id)
    |> Ash.read(actor: actor(tenant_id), domain: Mezzanine.Control)
    |> case do
      {:ok, [control_session | _]} -> {:ok, control_session}
      {:ok, []} -> {:ok, nil}
      {:error, reason} -> {:error, reason}
    end
  end

  defp open_escalation_count(tenant_id, work_objects) do
    work_ids = Enum.map(work_objects, & &1.id)

    Escalation
    |> Ash.Query.set_tenant(tenant_id)
    |> Ash.Query.filter(work_object_id in ^work_ids and status == :open)
    |> Ash.read(actor: actor(tenant_id), domain: Mezzanine.Review)
    |> case do
      {:ok, escalations} -> {:ok, length(escalations)}
      {:error, reason} -> {:error, reason}
    end
  end

  defp stalled_count(tenant_id, work_objects) do
    run_ids =
      work_objects
      |> Enum.map(&fetch_current_run_id(tenant_id, &1.id))
      |> Enum.filter(&is_binary/1)

    if run_ids == [] do
      {:ok, 0}
    else
      Run
      |> Ash.Query.set_tenant(tenant_id)
      |> Ash.Query.filter(id in ^run_ids and status == :stalled)
      |> Ash.read(actor: actor(tenant_id), domain: Mezzanine.Runs)
      |> case do
        {:ok, runs} -> {:ok, length(runs)}
        {:error, reason} -> {:error, reason}
      end
    end
  end

  defp fetch_current_run_id(tenant_id, work_object_id) do
    case list_run_series(tenant_id, work_object_id) do
      {:ok, [series | _]} -> series.current_run_id
      _ -> nil
    end
  end

  defp obligation_ids(nil), do: []
  defp obligation_ids(plan), do: plan.obligation_ids || []

  defp fetch_string(attrs, opts, key) do
    case map_value(attrs, key) || Keyword.get(opts, key) do
      value when is_binary(value) -> {:ok, value}
      _ -> {:error, {:missing_required, key}}
    end
  end

  defp map_value(map, key) when is_map(map),
    do: Map.get(map, key) || Map.get(map, Atom.to_string(key))

  defp actor(tenant_id), do: %{tenant_id: tenant_id}
end
