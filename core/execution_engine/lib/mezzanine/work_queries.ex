defmodule Mezzanine.WorkQueries do
  @moduledoc """
  Neutral governed-work reads and intake helpers used by the northbound bridge.
  """

  require Ash.Query

  alias Mezzanine.Audit.WorkAudit
  alias Mezzanine.Execution.ExecutionRecord
  alias Mezzanine.Review.{Escalation, ReviewUnit}
  alias Mezzanine.Reviews
  alias Mezzanine.Runs.{Run, RunSeries}
  alias Mezzanine.ServiceSupport
  alias Mezzanine.Work.WorkObject
  alias Mezzanine.Work.WorkPlan
  alias Mezzanine.WorkControl
  alias Mezzanine.WorkProjectionFacts

  @active_statuses [:pending, :planning, :planned, :running, :awaiting_review, :blocked]

  @spec ingest_subject(map(), keyword()) :: {:ok, map()} | {:error, term()}
  def ingest_subject(attrs, opts \\ []) when is_map(attrs) and is_list(opts) do
    attrs = Map.new(attrs)

    with {:ok, tenant_id} <- fetch_string(attrs, opts, :tenant_id),
         {:ok, program_id} <- fetch_string(attrs, opts, :program_id),
         {:ok, work_class_id} <- fetch_string(attrs, opts, :work_class_id),
         {:ok, external_ref} <- fetch_string(attrs, opts, :external_ref),
         {:ok, work_object} <-
           upsert_work_object(tenant_id, program_id, work_class_id, external_ref, attrs),
         {:ok, planned_work_object} <- refresh_plan(work_object, tenant_id, attrs) do
      {:ok, subject_summary(planned_work_object)}
    end
  end

  @spec list_subjects(String.t(), Ecto.UUID.t(), map()) :: {:ok, [map()]} | {:error, term()}
  def list_subjects(tenant_id, program_id, filters \\ %{})
      when is_binary(tenant_id) and is_binary(program_id) and is_map(filters) do
    with {:ok, work_objects} <-
           WorkObject.list_for_program(program_id, actor: actor(tenant_id), tenant: tenant_id) do
      {:ok,
       work_objects
       |> Enum.filter(&active_work_object?(&1, filters))
       |> Enum.map(&subject_summary/1)}
    end
  end

  @spec get_subject_detail(String.t(), Ecto.UUID.t()) :: {:ok, map()} | {:error, term()}
  def get_subject_detail(tenant_id, subject_id)
      when is_binary(tenant_id) and is_binary(subject_id) do
    with {:ok, work_object} <- fetch_work_object(tenant_id, subject_id),
         {:ok, current_plan} <- fetch_current_plan(tenant_id, work_object.current_plan_id),
         {:ok, run_series} <- list_run_series(tenant_id, work_object.id),
         {:ok, active_run} <- fetch_active_run(tenant_id, run_series),
         {:ok, active_execution} <- fetch_active_execution(work_object.id),
         {:ok, latest_execution} <- fetch_latest_execution(work_object.id),
         {:ok, pending_reviews} <- list_pending_reviews_for_work(tenant_id, work_object.id),
         {:ok, control_session} <- fetch_control_session(tenant_id, work_object.id),
         {:ok, audit_report} <- WorkAudit.work_report(tenant_id, work_object.id),
         {:ok, gate_status} <- Reviews.gate_status(tenant_id, work_object.id) do
      projection_facts =
        WorkProjectionFacts.build(
          work_object,
          current_plan,
          active_run,
          pending_reviews,
          control_session,
          gate_status
        )

      {:ok,
       %{
         subject_id: work_object.id,
         subject_kind: :work_object,
         program_id: work_object.program_id,
         work_class_id: work_object.work_class_id,
         external_ref: work_object.external_ref,
         title: work_object.title,
         description: work_object.description,
         status: work_object.status,
         priority: work_object.priority,
         source_kind: work_object.source_kind,
         current_plan_id: current_plan_id(current_plan),
         current_plan_status: current_plan_status(current_plan),
         active_run_id: active_run_id(active_run),
         active_run_status: active_run_status(active_run),
         active_execution_id: active_execution_id(active_execution),
         active_execution_dispatch_state: active_execution_dispatch_state(active_execution),
         active_execution_trace_id: active_execution_trace_id(active_execution),
         latest_execution_id: latest_execution_id(latest_execution),
         latest_execution_dispatch_state: latest_execution_dispatch_state(latest_execution),
         latest_execution_trace_id: latest_execution_trace_id(latest_execution),
         run_series_ids: Enum.map(run_series, & &1.id),
         obligation_ids: obligation_ids(current_plan),
         pending_review_ids: Enum.map(pending_reviews, & &1.id),
         evidence_bundle_id: latest_evidence_bundle_id(audit_report),
         control_session_id: control_session_id(control_session),
         control_mode: control_mode(control_session),
         gate_status: gate_status,
         pending_obligations: projection_facts.pending_obligations,
         blocking_conditions: projection_facts.blocking_conditions,
         next_step_preview: projection_facts.next_step_preview,
         timeline: audit_report.timeline,
         audit_events: audit_report.audit_events,
         last_event_at: last_event_at(audit_report.timeline)
       }}
    end
  end

  @spec get_subject_projection(String.t(), Ecto.UUID.t()) :: {:ok, map()} | {:error, term()}
  def get_subject_projection(tenant_id, subject_id)
      when is_binary(tenant_id) and is_binary(subject_id) do
    with {:ok, work_object} <- fetch_work_object(tenant_id, subject_id),
         {:ok, current_plan} <- fetch_current_plan(tenant_id, work_object.current_plan_id),
         {:ok, run_series} <- list_run_series(tenant_id, work_object.id),
         {:ok, active_run} <- fetch_active_run(tenant_id, run_series),
         {:ok, active_execution} <- fetch_active_execution(work_object.id),
         {:ok, latest_execution} <- fetch_latest_execution(work_object.id),
         {:ok, pending_reviews} <- list_pending_reviews_for_work(tenant_id, work_object.id),
         {:ok, control_session} <- fetch_control_session(tenant_id, work_object.id),
         {:ok, gate_status} <- Reviews.gate_status(tenant_id, work_object.id),
         {:ok, timeline_projection} <- WorkAudit.timeline_for_work(tenant_id, work_object.id) do
      projection_facts =
        WorkProjectionFacts.build(
          work_object,
          current_plan,
          active_run,
          pending_reviews,
          control_session,
          gate_status
        )

      {:ok,
       %{
         subject_id: work_object.id,
         subject_kind: :work_object,
         work_status: work_object.status,
         plan_status: current_plan_status(current_plan),
         run_status: active_run_status(active_run),
         execution_dispatch_state: active_execution_dispatch_state(active_execution),
         latest_execution_dispatch_state: latest_execution_dispatch_state(latest_execution),
         latest_execution_trace_id: latest_execution_trace_id(latest_execution),
         control_mode: control_mode(control_session),
         review_status: gate_status.status,
         release_ready?: gate_status.release_ready?,
         pending_obligations: projection_facts.pending_obligations,
         blocking_conditions: projection_facts.blocking_conditions,
         next_step_preview: projection_facts.next_step_preview,
         last_event_at: timeline_projection.last_event_at
       }}
    end
  end

  @spec queue_stats(String.t(), Ecto.UUID.t()) :: {:ok, map()} | {:error, term()}
  def queue_stats(tenant_id, program_id)
      when is_binary(tenant_id) and is_binary(program_id) do
    with {:ok, work_objects} <- list_subjects(tenant_id, program_id),
         {:ok, open_escalation_count} <- open_escalation_count(tenant_id, work_objects),
         {:ok, stalled_count} <- stalled_count(tenant_id, work_objects) do
      counts_by_status = Enum.frequencies_by(work_objects, & &1.status)

      {:ok,
       %{
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

  @spec active_run_count(String.t(), Ecto.UUID.t()) :: {:ok, non_neg_integer()} | {:error, term()}
  def active_run_count(tenant_id, program_id)
      when is_binary(tenant_id) and is_binary(program_id) do
    with {:ok, work_objects} <-
           WorkObject.list_for_program(program_id, actor: actor(tenant_id), tenant: tenant_id) do
      work_objects
      |> Enum.map(& &1.id)
      |> count_active_runs_for_work_ids(tenant_id)
    end
  end

  defp subject_summary(work_object) do
    %{
      subject_id: work_object.id,
      subject_kind: :work_object,
      program_id: work_object.program_id,
      work_class_id: work_object.work_class_id,
      external_ref: work_object.external_ref,
      title: work_object.title,
      description: work_object.description,
      status: work_object.status,
      priority: work_object.priority,
      source_kind: work_object.source_kind,
      current_plan_id: work_object.current_plan_id,
      inserted_at: work_object.inserted_at,
      updated_at: work_object.updated_at
    }
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
        work_object
        |> Ash.Changeset.for_update(
          :refresh_intake,
          refresh_intake_attrs(attrs, work_class_id, external_ref)
        )
        |> Ash.Changeset.set_tenant(tenant_id)
        |> Ash.update(actor: actor(tenant_id), domain: Mezzanine.Work)

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

  defp fetch_active_execution(subject_id) do
    case ExecutionRecord.active_for_subject(subject_id) do
      {:ok, [execution | _]} -> {:ok, execution}
      {:ok, []} -> {:ok, nil}
      {:error, reason} -> {:error, reason}
    end
  end

  defp fetch_latest_execution(subject_id) do
    ExecutionRecord
    |> Ash.Query.filter(subject_id == ^subject_id)
    |> Ash.read(authorize?: false, domain: Mezzanine.Execution)
    |> case do
      {:ok, executions} ->
        {:ok, Enum.max_by(executions, &execution_sort_key/1, fn -> nil end)}

      {:error, reason} ->
        {:error, reason}
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
    WorkControl.control_session_for_work(tenant_id, work_object_id)
  end

  defp open_escalation_count(tenant_id, work_objects) do
    work_ids = Enum.map(work_objects, & &1.subject_id)

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
      |> Enum.map(&fetch_current_run_id(tenant_id, &1.subject_id))
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

  defp count_active_runs_for_work_ids([], _tenant_id), do: {:ok, 0}

  defp count_active_runs_for_work_ids(work_ids, tenant_id) do
    RunSeries
    |> Ash.Query.set_tenant(tenant_id)
    |> Ash.Query.filter(work_object_id in ^work_ids and status == :active)
    |> Ash.read(actor: actor(tenant_id), domain: Mezzanine.Runs)
    |> case do
      {:ok, series} -> {:ok, Enum.count(series, &is_binary(&1.current_run_id))}
      {:error, reason} -> {:error, reason}
    end
  end

  defp obligation_ids(nil), do: []
  defp obligation_ids(plan), do: plan.obligation_ids || []

  defp latest_evidence_bundle_id(audit_report) do
    audit_report.evidence_bundles
    |> Enum.reverse()
    |> List.first()
    |> case do
      nil -> nil
      evidence_bundle -> evidence_bundle.id
    end
  end

  defp last_event_at([]), do: nil

  defp last_event_at(timeline) do
    timeline
    |> Enum.reverse()
    |> List.first()
    |> case do
      nil -> nil
      event -> Map.get(event, :occurred_at)
    end
  end

  defp current_plan_id(nil), do: nil
  defp current_plan_id(plan), do: plan.id

  defp current_plan_status(nil), do: nil
  defp current_plan_status(plan), do: plan.status

  defp active_run_id(nil), do: nil
  defp active_run_id(run), do: run.id

  defp active_run_status(nil), do: nil
  defp active_run_status(run), do: run.status

  defp active_execution_id(nil), do: nil
  defp active_execution_id(execution), do: execution.id

  defp active_execution_dispatch_state(nil), do: nil
  defp active_execution_dispatch_state(execution), do: execution.dispatch_state

  defp active_execution_trace_id(nil), do: nil
  defp active_execution_trace_id(execution), do: execution.trace_id

  defp latest_execution_id(nil), do: nil
  defp latest_execution_id(execution), do: execution.id

  defp latest_execution_dispatch_state(nil), do: nil
  defp latest_execution_dispatch_state(execution), do: execution.dispatch_state

  defp latest_execution_trace_id(nil), do: nil
  defp latest_execution_trace_id(execution), do: execution.trace_id

  defp control_session_id(nil), do: nil
  defp control_session_id(control_session), do: control_session.id

  defp control_mode(nil), do: nil
  defp control_mode(control_session), do: control_session.current_mode

  defp execution_sort_key(execution) do
    {execution.updated_at || execution.inserted_at, execution.inserted_at, execution.id}
  end

  defp fetch_string(attrs, opts, key) do
    ServiceSupport.fetch_string(attrs, opts, key, {:missing_required, key})
  end

  defp map_value(map, key), do: ServiceSupport.map_value(map, key)
  defp actor(tenant_id), do: ServiceSupport.actor(tenant_id)
end
