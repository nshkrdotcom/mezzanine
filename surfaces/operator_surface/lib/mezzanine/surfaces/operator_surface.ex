defmodule Mezzanine.Surfaces.OperatorSurface do
  @moduledoc """
  Reusable operator-facing projections and intervention entrypoints.
  """

  require Ash.Query

  alias Mezzanine.Assurance
  alias Mezzanine.Control.Commands
  alias Mezzanine.Control.ControlSession
  alias Mezzanine.Review.ReviewUnit
  alias Mezzanine.Runs.{Run, RunArtifact, RunSeries}
  alias Mezzanine.Surfaces.{OperatorAlert, ReviewSummary, RunDetail, SystemHealth, WorkSurface}
  alias Mezzanine.Work.WorkObject

  @spec list_operator_alerts(String.t(), Ecto.UUID.t()) ::
          {:ok, [OperatorAlert.t()]} | {:error, term()}
  def list_operator_alerts(tenant_id, program_id)
      when is_binary(tenant_id) and is_binary(program_id) do
    with {:ok, work_objects} <-
           WorkObject.list_for_program(program_id, actor: actor(tenant_id), tenant: tenant_id) do
      alerts =
        work_objects
        |> Enum.flat_map(&alerts_for_work(tenant_id, &1))
        |> Enum.sort_by(
          &{severity_rank(&1.severity), DateTime.to_unix(&1.raised_at, :microsecond)},
          :desc
        )

      {:ok, alerts}
    end
  end

  @spec get_run_detail(String.t(), Ecto.UUID.t()) :: {:ok, RunDetail.t()} | {:error, term()}
  def get_run_detail(tenant_id, run_id) when is_binary(tenant_id) and is_binary(run_id) do
    with {:ok, run} <- fetch_run(tenant_id, run_id),
         {:ok, run_series} <- fetch_run_series(tenant_id, run.run_series_id),
         {:ok, work_object} <- fetch_work_object(tenant_id, run_series.work_object_id),
         {:ok, review_units} <- list_review_units_for_run(tenant_id, run.id),
         {:ok, run_artifacts} <-
           RunArtifact.list_for_run(run.id, actor: actor(tenant_id), tenant: tenant_id),
         {:ok, audit_report} <- Mezzanine.WorkAudit.work_report(tenant_id, work_object.id) do
      {:ok,
       %RunDetail{
         run: run,
         run_series: run_series,
         work_object: work_object,
         review_units: review_units,
         run_artifacts: run_artifacts,
         evidence_bundles: audit_report.evidence_bundles,
         timeline: audit_report.timeline,
         audit_events: audit_report.audit_events
       }}
    end
  end

  @spec execute_control(String.t(), Ecto.UUID.t(), atom(), map(), map()) ::
          {:ok, map()} | {:error, term()}
  def execute_control(tenant_id, work_object_id, action, params, actor)
      when is_binary(tenant_id) and is_binary(work_object_id) and is_atom(action) and
             is_map(params) and
             is_map(actor) do
    dispatch_control(tenant_id, work_object_id, action, params, actor_ref(actor))
  end

  @spec override_grant_profile(String.t(), Ecto.UUID.t(), map(), map()) ::
          {:ok, map()} | {:error, term()}
  def override_grant_profile(tenant_id, work_object_id, new_grants, actor)
      when is_binary(tenant_id) and is_binary(work_object_id) and is_map(new_grants) and
             is_map(actor) do
    execute_control(tenant_id, work_object_id, :grant_override, new_grants, actor)
  end

  @spec get_system_health(String.t(), Ecto.UUID.t()) :: {:ok, SystemHealth.t()} | {:error, term()}
  def get_system_health(tenant_id, program_id)
      when is_binary(tenant_id) and is_binary(program_id) do
    with {:ok, queue_stats} <- WorkSurface.work_queue_stats(tenant_id, program_id),
         {:ok, work_objects} <-
           WorkObject.list_for_program(program_id, actor: actor(tenant_id), tenant: tenant_id),
         {:ok, pending_reviews} <- list_pending_reviews(tenant_id, program_id),
         {:ok, open_control_sessions} <- open_control_sessions(tenant_id, program_id),
         {:ok, active_run_count} <- active_run_count(tenant_id, work_objects) do
      {:ok,
       %SystemHealth{
         program_id: program_id,
         queue_stats: queue_stats,
         pending_review_count: length(pending_reviews),
         open_control_session_count: length(open_control_sessions),
         active_run_count: active_run_count,
         stalled_run_count: queue_stats.stalled_count,
         open_escalation_count: queue_stats.open_escalation_count
       }}
    end
  end

  @spec list_pending_reviews(String.t(), Ecto.UUID.t()) ::
          {:ok, [ReviewSummary.t()]} | {:error, term()}
  def list_pending_reviews(tenant_id, program_id)
      when is_binary(tenant_id) and is_binary(program_id) do
    with {:ok, work_index} <- program_work_index(tenant_id, program_id),
         {:ok, review_units} <- Assurance.list_pending_reviews(tenant_id) do
      summaries =
        review_units
        |> Enum.filter(&Map.has_key?(work_index, &1.work_object_id))
        |> Enum.map(fn review_unit ->
          work_object = Map.fetch!(work_index, review_unit.work_object_id)

          %ReviewSummary{
            review_unit_id: review_unit.id,
            work_object_id: review_unit.work_object_id,
            status: review_unit.status,
            review_kind: review_unit.review_kind,
            required_by: review_unit.required_by,
            reviewer_actor: review_unit.reviewer_actor,
            work_title: work_object.title
          }
        end)
        |> Enum.sort_by(&{&1.required_by || DateTime.utc_now(), &1.review_unit_id})

      {:ok, summaries}
    end
  end

  defp dispatch_control(tenant_id, work_object_id, :pause, params, operator_ref),
    do: Commands.pause_work(tenant_id, work_object_id, operator_ref, params)

  defp dispatch_control(tenant_id, work_object_id, :resume, params, operator_ref),
    do: Commands.resume_work(tenant_id, work_object_id, operator_ref, params)

  defp dispatch_control(tenant_id, work_object_id, :cancel, params, operator_ref),
    do: Commands.cancel_work(tenant_id, work_object_id, operator_ref, params)

  defp dispatch_control(tenant_id, work_object_id, :replan, params, operator_ref),
    do: Commands.request_replan(tenant_id, work_object_id, operator_ref, params)

  defp dispatch_control(tenant_id, work_object_id, :grant_override, params, operator_ref),
    do: Commands.override_grant_profile(tenant_id, work_object_id, operator_ref, params)

  defp dispatch_control(_tenant_id, _work_object_id, :force_review, _params, _operator_ref),
    do: {:error, :unsupported_action}

  defp dispatch_control(_tenant_id, _work_object_id, _action, _params, _operator_ref),
    do: {:error, :unsupported_action}

  defp alerts_for_work(tenant_id, work_object) do
    with {:ok, gate_status} <- Assurance.gate_status(tenant_id, work_object.id),
         {:ok, current_run_status} <- current_run_status(tenant_id, work_object.id) do
      []
      |> maybe_add_alert(work_object.status == :blocked, %OperatorAlert{
        work_object_id: work_object.id,
        alert_kind: :blocked,
        severity: :warning,
        message: "Work is blocked and needs operator attention",
        raised_at: work_object.updated_at || work_object.inserted_at,
        work_title: work_object.title
      })
      |> maybe_add_alert(gate_status.escalated_count > 0, %OperatorAlert{
        work_object_id: work_object.id,
        alert_kind: :escalated_review,
        severity: :critical,
        message: "Review escalation is open",
        raised_at: work_object.updated_at || work_object.inserted_at,
        work_title: work_object.title
      })
      |> maybe_add_alert(gate_status.pending_count > 0, %OperatorAlert{
        work_object_id: work_object.id,
        alert_kind: :pending_review,
        severity: :info,
        message: "Review is pending before release",
        raised_at: work_object.updated_at || work_object.inserted_at,
        work_title: work_object.title
      })
      |> maybe_add_alert(current_run_status == :stalled, %OperatorAlert{
        work_object_id: work_object.id,
        alert_kind: :stalled,
        severity: :critical,
        message: "Active run is stalled",
        raised_at: work_object.updated_at || work_object.inserted_at,
        work_title: work_object.title
      })
    else
      _ -> []
    end
  end

  defp maybe_add_alert(alerts, true, alert), do: [alert | alerts]
  defp maybe_add_alert(alerts, false, _alert), do: alerts

  defp current_run_status(tenant_id, work_object_id) do
    with {:ok, [series | _]} <-
           RunSeries.list_for_work_object(work_object_id,
             actor: actor(tenant_id),
             tenant: tenant_id
           ),
         true <- is_binary(series.current_run_id),
         {:ok, run} <- fetch_run(tenant_id, series.current_run_id) do
      {:ok, run.status}
    else
      {:ok, []} -> {:ok, nil}
      false -> {:ok, nil}
      error -> error
    end
  end

  defp program_work_index(tenant_id, program_id) do
    WorkObject.list_for_program(program_id, actor: actor(tenant_id), tenant: tenant_id)
    |> case do
      {:ok, work_objects} -> {:ok, Map.new(work_objects, &{&1.id, &1})}
      {:error, reason} -> {:error, reason}
    end
  end

  defp open_control_sessions(tenant_id, program_id) do
    ControlSession.list_for_program(program_id, actor: actor(tenant_id), tenant: tenant_id)
    |> case do
      {:ok, sessions} -> {:ok, Enum.filter(sessions, &(&1.status == :active))}
      {:error, reason} -> {:error, reason}
    end
  end

  defp active_run_count(tenant_id, work_objects) do
    work_ids = Enum.map(work_objects, & &1.id)

    if work_ids == [] do
      {:ok, 0}
    else
      RunSeries
      |> Ash.Query.set_tenant(tenant_id)
      |> Ash.Query.filter(work_object_id in ^work_ids and status == :active)
      |> Ash.read(actor: actor(tenant_id), domain: Mezzanine.Runs)
      |> case do
        {:ok, series} -> {:ok, Enum.count(series, &is_binary(&1.current_run_id))}
        {:error, reason} -> {:error, reason}
      end
    end
  end

  defp list_review_units_for_run(tenant_id, run_id) do
    ReviewUnit
    |> Ash.Query.set_tenant(tenant_id)
    |> Ash.Query.filter(run_id == ^run_id)
    |> Ash.read(actor: actor(tenant_id), domain: Mezzanine.Review)
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

  defp fetch_run_series(_tenant_id, nil), do: {:ok, nil}

  defp fetch_run_series(tenant_id, run_series_id) do
    RunSeries
    |> Ash.Query.set_tenant(tenant_id)
    |> Ash.Query.filter(id == ^run_series_id)
    |> Ash.read(actor: actor(tenant_id), domain: Mezzanine.Runs)
    |> case do
      {:ok, [run_series]} -> {:ok, run_series}
      {:ok, []} -> {:error, :not_found}
      {:error, reason} -> {:error, reason}
    end
  end

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

  defp actor_ref(actor) do
    Map.get(actor, :actor_ref) || Map.get(actor, "actor_ref") || Map.get(actor, :id) ||
      Map.get(actor, "id") || "operator"
  end

  defp severity_rank(:critical), do: 3
  defp severity_rank(:warning), do: 2
  defp severity_rank(:info), do: 1

  defp actor(tenant_id), do: %{tenant_id: tenant_id}
end
