defmodule Mezzanine.WorkControl do
  @moduledoc """
  Neutral control-session reads and ensures for governed work.
  """

  require Ash.Query

  alias Mezzanine.Audit.WorkAudit
  alias Mezzanine.Control.ControlSession
  alias Mezzanine.Review.ReviewUnit
  alias Mezzanine.Runs.{Run, RunSeries}
  alias Mezzanine.ServiceSupport
  alias Mezzanine.Work.WorkObject
  alias Mezzanine.Work.WorkPlan

  @active_run_statuses [:pending, :scheduled, :running]
  @review_kind_map %{
    :code_review => :code_review,
    :policy_review => :policy_review,
    :release_review => :release_review,
    :operator_review => :operator_review,
    :evidence_review => :evidence_review,
    "code_review" => :code_review,
    "policy_review" => :policy_review,
    "release_review" => :release_review,
    "operator_review" => :operator_review,
    "evidence_review" => :evidence_review
  }

  @type control_session_record :: struct()
  @type work_object_record :: struct()
  @type work_plan_record :: struct()
  @type run_series_record :: struct()
  @type run_record :: struct()
  @type review_unit_record :: struct()
  @type prepared_run_request :: %{
          work_object: work_object_record(),
          plan: work_plan_record()
        }
  @type started_run :: %{
          work_object: work_object_record(),
          plan: work_plan_record(),
          control_session: control_session_record(),
          run_series: run_series_record(),
          run: run_record(),
          review_unit: review_unit_record() | nil
        }

  @spec prepare_run_request(String.t(), map()) ::
          {:ok, prepared_run_request()} | {:error, term()}
  def prepare_run_request(tenant_id, attrs) when is_binary(tenant_id) and is_map(attrs) do
    attrs = Map.new(attrs)

    with {:ok, program_id} <- required_string(attrs, :program_id, :missing_program_id),
         {:ok, work_class_id} <- required_string(attrs, :work_class_id, :missing_work_class_id),
         {:ok, work_object} <- ingest_work_object(attrs, tenant_id, program_id, work_class_id),
         {:ok, planned_work_object} <- compile_plan(work_object, tenant_id),
         {:ok, plan} <- fetch_current_plan(tenant_id, planned_work_object.current_plan_id) do
      {:ok, %{work_object: planned_work_object, plan: plan}}
    end
  end

  @spec start_run_for_subject(String.t(), Ecto.UUID.t(), map()) ::
          {:ok, started_run()} | {:error, term()}
  def start_run_for_subject(tenant_id, work_object_id, attrs)
      when is_binary(tenant_id) and is_binary(work_object_id) and is_map(attrs) do
    attrs = Map.new(attrs)

    with {:ok, work_object} <- fetch_work_object(tenant_id, work_object_id),
         {:ok, ensured_work_object} <- ensure_current_plan(tenant_id, work_object),
         {:ok, plan} <- fetch_current_plan(tenant_id, ensured_work_object.current_plan_id),
         {:ok, control_session} <- ensure_control_session(tenant_id, ensured_work_object),
         {:ok, run_series} <- ensure_run_series(tenant_id, ensured_work_object, control_session),
         {:ok, run, review_unit, scheduled?} <-
           ensure_active_run(tenant_id, ensured_work_object, plan, run_series, attrs),
         :ok <-
           maybe_finalize_run_schedule(
             scheduled?,
             tenant_id,
             ensured_work_object,
             run,
             review_unit,
             attrs
           ) do
      {:ok,
       %{
         work_object: ensured_work_object,
         plan: plan,
         control_session: control_session,
         run_series: run_series,
         run: run,
         review_unit: review_unit
       }}
    else
      {:error, reason} -> {:error, reason}
    end
  end

  @spec control_session_for_work(String.t(), Ecto.UUID.t()) ::
          {:ok, control_session_record() | nil} | {:error, term()}
  def control_session_for_work(tenant_id, work_object_id)
      when is_binary(tenant_id) and is_binary(work_object_id) do
    ControlSession
    |> Ash.Query.set_tenant(tenant_id)
    |> Ash.Query.filter(work_object_id == ^work_object_id)
    |> Ash.read(actor: actor(tenant_id), authorize?: false, domain: Mezzanine.Control)
    |> case do
      {:ok, [control_session | _]} -> {:ok, control_session}
      {:ok, []} -> {:ok, nil}
      {:error, error} -> {:error, error}
    end
  end

  @spec ensure_control_session(String.t(), work_object_record()) ::
          {:ok, control_session_record()} | {:error, term()}
  def ensure_control_session(tenant_id, %WorkObject{} = work_object)
      when is_binary(tenant_id) do
    case control_session_for_work(tenant_id, work_object.id) do
      {:ok, %ControlSession{} = control_session} ->
        {:ok, control_session}

      {:ok, nil} ->
        ControlSession.open(
          %{program_id: work_object.program_id, work_object_id: work_object.id},
          actor: actor(tenant_id),
          tenant: tenant_id
        )

      {:error, reason} ->
        {:error, reason}
    end
  end

  @spec open_control_sessions(String.t(), Ecto.UUID.t()) ::
          {:ok, [control_session_record()]} | {:error, term()}
  def open_control_sessions(tenant_id, program_id)
      when is_binary(tenant_id) and is_binary(program_id) do
    case ControlSession.list_for_program(program_id, actor: actor(tenant_id), tenant: tenant_id) do
      {:ok, control_sessions} ->
        {:ok, Enum.filter(control_sessions, &(&1.status == :active))}

      {:error, reason} ->
        {:error, reason}
    end
  end

  defp ingest_work_object(attrs, tenant_id, program_id, work_class_id) do
    WorkObject.ingest(
      %{
        program_id: program_id,
        work_class_id: work_class_id,
        external_ref: external_ref(attrs),
        title: title(attrs),
        description: description(attrs),
        priority: map_value(attrs, :priority) || 50,
        source_kind: map_value(attrs, :source_kind) || "app_kit",
        payload: payload(attrs),
        normalized_payload: map_value(attrs, :normalized_payload) || payload(attrs)
      },
      actor: actor(tenant_id),
      tenant: tenant_id
    )
    |> normalize_result(:invalid_work_request)
  end

  defp compile_plan(work_object, tenant_id) do
    WorkObject.compile_plan(work_object, %{}, actor: actor(tenant_id), tenant: tenant_id)
    |> normalize_result(:plan_compile_failed)
  end

  defp ensure_current_plan(tenant_id, %WorkObject{current_plan_id: nil} = work_object),
    do: compile_plan(work_object, tenant_id)

  defp ensure_current_plan(tenant_id, %WorkObject{} = work_object) do
    case fetch_current_plan(tenant_id, work_object.current_plan_id) do
      {:ok, _plan} -> {:ok, work_object}
      {:error, _reason} -> compile_plan(work_object, tenant_id)
    end
  end

  defp fetch_current_plan(_tenant_id, nil), do: {:error, :missing_plan}

  defp fetch_current_plan(tenant_id, plan_id) do
    WorkPlan
    |> Ash.Query.set_tenant(tenant_id)
    |> Ash.Query.filter(id == ^plan_id)
    |> Ash.read(actor: actor(tenant_id), domain: Mezzanine.Work)
    |> case do
      {:ok, [plan]} -> {:ok, plan}
      {:ok, []} -> {:error, :missing_plan}
      {:error, _reason} -> {:error, :missing_plan}
    end
  end

  defp fetch_work_object(tenant_id, work_object_id) do
    WorkObject
    |> Ash.get(
      work_object_id,
      actor: actor(tenant_id),
      authorize?: false,
      domain: Mezzanine.Work,
      tenant: tenant_id
    )
    |> case do
      {:ok, %WorkObject{} = work_object} -> {:ok, work_object}
      {:error, _reason} -> {:error, :not_found}
    end
  end

  defp ensure_run_series(tenant_id, %WorkObject{} = work_object, control_session) do
    case RunSeries.list_for_work_object(work_object.id,
           actor: actor(tenant_id),
           tenant: tenant_id
         ) do
      {:ok, run_series} ->
        case Enum.find(run_series, &(&1.status == :active)) || List.first(run_series) do
          %RunSeries{} = series ->
            {:ok, series}

          nil ->
            RunSeries.open_series(
              %{work_object_id: work_object.id, control_session_id: control_session.id},
              actor: actor(tenant_id),
              tenant: tenant_id
            )
        end

      {:error, _reason} ->
        {:error, :run_series_unavailable}
    end
  end

  defp ensure_active_run(
         tenant_id,
         %WorkObject{} = work_object,
         %WorkPlan{} = plan,
         %RunSeries{} = run_series,
         attrs
       ) do
    with {:ok, current_run} <- fetch_current_run(tenant_id, run_series) do
      case current_run do
        %Run{} = run when run.status in @active_run_statuses ->
          active_run_result(tenant_id, work_object, plan, run, attrs)

        _other ->
          schedule_run(tenant_id, work_object, plan, run_series, attrs)
      end
    end
  end

  defp active_run_result(
         tenant_id,
         %WorkObject{} = work_object,
         %WorkPlan{} = plan,
         %Run{} = run,
         attrs
       ) do
    case ensure_review_unit(tenant_id, work_object, plan, run, attrs) do
      {:ok, review_unit} -> {:ok, run, review_unit, false}
      {:error, reason} -> {:error, reason}
    end
  end

  defp fetch_current_run(_tenant_id, %RunSeries{current_run_id: nil}), do: {:ok, nil}

  defp fetch_current_run(tenant_id, %RunSeries{current_run_id: run_id}) do
    Run
    |> Ash.get(
      run_id,
      actor: actor(tenant_id),
      authorize?: false,
      domain: Mezzanine.Runs,
      tenant: tenant_id
    )
    |> case do
      {:ok, %Run{} = run} -> {:ok, run}
      {:error, _reason} -> {:ok, nil}
    end
  end

  defp schedule_run(
         tenant_id,
         %WorkObject{} = work_object,
         %WorkPlan{} = plan,
         %RunSeries{} = run_series,
         attrs
       ) do
    with {:ok, attempt} <- next_attempt(tenant_id, run_series),
         {:ok, run} <-
           Run.schedule(
             %{
               run_series_id: run_series.id,
               attempt: attempt,
               runtime_profile: runtime_profile(plan),
               placement_profile_id: placement_profile_id(plan),
               grant_profile: grant_profile(plan)
             },
             actor: actor(tenant_id),
             tenant: tenant_id
           ),
         {:ok, _run_series} <-
           RunSeries.attach_current_run(run_series, %{current_run_id: run.id},
             actor: actor(tenant_id),
             tenant: tenant_id
           ),
         {:ok, review_unit} <-
           ensure_review_unit(tenant_id, work_object, plan, run, attrs) do
      {:ok, run, review_unit, true}
    else
      {:error, reason} -> {:error, reason}
    end
  end

  defp next_attempt(tenant_id, %RunSeries{} = run_series) do
    case Run.list_for_series(run_series.id, actor: actor(tenant_id), tenant: tenant_id) do
      {:ok, runs} ->
        attempt =
          runs
          |> Enum.map(& &1.attempt)
          |> Enum.max(fn -> 0 end)
          |> Kernel.+(1)

        {:ok, attempt}

      {:error, _reason} ->
        {:error, :run_schedule_failed}
    end
  end

  defp ensure_review_unit(
         tenant_id,
         %WorkObject{} = work_object,
         %WorkPlan{} = plan,
         %Run{} = run,
         attrs
       ) do
    if review_required?(plan) do
      case pending_review_unit_for_run(tenant_id, run.id) do
        {:ok, %ReviewUnit{} = review_unit} ->
          {:ok, review_unit}

        {:ok, nil} ->
          create_review_unit(tenant_id, work_object, plan, run, attrs)

        {:error, reason} ->
          {:error, reason}
      end
    else
      {:ok, nil}
    end
  end

  defp pending_review_unit_for_run(tenant_id, run_id) do
    ReviewUnit
    |> Ash.Query.set_tenant(tenant_id)
    |> Ash.Query.filter(run_id == ^run_id and status in [:pending, :in_review])
    |> Ash.read(actor: actor(tenant_id), authorize?: false, domain: Mezzanine.Review)
    |> case do
      {:ok, [review_unit | _]} -> {:ok, review_unit}
      {:ok, []} -> {:ok, nil}
      {:error, _reason} -> {:error, :review_unit_unavailable}
    end
  end

  defp create_review_unit(
         tenant_id,
         %WorkObject{} = work_object,
         %WorkPlan{} = plan,
         %Run{} = run,
         attrs
       ) do
    ReviewUnit.create_review_unit(
      %{
        work_object_id: work_object.id,
        run_id: run.id,
        review_kind: review_kind(plan),
        required_by: review_required_by(plan),
        decision_profile: decision_profile(plan),
        reviewer_actor: reviewer_actor(plan, attrs)
      },
      actor: actor(tenant_id),
      tenant: tenant_id
    )
    |> normalize_result(:review_unit_create_failed)
  end

  defp maybe_mark_running(tenant_id, %WorkObject{} = work_object, nil) do
    case WorkObject.mark_running(work_object, actor: actor(tenant_id), tenant: tenant_id) do
      {:ok, _updated_work_object} -> :ok
      {:error, _reason} -> {:error, :work_state_update_failed}
    end
  end

  defp maybe_mark_running(_tenant_id, _work_object, _review_unit), do: :ok

  defp maybe_finalize_run_schedule(false, _tenant_id, _work_object, _run, _review_unit, _attrs),
    do: :ok

  defp maybe_finalize_run_schedule(
         true,
         tenant_id,
         %WorkObject{} = work_object,
         %Run{} = run,
         review_unit,
         attrs
       ) do
    with :ok <- maybe_mark_running(tenant_id, work_object, review_unit),
         {:ok, _audit_event} <-
           record_run_scheduled_event(
             tenant_id,
             work_object,
             run,
             run.attempt,
             map_value(attrs, :trace_id),
             map_value(attrs, :recipe_ref)
           ) do
      :ok
    end
  end

  defp record_run_scheduled_event(
         tenant_id,
         %WorkObject{} = work_object,
         %Run{} = run,
         attempt,
         trace_id,
         recipe_ref
       ) do
    WorkAudit.record_event(tenant_id, %{
      program_id: work_object.program_id,
      work_object_id: work_object.id,
      run_id: run.id,
      event_kind: :run_scheduled,
      actor_kind: :system,
      actor_ref: "work_control",
      payload: %{
        attempt: attempt,
        trace_id: trace_id,
        recipe_ref: recipe_ref
      }
    })
    |> normalize_result(:audit_event_failed)
  end

  defp required_string(attrs, key, error) do
    case map_value(attrs, key) do
      value when is_binary(value) and value != "" -> {:ok, value}
      _ -> {:error, error}
    end
  end

  defp external_ref(attrs) do
    map_value(attrs, :external_ref) || "app_kit:#{map_value(attrs, :route_name) || "run"}"
  end

  defp title(attrs) do
    map_value(attrs, :title) || map_value(attrs, :route_name) || "AppKit work item"
  end

  defp description(attrs) do
    map_value(attrs, :description) || "Started through AppKit work-control adapter"
  end

  defp payload(attrs) do
    map_value(attrs, :payload) || Map.drop(attrs, [:program_id, :work_class_id])
  end

  defp first_run_intent(plan) do
    case plan.derived_run_intents do
      [intent | _] -> intent
      _ -> %{}
    end
  end

  defp first_review_intent(plan) do
    case plan.derived_review_intents do
      [intent | _] -> intent
      _ -> %{}
    end
  end

  defp runtime_profile(plan) do
    map_value(first_run_intent(plan), :runtime_profile) || %{"runtime" => "session"}
  end

  defp placement_profile_id(plan), do: map_value(first_run_intent(plan), :placement_profile_id)
  defp grant_profile(plan), do: map_value(first_run_intent(plan), :grant_profile) || %{}

  defp review_kind(plan) do
    plan
    |> first_review_intent()
    |> map_value(:review_kind)
    |> then(&Map.get(@review_kind_map, &1, :operator_review))
  end

  defp review_required_by(plan) do
    case map_value(first_review_intent(plan), :required_by) do
      %DateTime{} = required_by -> required_by
      _other -> DateTime.add(DateTime.utc_now(), 72 * 60 * 60, :second)
    end
  end

  defp decision_profile(plan) do
    map_value(first_review_intent(plan), :decision_profile) || %{"required_decisions" => 1}
  end

  defp reviewer_actor(plan, attrs) do
    map_value(first_review_intent(plan), :reviewer_actor) ||
      map_value(attrs, :reviewer_actor) ||
      %{"kind" => "human", "ref" => map_value(attrs, :actor_ref) || "operator"}
  end

  defp review_required?(plan), do: plan.derived_review_intents != []

  defp map_value(map, key), do: ServiceSupport.map_value(map, key)
  defp actor(tenant_id), do: ServiceSupport.actor(tenant_id)

  defp normalize_result({:ok, value}, _fallback), do: {:ok, value}
  defp normalize_result({:error, _reason}, fallback), do: {:error, fallback}
  defp normalize_result(other, _fallback), do: other
end
