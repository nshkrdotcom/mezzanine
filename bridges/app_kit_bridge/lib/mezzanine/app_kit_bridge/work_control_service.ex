defmodule Mezzanine.AppKitBridge.WorkControlService do
  @moduledoc """
  Backend-oriented run-start service for the transitional AppKit bridge.
  """

  require Ash.Query

  alias AppKit.Core.{RequestContext, Result, RunRef, RunRequest}
  alias Mezzanine.AppKitBridge.AdapterSupport
  alias Mezzanine.Control.ControlSession
  alias Mezzanine.Review.ReviewUnit
  alias Mezzanine.Runs.{Run, RunSeries}
  alias Mezzanine.Work.{WorkObject, WorkPlan}
  alias Mezzanine.WorkAudit

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

  @typep request_context_input :: %{
           required(:__struct__) => RequestContext,
           required(:trace_id) => String.t(),
           required(:actor_ref) => %{required(:id) => String.t()},
           required(:tenant_ref) => %{required(:id) => String.t()},
           optional(:installation_ref) => map() | nil,
           optional(:causation_id) => String.t() | nil,
           optional(:request_id) => String.t() | nil,
           optional(:idempotency_key) => String.t() | nil,
           optional(:feature_flags) => %{optional(String.t()) => boolean()},
           optional(:metadata) => map()
         }

  @typep run_request_input :: %{
           required(:__struct__) => RunRequest,
           required(:subject_ref) => %{required(:id) => String.t()},
           optional(:recipe_ref) => String.t() | nil,
           optional(:params) => map(),
           optional(:reason) => String.t() | nil,
           optional(:metadata) => map()
         }

  @spec start_run(map(), keyword()) :: {:ok, Result.t()} | {:error, atom()}
  def start_run(domain_call, opts \\ []) when is_map(domain_call) and is_list(opts) do
    attrs = Map.new(domain_call)

    with {:ok, tenant_id} <- fetch_tenant_id(opts),
         {:ok, program_id} <- fetch_program_id(attrs, opts),
         {:ok, work_class_id} <- fetch_work_class_id(attrs, opts),
         {:ok, work_object} <- ingest_work_object(attrs, tenant_id, program_id, work_class_id),
         {:ok, planned_work_object} <- compile_plan(work_object, tenant_id),
         {:ok, plan} <- fetch_current_plan(tenant_id, planned_work_object.current_plan_id),
         {:ok, run_ref} <- build_run_ref(plan, planned_work_object, attrs, opts),
         {:ok, result} <- build_result(run_ref, planned_work_object, plan) do
      {:ok, result}
    else
      {:error, reason} -> {:error, reason}
      _ -> {:error, :bridge_failed}
    end
  end

  @spec start_run(request_context_input(), run_request_input(), keyword()) ::
          {:ok, Result.t()} | {:error, atom()}
  def start_run(%RequestContext{} = context, %RunRequest{} = run_request, opts)
      when is_list(opts) do
    with {:ok, tenant_id} <- fetch_tenant_id(context, opts),
         {:ok, work_object} <- fetch_work_object(tenant_id, run_request.subject_ref.id),
         {:ok, ensured_work_object} <- ensure_current_plan(work_object, tenant_id),
         {:ok, plan} <- fetch_current_plan(tenant_id, ensured_work_object.current_plan_id),
         {:ok, control_session} <- ensure_control_session(tenant_id, ensured_work_object),
         {:ok, run_series} <- ensure_run_series(tenant_id, ensured_work_object, control_session),
         {:ok, run, review_unit} <-
           ensure_active_run(
             tenant_id,
             ensured_work_object,
             plan,
             run_series,
             context,
             run_request
           ),
         {:ok, run_ref} <-
           build_typed_run_ref(
             context,
             run_request,
             ensured_work_object,
             plan,
             run,
             review_unit,
             opts
           ),
         {:ok, result} <-
           build_typed_result(
             context,
             run_request,
             ensured_work_object,
             plan,
             run_ref,
             review_unit
           ) do
      {:ok, result}
    else
      {:error, reason} -> {:error, reason}
      _ -> {:error, :bridge_failed}
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
        priority: Map.get(attrs, :priority, 50),
        source_kind: Map.get(attrs, :source_kind, "app_kit"),
        payload: payload(attrs),
        normalized_payload: Map.get(attrs, :normalized_payload, payload(attrs))
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

  defp ensure_current_plan(%WorkObject{current_plan_id: nil} = work_object, tenant_id),
    do: compile_plan(work_object, tenant_id)

  defp ensure_current_plan(%WorkObject{} = work_object, tenant_id) do
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

  defp ensure_control_session(tenant_id, %WorkObject{} = work_object) do
    ControlSession
    |> Ash.Query.set_tenant(tenant_id)
    |> Ash.Query.filter(work_object_id == ^work_object.id and status == :active)
    |> Ash.read(actor: actor(tenant_id), authorize?: false, domain: Mezzanine.Control)
    |> case do
      {:ok, [session | _]} ->
        {:ok, session}

      {:ok, []} ->
        ControlSession.open(
          %{program_id: work_object.program_id, work_object_id: work_object.id},
          actor: actor(tenant_id),
          tenant: tenant_id
        )

      {:error, _reason} ->
        {:error, :control_session_unavailable}
    end
  end

  defp ensure_run_series(
         tenant_id,
         %WorkObject{} = work_object,
         %ControlSession{} = control_session
       ) do
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
         %RequestContext{} = context,
         %RunRequest{} = run_request
       ) do
    with {:ok, current_run} <- fetch_current_run(tenant_id, run_series) do
      case current_run do
        %Run{} = run when run.status in @active_run_statuses ->
          active_run_result(tenant_id, work_object, plan, run, context, run_request)

        _other ->
          schedule_run(tenant_id, work_object, plan, run_series, context, run_request)
      end
    end
  end

  defp active_run_result(
         tenant_id,
         %WorkObject{} = work_object,
         %WorkPlan{} = plan,
         %Run{} = run,
         %RequestContext{} = context,
         %RunRequest{} = run_request
       ) do
    case ensure_review_unit(tenant_id, work_object, plan, run, context, run_request) do
      {:ok, review_unit} -> {:ok, run, review_unit}
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
         %RequestContext{} = context,
         %RunRequest{} = run_request
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
           ensure_review_unit(tenant_id, work_object, plan, run, context, run_request),
         :ok <- maybe_mark_running(tenant_id, work_object, review_unit),
         {:ok, _audit_event} <-
           record_run_scheduled_event(
             tenant_id,
             work_object,
             run,
             attempt,
             context.trace_id,
             run_request.recipe_ref
           ) do
      {:ok, run, review_unit}
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
         %RequestContext{} = context,
         %RunRequest{} = run_request
       ) do
    if review_required?(plan) do
      case pending_review_unit_for_run(tenant_id, run.id) do
        {:ok, %ReviewUnit{} = review_unit} ->
          {:ok, review_unit}

        {:ok, nil} ->
          create_review_unit(tenant_id, work_object, plan, run, context, run_request)

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
         %RequestContext{} = context,
         %RunRequest{} = run_request
       ) do
    ReviewUnit.create_review_unit(
      %{
        work_object_id: work_object.id,
        run_id: run.id,
        review_kind: review_kind(plan),
        required_by: review_required_by(plan),
        decision_profile: decision_profile(plan),
        reviewer_actor: reviewer_actor(plan, context, run_request)
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
      actor_ref: "app_kit_bridge",
      payload: %{
        attempt: attempt,
        trace_id: trace_id,
        recipe_ref: recipe_ref
      }
    })
    |> normalize_result(:audit_event_failed)
  end

  defp build_run_ref(plan, work_object, attrs, opts) do
    intent = first_run_intent(plan)

    RunRef.new(%{
      run_id: map_value(intent, :intent_id) || "work/#{work_object.id}",
      scope_id: Keyword.get(opts, :scope_id, "program/#{work_object.program_id}"),
      metadata: %{
        tenant_id: Keyword.get(opts, :tenant_id),
        work_object_id: work_object.id,
        plan_id: plan.id,
        review_required: review_required?(plan),
        review_unit_id: Map.get(attrs, :review_unit_id, Map.get(attrs, "review_unit_id")),
        program_id: work_object.program_id
      }
    })
    |> normalize_result(:invalid_run_ref)
  end

  defp build_result(run_ref, work_object, plan) do
    state = if review_required?(plan), do: :waiting_review, else: :scheduled

    Result.new(%{
      surface: :work_control,
      state: state,
      payload: %{
        run_ref: run_ref,
        work_object_id: work_object.id,
        plan_id: plan.id,
        run_intent: first_run_intent(plan),
        review_required: review_required?(plan)
      }
    })
    |> normalize_result(:invalid_result)
  end

  defp build_typed_run_ref(
         %RequestContext{} = context,
         %RunRequest{} = run_request,
         %WorkObject{} = work_object,
         %WorkPlan{} = plan,
         %Run{} = run,
         review_unit,
         opts
       ) do
    RunRef.new(%{
      run_id: run.id,
      scope_id: Keyword.get(opts, :scope_id, "program/#{work_object.program_id}"),
      metadata: %{
        tenant_id: context.tenant_ref.id,
        work_object_id: work_object.id,
        plan_id: plan.id,
        program_id: work_object.program_id,
        review_required: review_required?(plan),
        review_unit_id: review_unit_id(review_unit),
        recipe_ref: run_request.recipe_ref || map_value(first_run_intent(plan), :intent_id),
        trace_id: context.trace_id
      }
    })
    |> normalize_result(:invalid_run_ref)
  end

  defp build_typed_result(
         %RequestContext{} = context,
         %RunRequest{} = run_request,
         %WorkObject{} = work_object,
         %WorkPlan{} = plan,
         %RunRef{} = run_ref,
         review_unit
       ) do
    state = if review_required?(plan), do: :waiting_review, else: :scheduled

    Result.new(%{
      surface: :work_control,
      state: state,
      payload: %{
        run_ref: run_ref,
        work_object_id: work_object.id,
        subject_ref: run_request.subject_ref,
        trace_id: context.trace_id,
        plan_id: plan.id,
        recipe_ref: run_request.recipe_ref,
        params: run_request.params,
        run_intent: first_run_intent(plan),
        review_required: review_required?(plan),
        review_unit_id: review_unit_id(review_unit)
      }
    })
    |> normalize_result(:invalid_result)
  end

  defp fetch_program_id(attrs, opts),
    do: fetch_string_value(attrs, opts, :program_id, :missing_program_id)

  defp fetch_work_class_id(attrs, opts),
    do: fetch_string_value(attrs, opts, :work_class_id, :missing_work_class_id)

  defp fetch_tenant_id(opts) do
    case Keyword.get(opts, :tenant_id) do
      value when is_binary(value) -> {:ok, value}
      _ -> {:error, :missing_tenant_id}
    end
  end

  defp fetch_tenant_id(%RequestContext{tenant_ref: %{id: tenant_id}}, _opts)
       when is_binary(tenant_id),
       do: {:ok, tenant_id}

  defp fetch_tenant_id(_context, opts), do: fetch_tenant_id(opts)

  defp fetch_string_value(attrs, opts, key, error) do
    AdapterSupport.fetch_string(attrs, opts, key, error)
  end

  defp external_ref(attrs) do
    Map.get(attrs, :external_ref) || Map.get(attrs, "external_ref") ||
      "app_kit:#{Map.get(attrs, :route_name, Map.get(attrs, "route_name", "run"))}"
  end

  defp title(attrs) do
    Map.get(attrs, :title) ||
      Map.get(attrs, "title") ||
      Map.get(attrs, :route_name) ||
      Map.get(attrs, "route_name") ||
      "AppKit work item"
  end

  defp description(attrs) do
    Map.get(attrs, :description) ||
      Map.get(attrs, "description") ||
      "Started through AppKit work-control adapter"
  end

  defp payload(attrs) do
    Map.get(attrs, :payload) || Map.get(attrs, "payload") ||
      Map.drop(attrs, [:program_id, :work_class_id])
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

  defp reviewer_actor(plan, %RequestContext{} = context, %RunRequest{} = run_request) do
    map_value(first_review_intent(plan), :reviewer_actor) ||
      Map.get(run_request.metadata, :reviewer_actor) ||
      Map.get(run_request.metadata, "reviewer_actor") ||
      %{"kind" => "human", "ref" => context.actor_ref.id}
  end

  defp review_required?(plan), do: plan.derived_review_intents != []
  defp review_unit_id(nil), do: nil
  defp review_unit_id(%ReviewUnit{id: review_unit_id}), do: review_unit_id

  defp map_value(map, key), do: AdapterSupport.map_value(map, key)
  defp actor(tenant_id), do: AdapterSupport.actor(tenant_id)

  defp normalize_result({:ok, value}, _fallback), do: {:ok, value}
  defp normalize_result({:error, _reason}, fallback), do: {:error, fallback}
  defp normalize_result(other, _fallback), do: other
end
