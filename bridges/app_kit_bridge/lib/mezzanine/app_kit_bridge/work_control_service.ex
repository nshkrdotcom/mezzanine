defmodule Mezzanine.AppKitBridge.WorkControlService do
  @moduledoc """
  Backend-oriented run-start service for the transitional AppKit bridge.
  """

  require Ash.Query

  alias AppKit.Core.{Result, RunRef}
  alias Mezzanine.Work.{WorkObject, WorkPlan}

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

  defp fetch_string_value(attrs, opts, key, error) do
    case Map.get(attrs, key) || Map.get(attrs, Atom.to_string(key)) || Keyword.get(opts, key) do
      value when is_binary(value) -> {:ok, value}
      _ -> {:error, error}
    end
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

  defp review_required?(plan), do: plan.derived_review_intents != []

  defp map_value(map, key) when is_map(map),
    do: Map.get(map, key) || Map.get(map, Atom.to_string(key))

  defp map_value(_map, _key), do: nil

  defp actor(tenant_id), do: %{tenant_id: tenant_id}

  defp normalize_result({:ok, value}, _fallback), do: {:ok, value}
  defp normalize_result({:error, _reason}, fallback), do: {:error, fallback}
  defp normalize_result(other, _fallback), do: other
end
