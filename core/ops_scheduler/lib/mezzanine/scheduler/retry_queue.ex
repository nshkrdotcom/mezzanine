defmodule Mezzanine.Scheduler.RetryQueue do
  @moduledoc """
  Selects failed runs that are due for retry.
  """

  require Ash.Query

  alias Mezzanine.Planner
  alias Mezzanine.Runs.{Run, RunSeries}
  alias Mezzanine.Work.{WorkObject, WorkPlan}

  @spec due_runs(String.t(), DateTime.t()) :: {:ok, [map()]} | {:error, term()}
  def due_runs(tenant_id, now \\ DateTime.utc_now()) do
    actor = %{tenant_id: tenant_id}

    with {:ok, failed_runs} <-
           Run
           |> Ash.Query.set_tenant(tenant_id)
           |> Ash.Query.filter(status == :failed and not is_nil(completed_at))
           |> Ash.read(actor: actor, domain: Mezzanine.Runs) do
      {:ok,
       failed_runs
       |> Enum.reduce([], fn run, acc ->
         case due_item(actor, tenant_id, run) do
           {:ok, %{retry: %{due_at: due_at}} = item} ->
             if DateTime.compare(due_at, now) in [:lt, :eq], do: [item | acc], else: acc

           _ ->
             acc
         end
       end)
       |> Enum.sort_by(fn %{retry: %{due_at: due_at}} ->
         DateTime.to_unix(due_at, :microsecond)
       end)}
    end
  end

  defp due_item(actor, tenant_id, %Run{} = run) do
    with {:ok, run_series} <- fetch_run_series(actor, tenant_id, run.run_series_id),
         {:ok, work_object} <- fetch_work_object(actor, tenant_id, run_series.work_object_id),
         {:ok, work_plan} <- fetch_work_plan(actor, tenant_id, work_object.current_plan_id),
         {:ok, retry} <-
           Planner.next_retry(
             %{attempts: run.attempt},
             normalize_retry_profile(work_plan.metadata),
             run.completed_at
           ) do
      {:ok,
       %{
         run: run,
         run_series: run_series,
         work_object: work_object,
         work_plan: work_plan,
         retry: retry
       }}
    end
  end

  defp fetch_run_series(actor, tenant_id, run_series_id) do
    case RunSeries
         |> Ash.Query.set_tenant(tenant_id)
         |> Ash.Query.filter(id == ^run_series_id)
         |> Ash.read(actor: actor, domain: Mezzanine.Runs) do
      {:ok, [run_series]} -> {:ok, run_series}
      {:ok, []} -> {:error, :run_series_not_found}
      {:error, error} -> {:error, error}
    end
  end

  defp fetch_work_object(actor, tenant_id, work_object_id) do
    case WorkObject
         |> Ash.Query.set_tenant(tenant_id)
         |> Ash.Query.filter(id == ^work_object_id)
         |> Ash.read(actor: actor, domain: Mezzanine.Work) do
      {:ok, [work_object]} -> {:ok, work_object}
      {:ok, []} -> {:error, :work_object_not_found}
      {:error, error} -> {:error, error}
    end
  end

  defp fetch_work_plan(_actor, _tenant_id, nil), do: {:error, :work_plan_not_found}

  defp fetch_work_plan(actor, tenant_id, work_plan_id) do
    case WorkPlan
         |> Ash.Query.set_tenant(tenant_id)
         |> Ash.Query.filter(id == ^work_plan_id)
         |> Ash.read(actor: actor, domain: Mezzanine.Work) do
      {:ok, [work_plan]} -> {:ok, work_plan}
      {:ok, []} -> {:error, :work_plan_not_found}
      {:error, error} -> {:error, error}
    end
  end

  defp normalize_retry_profile(metadata) do
    metadata
    |> Map.get("retry_profile", Map.get(metadata, :retry_profile, %{}))
    |> Enum.reduce(%{}, fn {key, value}, acc ->
      Map.put(acc, normalize_key(key), value)
    end)
  end

  defp normalize_key(key) when is_atom(key), do: key
  defp normalize_key(key) when is_binary(key), do: String.to_existing_atom(key)
end
