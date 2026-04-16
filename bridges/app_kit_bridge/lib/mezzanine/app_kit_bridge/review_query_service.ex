defmodule Mezzanine.AppKitBridge.ReviewQueryService do
  @moduledoc """
  Backend-oriented review listings and detail projections for AppKit consumers.

  The service returns bridge-shaped maps so later northbound DTO contracts can
  bind here without inheriting the deprecated review-surface structs.
  """

  require Ash.Query

  alias Mezzanine.AppKitBridge.AdapterSupport
  alias Mezzanine.Assurance
  alias Mezzanine.Audit
  alias Mezzanine.Evidence.EvidenceItem
  alias Mezzanine.Review.ReviewUnit
  alias Mezzanine.Runs.{Run, RunArtifact, RunSeries}
  alias Mezzanine.Work.WorkObject

  @spec list_pending_reviews(String.t(), Ecto.UUID.t()) :: {:ok, [map()]} | {:error, term()}
  def list_pending_reviews(tenant_id, program_id)
      when is_binary(tenant_id) and is_binary(program_id) do
    with {:ok, work_index} <- program_work_index(tenant_id, program_id),
         {:ok, review_units} <- Assurance.list_pending_reviews(tenant_id) do
      summaries =
        review_units
        |> Enum.filter(&Map.has_key?(work_index, &1.work_object_id))
        |> Enum.map(fn review_unit ->
          work_object = Map.fetch!(work_index, review_unit.work_object_id)
          ref = subject_ref(work_object.id)

          %{
            decision_ref: decision_ref(review_unit.id, ref, review_unit.review_kind),
            subject_ref: ref,
            status: normalize_state(review_unit.status),
            required_by: review_unit.required_by,
            summary: work_object.title,
            payload: %{
              reviewer_actor: normalize_value(review_unit.reviewer_actor),
              review_kind: normalize_state(review_unit.review_kind)
            }
          }
        end)
        |> Enum.sort_by(&{&1.required_by || DateTime.utc_now(), &1.decision_ref.id})

      {:ok, summaries}
    else
      {:error, reason} -> {:error, normalize_error(reason)}
    end
  end

  @spec get_review_detail(String.t(), Ecto.UUID.t()) :: {:ok, map()} | {:error, term()}
  def get_review_detail(tenant_id, review_unit_id)
      when is_binary(tenant_id) and is_binary(review_unit_id) do
    with {:ok, assurance_detail} <- Assurance.review_detail(tenant_id, review_unit_id),
         %ReviewUnit{} = review_unit <- assurance_detail.review_unit,
         subject_ref = subject_ref(review_unit.work_object_id),
         {:ok, work_object} <- fetch_work_object(tenant_id, review_unit.work_object_id),
         {:ok, run} <- fetch_review_run(tenant_id, review_unit),
         {:ok, audit_report} <- Audit.work_report(tenant_id, review_unit.work_object_id),
         {:ok, evidence_items} <- list_evidence_items(tenant_id, review_unit.evidence_bundle_id),
         {:ok, run_artifacts} <- list_run_artifacts(tenant_id, review_unit.run_id) do
      {:ok,
       %{
         decision_ref: decision_ref(review_unit.id, subject_ref, review_unit.review_kind),
         subject_ref: subject_ref,
         status: normalize_state(review_unit.status),
         required_by: review_unit.required_by,
         summary: work_object.title,
         payload: %{
           review_kind: normalize_state(review_unit.review_kind),
           reviewer_actor: normalize_value(review_unit.reviewer_actor),
           review_unit: normalize_value(review_unit),
           work_object: normalize_value(work_object),
           run: normalize_value(run),
           evidence_bundle: normalize_value(select_evidence_bundle(audit_report, review_unit)),
           evidence_items: normalize_value(evidence_items),
           run_artifacts: normalize_value(run_artifacts),
           audit_timeline: %{
             work_object_id: work_object.id,
             timeline: normalize_value(audit_report.timeline),
             audit_events: normalize_value(audit_report.audit_events)
           },
           gate_status: normalize_value(assurance_detail.gate_status),
           decisions: normalize_value(assurance_detail.decisions),
           waivers: normalize_value(assurance_detail.waivers),
           escalations: normalize_value(assurance_detail.escalations)
         }
       }}
    else
      {:error, reason} -> {:error, normalize_error(reason)}
    end
  end

  defp program_work_index(tenant_id, program_id) do
    WorkObject.list_for_program(program_id, actor: actor(tenant_id), tenant: tenant_id)
    |> case do
      {:ok, work_objects} -> {:ok, Map.new(work_objects, &{&1.id, &1})}
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

  defp fetch_review_run(tenant_id, %ReviewUnit{run_id: run_id}) when is_binary(run_id),
    do: fetch_run(tenant_id, run_id)

  defp fetch_review_run(tenant_id, %ReviewUnit{work_object_id: work_object_id}),
    do: fetch_active_run_for_work(tenant_id, work_object_id)

  defp fetch_active_run_for_work(tenant_id, work_object_id) do
    with {:ok, run_series} <-
           RunSeries.list_for_work_object(
             work_object_id,
             actor: actor(tenant_id),
             tenant: tenant_id
           ) do
      fetch_active_run(tenant_id, run_series)
    end
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

  defp list_evidence_items(_tenant_id, nil), do: {:ok, []}

  defp list_evidence_items(tenant_id, evidence_bundle_id) do
    EvidenceItem
    |> Ash.Query.set_tenant(tenant_id)
    |> Ash.Query.filter(evidence_bundle_id == ^evidence_bundle_id)
    |> Ash.read(actor: actor(tenant_id), domain: Mezzanine.Evidence)
  end

  defp list_run_artifacts(_tenant_id, nil), do: {:ok, []}

  defp list_run_artifacts(tenant_id, run_id) do
    RunArtifact.list_for_run(run_id, actor: actor(tenant_id), tenant: tenant_id)
  end

  defp select_evidence_bundle(audit_report, %ReviewUnit{evidence_bundle_id: evidence_bundle_id}) do
    audit_report.evidence_bundles
    |> Enum.find(&(&1.id == evidence_bundle_id))
    |> case do
      nil -> List.first(Enum.reverse(audit_report.evidence_bundles))
      evidence_bundle -> evidence_bundle
    end
  end

  defp subject_ref(subject_id), do: %{id: subject_id, subject_kind: "work_object"}

  defp decision_ref(review_unit_id, subject_ref, review_kind) do
    %{
      id: review_unit_id,
      decision_kind: normalize_state(review_kind),
      subject_ref: subject_ref
    }
  end

  defp actor(tenant_id), do: AdapterSupport.actor(tenant_id)
  defp normalize_state(value), do: AdapterSupport.normalize_state(value)
  defp normalize_value(value), do: AdapterSupport.normalize_value(value)
  defp normalize_error(reason), do: AdapterSupport.normalize_error(reason)
end
