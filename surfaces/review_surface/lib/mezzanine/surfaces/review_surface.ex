defmodule Mezzanine.Surfaces.ReviewSurface do
  @moduledoc """
  Reusable northbound review-decision and release surface.
  """

  require Ash.Query

  alias Mezzanine.Assurance
  alias Mezzanine.Evidence.EvidenceItem
  alias Mezzanine.Review.ReviewUnit
  alias Mezzanine.Runs.{Run, RunArtifact, RunSeries}
  alias Mezzanine.Surfaces.{ReviewDetail, ReviewListing}
  alias Mezzanine.Work.WorkObject
  alias Mezzanine.WorkAudit

  @opaque work_resource :: struct()

  @spec list_pending_reviews(String.t(), Ecto.UUID.t()) ::
          {:ok, [ReviewListing.t()]} | {:error, term()}
  def list_pending_reviews(tenant_id, program_id)
      when is_binary(tenant_id) and is_binary(program_id) do
    with {:ok, work_index} <- program_work_index(tenant_id, program_id),
         {:ok, review_units} <- Assurance.list_pending_reviews(tenant_id) do
      {:ok,
       review_units
       |> Enum.filter(&Map.has_key?(work_index, &1.work_object_id))
       |> Enum.map(fn review_unit ->
         work_object = Map.fetch!(work_index, review_unit.work_object_id)

         %ReviewListing{
           review_unit_id: review_unit.id,
           work_object_id: review_unit.work_object_id,
           status: review_unit.status,
           review_kind: review_unit.review_kind,
           required_by: review_unit.required_by,
           reviewer_actor: review_unit.reviewer_actor,
           work_title: work_object.title
         }
       end)
       |> Enum.sort_by(&{&1.required_by || DateTime.utc_now(), &1.review_unit_id})}
    end
  end

  @spec get_review_detail(String.t(), Ecto.UUID.t()) :: {:ok, ReviewDetail.t()} | {:error, term()}
  def get_review_detail(tenant_id, review_unit_id)
      when is_binary(tenant_id) and is_binary(review_unit_id) do
    with {:ok, assurance_detail} <- Assurance.review_detail(tenant_id, review_unit_id),
         review_unit = assurance_detail.review_unit,
         {:ok, work_object} <- fetch_work_object(tenant_id, review_unit.work_object_id),
         {:ok, active_run} <- fetch_active_run_for_work(tenant_id, review_unit.work_object_id),
         {:ok, audit_report} <- WorkAudit.work_report(tenant_id, review_unit.work_object_id),
         {:ok, evidence_items} <- list_evidence_items(tenant_id, review_unit.evidence_bundle_id),
         {:ok, run_artifacts} <- list_run_artifacts(tenant_id, review_unit.run_id) do
      {:ok,
       %ReviewDetail{
         review_unit: review_unit,
         work_object: work_object,
         run: active_run,
         evidence_bundle: List.first(Enum.reverse(audit_report.evidence_bundles)),
         evidence_items: evidence_items,
         run_artifacts: run_artifacts,
         audit_timeline: %{
           work_object_id: work_object.id,
           timeline: audit_report.timeline,
           audit_events: audit_report.audit_events
         },
         gate_status: assurance_detail.gate_status,
         decisions: assurance_detail.decisions,
         waivers: assurance_detail.waivers,
         escalations: assurance_detail.escalations
       }}
    end
  end

  @spec accept_review(String.t(), Ecto.UUID.t(), String.t(), map()) ::
          {:ok, map()} | {:error, term()}
  def accept_review(tenant_id, review_unit_id, reason, actor)
      when is_binary(tenant_id) and is_binary(review_unit_id) and is_binary(reason) and
             is_map(actor) do
    with {:ok, review_unit} <- fetch_review_unit(tenant_id, review_unit_id),
         {:ok, work_object} <- fetch_work_object(tenant_id, review_unit.work_object_id) do
      Assurance.record_decision(tenant_id, review_unit_id, %{
        program_id: work_object.program_id,
        decision: :accept,
        actor_kind: :human,
        actor_ref: actor_ref(actor),
        reason: reason,
        payload: %{}
      })
    end
  end

  @spec reject_review(String.t(), Ecto.UUID.t(), String.t(), map()) ::
          {:ok, map()} | {:error, term()}
  def reject_review(tenant_id, review_unit_id, reason, actor)
      when is_binary(tenant_id) and is_binary(review_unit_id) and is_binary(reason) and
             is_map(actor) do
    with {:ok, review_unit} <- fetch_review_unit(tenant_id, review_unit_id),
         {:ok, work_object} <- fetch_work_object(tenant_id, review_unit.work_object_id) do
      Assurance.record_decision(tenant_id, review_unit_id, %{
        program_id: work_object.program_id,
        decision: :reject,
        actor_kind: :human,
        actor_ref: actor_ref(actor),
        reason: reason,
        payload: %{}
      })
    end
  end

  @spec waive_review(String.t(), Ecto.UUID.t(), map(), map()) :: {:ok, map()} | {:error, term()}
  def waive_review(tenant_id, review_unit_id, params, actor)
      when is_binary(tenant_id) and is_binary(review_unit_id) and is_map(params) and is_map(actor) do
    with {:ok, review_unit} <- fetch_review_unit(tenant_id, review_unit_id),
         {:ok, work_object} <- fetch_work_object(tenant_id, review_unit.work_object_id) do
      Assurance.waive_review(tenant_id, review_unit_id, %{
        program_id: work_object.program_id,
        actor_ref: actor_ref(actor),
        reason: Map.get(params, :reason) || Map.get(params, "reason") || "waived by operator",
        expires_at: Map.get(params, :expires_at) || Map.get(params, "expires_at"),
        conditions: Map.get(params, :conditions) || Map.get(params, "conditions") || []
      })
    end
  end

  @spec escalate_review(String.t(), Ecto.UUID.t(), map(), map()) ::
          {:ok, map()} | {:error, term()}
  def escalate_review(tenant_id, review_unit_id, params, actor)
      when is_binary(tenant_id) and is_binary(review_unit_id) and is_map(params) and is_map(actor) do
    with {:ok, review_unit} <- fetch_review_unit(tenant_id, review_unit_id),
         {:ok, work_object} <- fetch_work_object(tenant_id, review_unit.work_object_id) do
      Assurance.escalate_review(tenant_id, review_unit_id, %{
        program_id: work_object.program_id,
        actor_ref: actor_ref(actor),
        reason: Map.get(params, :reason) || Map.get(params, "reason"),
        assigned_to: Map.get(params, :assigned_to) || Map.get(params, "assigned_to"),
        priority: Map.get(params, :priority) || Map.get(params, "priority") || :normal
      })
    end
  end

  @spec release_work(String.t(), Ecto.UUID.t(), map()) ::
          {:ok, work_resource()} | {:error, term()}
  def release_work(tenant_id, work_object_id, actor)
      when is_binary(tenant_id) and is_binary(work_object_id) and is_map(actor) do
    with {:ok, true} <- Assurance.release_ready?(tenant_id, work_object_id),
         {:ok, work_object} <- fetch_work_object(tenant_id, work_object_id),
         {:ok, completed_work} <-
           WorkObject.mark_terminal(
             work_object,
             %{status: :completed},
             actor: actor_map(tenant_id),
             tenant: tenant_id
           ),
         {:ok, _audit} <-
           WorkAudit.record_event(tenant_id, %{
             program_id: completed_work.program_id,
             work_object_id: completed_work.id,
             event_kind: :work_completed,
             actor_kind: :human,
             actor_ref: actor_ref(actor),
             payload: %{released: true}
           }) do
      {:ok, completed_work}
    else
      {:ok, false} -> {:error, :review_gate_not_satisfied}
      {:error, reason} -> {:error, reason}
    end
  end

  defp program_work_index(tenant_id, program_id) do
    WorkObject.list_for_program(program_id, actor: actor_map(tenant_id), tenant: tenant_id)
    |> case do
      {:ok, work_objects} -> {:ok, Map.new(work_objects, &{&1.id, &1})}
      {:error, reason} -> {:error, reason}
    end
  end

  defp fetch_active_run_for_work(tenant_id, work_object_id) do
    with {:ok, run_series} <-
           RunSeries.list_for_work_object(
             work_object_id,
             actor: actor_map(tenant_id),
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
    |> Ash.read(actor: actor_map(tenant_id), domain: Mezzanine.Runs)
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
    |> Ash.read(actor: actor_map(tenant_id), domain: Mezzanine.Evidence)
  end

  defp list_run_artifacts(_tenant_id, nil), do: {:ok, []}

  defp list_run_artifacts(tenant_id, run_id) do
    RunArtifact.list_for_run(run_id, actor: actor_map(tenant_id), tenant: tenant_id)
  end

  defp fetch_review_unit(tenant_id, review_unit_id) do
    ReviewUnit
    |> Ash.Query.set_tenant(tenant_id)
    |> Ash.Query.filter(id == ^review_unit_id)
    |> Ash.read(actor: actor_map(tenant_id), domain: Mezzanine.Review)
    |> case do
      {:ok, [review_unit]} -> {:ok, review_unit}
      {:ok, []} -> {:error, :not_found}
      {:error, reason} -> {:error, reason}
    end
  end

  defp fetch_work_object(tenant_id, work_object_id) do
    WorkObject
    |> Ash.Query.set_tenant(tenant_id)
    |> Ash.Query.filter(id == ^work_object_id)
    |> Ash.read(actor: actor_map(tenant_id), domain: Mezzanine.Work)
    |> case do
      {:ok, [work_object]} -> {:ok, work_object}
      {:ok, []} -> {:error, :not_found}
      {:error, reason} -> {:error, reason}
    end
  end

  defp actor_ref(actor) do
    Map.get(actor, :actor_ref) || Map.get(actor, "actor_ref") || Map.get(actor, :id) ||
      Map.get(actor, "id") || "reviewer"
  end

  defp actor_map(tenant_id), do: %{tenant_id: tenant_id}
end
