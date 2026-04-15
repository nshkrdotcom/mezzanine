defmodule Mezzanine.Assurance do
  @moduledoc """
  Review and release-readiness services above the durable review domain.
  """

  require Ash.Query

  alias Mezzanine.Assurance.{GateEvaluator, WaiverEngine}
  alias Mezzanine.Audit
  alias Mezzanine.Review.{Escalation, ReviewDecision, ReviewUnit, Waiver}

  @spec list_pending_reviews(String.t()) :: {:ok, [struct()]} | {:error, term()}
  def list_pending_reviews(tenant_id) when is_binary(tenant_id) do
    ReviewUnit
    |> Ash.Query.set_tenant(tenant_id)
    |> Ash.Query.filter(status in [:pending, :in_review, :escalated])
    |> Ash.read(actor: actor(tenant_id), domain: Mezzanine.Review)
  end

  @spec review_detail(String.t(), Ecto.UUID.t()) :: {:ok, map()} | {:error, term()}
  def review_detail(tenant_id, review_unit_id)
      when is_binary(tenant_id) and is_binary(review_unit_id) do
    with {:ok, review_unit} <- fetch_review_unit(tenant_id, review_unit_id),
         {:ok, decisions} <- list_decisions(tenant_id, review_unit.id),
         {:ok, waivers} <- list_waivers(tenant_id, review_unit.id),
         {:ok, escalations} <- list_escalations(tenant_id, review_unit.id) do
      {:ok,
       %{
         review_unit: review_unit,
         decisions: decisions,
         waivers: waivers,
         escalations: escalations,
         gate_status: GateEvaluator.evaluate([review_unit], escalations)
       }}
    end
  end

  @spec gate_status(String.t(), Ecto.UUID.t()) :: {:ok, map()} | {:error, term()}
  def gate_status(tenant_id, work_object_id)
      when is_binary(tenant_id) and is_binary(work_object_id) do
    with {:ok, review_units} <- review_units_for_work(tenant_id, work_object_id),
         {:ok, escalations} <- escalations_for_work(tenant_id, work_object_id) do
      {:ok, GateEvaluator.evaluate(review_units, escalations)}
    end
  end

  @spec release_ready?(String.t(), Ecto.UUID.t()) :: {:ok, boolean()} | {:error, term()}
  def release_ready?(tenant_id, work_object_id)
      when is_binary(tenant_id) and is_binary(work_object_id) do
    with {:ok, gate_status} <- gate_status(tenant_id, work_object_id) do
      {:ok, gate_status.release_ready?}
    end
  end

  @spec record_decision(String.t(), Ecto.UUID.t(), map()) :: {:ok, map()} | {:error, term()}
  def record_decision(tenant_id, review_unit_id, attrs)
      when is_binary(tenant_id) and is_binary(review_unit_id) and is_map(attrs) do
    decision = Map.get(attrs, :decision)

    if decision in [:accept, :reject] do
      with {:ok, review_unit} <- fetch_review_unit(tenant_id, review_unit_id),
           {:ok, decision_record} <- create_decision(tenant_id, review_unit_id, attrs),
           {:ok, updated_review_unit} <- transition_review_unit(tenant_id, review_unit, decision),
           {:ok, _audit} <- record_review_audit(tenant_id, review_unit, decision, attrs) do
        {:ok, %{review_unit: updated_review_unit, decision: decision_record}}
      end
    else
      {:error, :unsupported_decision}
    end
  end

  @spec waive_review(String.t(), Ecto.UUID.t(), map()) :: {:ok, map()} | {:error, term()}
  def waive_review(tenant_id, review_unit_id, attrs)
      when is_binary(tenant_id) and is_binary(review_unit_id) and is_map(attrs) do
    with {:ok, review_unit} <- fetch_review_unit(tenant_id, review_unit_id),
         {:ok, waiver} <- create_waiver(tenant_id, review_unit, attrs),
         {:ok, updated_review_unit} <- transition_review_unit(tenant_id, review_unit, :waive),
         {:ok, _audit} <- record_review_audit(tenant_id, review_unit, :waive, attrs) do
      {:ok, %{review_unit: updated_review_unit, waiver: waiver}}
    end
  end

  @spec escalate_review(String.t(), Ecto.UUID.t(), map()) :: {:ok, map()} | {:error, term()}
  def escalate_review(tenant_id, review_unit_id, attrs)
      when is_binary(tenant_id) and is_binary(review_unit_id) and is_map(attrs) do
    with {:ok, review_unit} <- fetch_review_unit(tenant_id, review_unit_id),
         {:ok, escalation} <- create_escalation(tenant_id, review_unit, attrs),
         {:ok, updated_review_unit} <- transition_review_unit(tenant_id, review_unit, :escalate),
         {:ok, _audit} <- record_review_audit(tenant_id, review_unit, :escalate, attrs) do
      {:ok, %{review_unit: updated_review_unit, escalation: escalation}}
    end
  end

  defp record_review_audit(tenant_id, review_unit, decision, attrs) do
    Audit.record_event(tenant_id, %{
      program_id: Map.fetch!(attrs, :program_id),
      work_object_id: review_unit.work_object_id,
      review_unit_id: review_unit.id,
      event_kind: audit_event_for(decision),
      actor_kind: Map.get(attrs, :actor_kind, :human),
      actor_ref: Map.get(attrs, :actor_ref, "reviewer"),
      payload: Map.get(attrs, :payload, %{})
    })
  end

  defp create_decision(tenant_id, review_unit_id, attrs) do
    ReviewDecision
    |> Ash.Changeset.for_create(:record_decision, %{
      review_unit_id: review_unit_id,
      decision: Map.fetch!(attrs, :decision),
      actor_kind: Map.get(attrs, :actor_kind, :human),
      actor_ref: Map.get(attrs, :actor_ref, "reviewer"),
      reason: Map.get(attrs, :reason),
      payload: Map.get(attrs, :payload, %{}),
      decided_at: Map.get(attrs, :decided_at, DateTime.utc_now())
    })
    |> Ash.Changeset.set_tenant(tenant_id)
    |> Ash.create(actor: actor(tenant_id), authorize?: false, domain: Mezzanine.Review)
  end

  defp create_waiver(tenant_id, review_unit, attrs) do
    if WaiverEngine.active?(Map.get(attrs, :expires_at, DateTime.utc_now())) do
      Waiver
      |> Ash.Changeset.for_create(:grant_waiver, %{
        review_unit_id: review_unit.id,
        work_object_id: review_unit.work_object_id,
        reason: Map.fetch!(attrs, :reason),
        granted_by: Map.fetch!(attrs, :actor_ref),
        expires_at: Map.get(attrs, :expires_at),
        conditions: Map.get(attrs, :conditions, [])
      })
      |> Ash.Changeset.set_tenant(tenant_id)
      |> Ash.create(actor: actor(tenant_id), authorize?: false, domain: Mezzanine.Review)
    else
      {:error, :expired_waiver}
    end
  end

  defp create_escalation(tenant_id, review_unit, attrs) do
    Escalation
    |> Ash.Changeset.for_create(:raise_escalation, %{
      review_unit_id: review_unit.id,
      work_object_id: review_unit.work_object_id,
      reason: Map.get(attrs, :reason),
      escalated_by: Map.get(attrs, :actor_ref),
      assigned_to: Map.get(attrs, :assigned_to),
      priority: Map.get(attrs, :priority, :normal)
    })
    |> Ash.Changeset.set_tenant(tenant_id)
    |> Ash.create(actor: actor(tenant_id), authorize?: false, domain: Mezzanine.Review)
  end

  defp transition_review_unit(tenant_id, review_unit, :accept) do
    transition_review_unit(tenant_id, review_unit, :accept, %{})
  end

  defp transition_review_unit(tenant_id, review_unit, :reject) do
    transition_review_unit(tenant_id, review_unit, :reject, %{})
  end

  defp transition_review_unit(tenant_id, review_unit, :waive) do
    transition_review_unit(tenant_id, review_unit, :waive, %{})
  end

  defp transition_review_unit(tenant_id, review_unit, :escalate) do
    transition_review_unit(tenant_id, review_unit, :escalate, %{})
  end

  defp transition_review_unit(tenant_id, review_unit, action, attrs) do
    review_unit
    |> Ash.Changeset.for_update(action, attrs)
    |> Ash.Changeset.set_tenant(tenant_id)
    |> Ash.update(actor: actor(tenant_id), authorize?: false, domain: Mezzanine.Review)
  end

  defp review_units_for_work(tenant_id, work_object_id) do
    ReviewUnit
    |> Ash.Query.set_tenant(tenant_id)
    |> Ash.Query.filter(work_object_id == ^work_object_id)
    |> Ash.read(actor: actor(tenant_id), domain: Mezzanine.Review)
  end

  defp escalations_for_work(tenant_id, work_object_id) do
    Escalation
    |> Ash.Query.set_tenant(tenant_id)
    |> Ash.Query.filter(work_object_id == ^work_object_id and status == :open)
    |> Ash.read(actor: actor(tenant_id), domain: Mezzanine.Review)
  end

  defp list_decisions(tenant_id, review_unit_id) do
    ReviewDecision
    |> Ash.Query.set_tenant(tenant_id)
    |> Ash.Query.filter(review_unit_id == ^review_unit_id)
    |> Ash.read(actor: actor(tenant_id), domain: Mezzanine.Review)
  end

  defp list_waivers(tenant_id, review_unit_id) do
    Waiver
    |> Ash.Query.set_tenant(tenant_id)
    |> Ash.Query.filter(review_unit_id == ^review_unit_id)
    |> Ash.read(actor: actor(tenant_id), domain: Mezzanine.Review)
  end

  defp list_escalations(tenant_id, review_unit_id) do
    Escalation
    |> Ash.Query.set_tenant(tenant_id)
    |> Ash.Query.filter(review_unit_id == ^review_unit_id)
    |> Ash.read(actor: actor(tenant_id), domain: Mezzanine.Review)
  end

  defp fetch_review_unit(tenant_id, review_unit_id) do
    ReviewUnit
    |> Ash.Query.set_tenant(tenant_id)
    |> Ash.Query.filter(id == ^review_unit_id)
    |> Ash.read(actor: actor(tenant_id), domain: Mezzanine.Review)
    |> case do
      {:ok, [review_unit]} -> {:ok, review_unit}
      {:ok, []} -> {:error, :not_found}
      {:error, error} -> {:error, error}
    end
  end

  defp audit_event_for(:accept), do: :review_accepted
  defp audit_event_for(:reject), do: :review_rejected
  defp audit_event_for(:waive), do: :review_waived
  defp audit_event_for(:escalate), do: :escalation_raised

  defp actor(tenant_id), do: %{tenant_id: tenant_id}
end
