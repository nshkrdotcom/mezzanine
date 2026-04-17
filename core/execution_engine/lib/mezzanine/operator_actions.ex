defmodule Mezzanine.OperatorActions do
  @moduledoc """
  Neutral operator command handling above the durable control, run, and work domains.
  """

  require Ash.Query

  alias Mezzanine.Audit.WorkAudit
  alias Mezzanine.Control.OperatorIntervention
  alias Mezzanine.Runs.{Run, RunSeries}
  alias Mezzanine.Work.WorkObject
  alias Mezzanine.Work.WorkPlan

  @spec pause_work(String.t(), Ecto.UUID.t(), String.t(), map()) ::
          {:ok, map()} | {:error, term()}
  def pause_work(tenant_id, work_object_id, operator_ref, payload \\ %{})
      when is_binary(tenant_id) and is_binary(work_object_id) and is_binary(operator_ref) and
             is_map(payload) do
    with {:ok, work_object} <- fetch_work_object(tenant_id, work_object_id),
         {:ok, control_session} <-
           Mezzanine.WorkControl.ensure_control_session(tenant_id, work_object),
         {:ok, paused_session} <- update_session(control_session, tenant_id, :pause, %{}),
         {:ok, intervention} <-
           record_intervention(tenant_id, paused_session.id, operator_ref, :pause, payload),
         {:ok, _audit} <-
           WorkAudit.record_event(tenant_id, %{
             program_id: work_object.program_id,
             work_object_id: work_object.id,
             event_kind: :operator_paused,
             actor_kind: :human,
             actor_ref: operator_ref,
             payload: Map.put(payload, :control_session_id, paused_session.id)
           }) do
      {:ok, %{control_session: paused_session, intervention: intervention}}
    end
  end

  @spec resume_work(String.t(), Ecto.UUID.t(), String.t(), map()) ::
          {:ok, map()} | {:error, term()}
  def resume_work(tenant_id, work_object_id, operator_ref, payload \\ %{})
      when is_binary(tenant_id) and is_binary(work_object_id) and is_binary(operator_ref) and
             is_map(payload) do
    with {:ok, work_object} <- fetch_work_object(tenant_id, work_object_id),
         {:ok, control_session} <-
           Mezzanine.WorkControl.ensure_control_session(tenant_id, work_object),
         {:ok, resumed_session} <- update_session(control_session, tenant_id, :resume, %{}),
         {:ok, intervention} <-
           record_intervention(tenant_id, resumed_session.id, operator_ref, :resume, payload),
         {:ok, _audit} <-
           WorkAudit.record_event(tenant_id, %{
             program_id: work_object.program_id,
             work_object_id: work_object.id,
             event_kind: :operator_resumed,
             actor_kind: :human,
             actor_ref: operator_ref,
             payload: Map.put(payload, :control_session_id, resumed_session.id)
           }) do
      {:ok, %{control_session: resumed_session, intervention: intervention}}
    end
  end

  @spec cancel_work(String.t(), Ecto.UUID.t(), String.t(), map()) ::
          {:ok, map()} | {:error, term()}
  def cancel_work(tenant_id, work_object_id, operator_ref, payload \\ %{})
      when is_binary(tenant_id) and is_binary(work_object_id) and is_binary(operator_ref) and
             is_map(payload) do
    with {:ok, work_object} <- fetch_work_object(tenant_id, work_object_id),
         {:ok, control_session} <-
           Mezzanine.WorkControl.ensure_control_session(tenant_id, work_object),
         {:ok, intervention} <-
           record_intervention(tenant_id, control_session.id, operator_ref, :cancel, payload),
         :ok <- cancel_active_run(tenant_id, work_object_id),
         {:ok, cancelled_work} <- mark_work_terminal(work_object, tenant_id, :cancelled),
         {:ok, _audit} <-
           WorkAudit.record_event(tenant_id, %{
             program_id: work_object.program_id,
             work_object_id: work_object.id,
             event_kind: :operator_cancelled,
             actor_kind: :human,
             actor_ref: operator_ref,
             payload: Map.merge(payload, %{control_session_id: control_session.id})
           }) do
      {:ok, %{work_object: cancelled_work, intervention: intervention}}
    end
  end

  @spec request_replan(String.t(), Ecto.UUID.t(), String.t(), map()) ::
          {:ok, map()} | {:error, term()}
  def request_replan(tenant_id, work_object_id, operator_ref, payload \\ %{})
      when is_binary(tenant_id) and is_binary(work_object_id) and is_binary(operator_ref) and
             is_map(payload) do
    with {:ok, work_object} <- fetch_work_object(tenant_id, work_object_id),
         {:ok, control_session} <-
           Mezzanine.WorkControl.ensure_control_session(tenant_id, work_object),
         {:ok, intervention} <-
           record_intervention(tenant_id, control_session.id, operator_ref, :replan, payload),
         {:ok, prior_plan} <- maybe_supersede_current_plan(tenant_id, work_object.current_plan_id),
         {:ok, replanned_work} <- compile_plan(work_object, tenant_id, prior_plan),
         {:ok, _audit} <-
           WorkAudit.record_event(tenant_id, %{
             program_id: work_object.program_id,
             work_object_id: work_object.id,
             event_kind: :replan_requested,
             actor_kind: :human,
             actor_ref: operator_ref,
             payload: Map.merge(payload, %{control_session_id: control_session.id})
           }) do
      {:ok, %{work_object: replanned_work, intervention: intervention, prior_plan: prior_plan}}
    end
  end

  @spec override_grant_profile(String.t(), Ecto.UUID.t(), String.t(), map()) ::
          {:ok, map()} | {:error, term()}
  def override_grant_profile(tenant_id, work_object_id, operator_ref, override_set)
      when is_binary(tenant_id) and is_binary(work_object_id) and is_binary(operator_ref) and
             is_map(override_set) do
    normalized_override_set = normalize_override_set(override_set)

    with {:ok, work_object} <- fetch_work_object(tenant_id, work_object_id),
         {:ok, control_session} <-
           Mezzanine.WorkControl.ensure_control_session(tenant_id, work_object),
         {:ok, updated_session} <-
           update_session(
             control_session,
             tenant_id,
             :apply_grant_override,
             %{active_override_set: normalized_override_set}
           ),
         {:ok, intervention} <-
           record_intervention(
             tenant_id,
             updated_session.id,
             operator_ref,
             :grant_override,
             normalized_override_set
           ),
         {:ok, _audit} <-
           WorkAudit.record_event(tenant_id, %{
             program_id: work_object.program_id,
             work_object_id: work_object.id,
             event_kind: :grant_override_applied,
             actor_kind: :human,
             actor_ref: operator_ref,
             payload: %{
               control_session_id: updated_session.id,
               active_override_set: normalized_override_set
             }
           }) do
      {:ok, %{control_session: updated_session, intervention: intervention}}
    end
  end

  defp update_session(session, tenant_id, action, attrs) do
    session
    |> Ash.Changeset.for_update(action, attrs)
    |> Ash.Changeset.set_tenant(tenant_id)
    |> Ash.update(actor: actor(tenant_id), authorize?: false, domain: Mezzanine.Control)
  end

  defp record_intervention(
         tenant_id,
         control_session_id,
         operator_ref,
         intervention_kind,
         payload
       ) do
    OperatorIntervention
    |> Ash.Changeset.for_create(:record_intervention, %{
      control_session_id: control_session_id,
      operator_ref: operator_ref,
      intervention_kind: intervention_kind,
      payload: payload,
      occurred_at: DateTime.utc_now()
    })
    |> Ash.Changeset.set_tenant(tenant_id)
    |> Ash.create(actor: actor(tenant_id), authorize?: false, domain: Mezzanine.Control)
  end

  defp normalize_override_set(override_set) do
    Map.new(override_set, fn {capability_id, grant_mode} ->
      {to_string(capability_id), normalize_override_value(grant_mode)}
    end)
  end

  defp normalize_override_value(grant_mode) when is_atom(grant_mode),
    do: Atom.to_string(grant_mode)

  defp normalize_override_value(grant_mode), do: grant_mode

  defp maybe_supersede_current_plan(_tenant_id, nil), do: {:ok, nil}

  defp maybe_supersede_current_plan(tenant_id, plan_id) do
    case fetch_work_plan(tenant_id, plan_id) do
      {:ok, plan} ->
        plan
        |> Ash.Changeset.for_update(:supersede, %{})
        |> Ash.Changeset.set_tenant(tenant_id)
        |> Ash.update(actor: actor(tenant_id), authorize?: false, domain: Mezzanine.Work)

      {:error, :not_found} ->
        {:ok, nil}

      {:error, reason} ->
        {:error, reason}
    end
  end

  defp compile_plan(work_object, tenant_id, nil) do
    work_object
    |> Ash.Changeset.for_update(:compile_plan, %{})
    |> Ash.Changeset.set_tenant(tenant_id)
    |> Ash.update(actor: actor(tenant_id), authorize?: false, domain: Mezzanine.Work)
  end

  defp compile_plan(work_object, tenant_id, prior_plan) do
    work_object
    |> Ash.Changeset.for_update(:compile_plan, %{policy_bundle_id: prior_plan.policy_bundle_id})
    |> Ash.Changeset.set_tenant(tenant_id)
    |> Ash.update(actor: actor(tenant_id), authorize?: false, domain: Mezzanine.Work)
  end

  defp cancel_active_run(tenant_id, work_object_id) do
    case current_run_for_work(tenant_id, work_object_id) do
      {:ok, %{run: run, series: series}} ->
        with {:ok, _} <-
               run
               |> Ash.Changeset.for_update(:record_cancelled, %{
                 completed_at: DateTime.utc_now(),
                 result_summary: "Cancelled by operator"
               })
               |> Ash.Changeset.set_tenant(tenant_id)
               |> Ash.update(actor: actor(tenant_id), authorize?: false, domain: Mezzanine.Runs),
             {:ok, _} <-
               series
               |> Ash.Changeset.for_update(:close_series, %{status: :cancelled})
               |> Ash.Changeset.set_tenant(tenant_id)
               |> Ash.update(actor: actor(tenant_id), authorize?: false, domain: Mezzanine.Runs) do
          :ok
        end

      {:error, :not_found} ->
        :ok

      {:error, reason} ->
        {:error, reason}
    end
  end

  defp mark_work_terminal(work_object, tenant_id, status) do
    work_object
    |> Ash.Changeset.for_update(:mark_terminal, %{status: status})
    |> Ash.Changeset.set_tenant(tenant_id)
    |> Ash.update(actor: actor(tenant_id), authorize?: false, domain: Mezzanine.Work)
  end

  defp current_run_for_work(tenant_id, work_object_id) do
    case list_run_series_for_work(tenant_id, work_object_id) do
      {:ok, [series | _]} when is_binary(series.current_run_id) ->
        case fetch_run(tenant_id, series.current_run_id) do
          {:ok, run} -> {:ok, %{series: series, run: run}}
          error -> error
        end

      {:ok, [_series | _]} ->
        {:error, :not_found}

      {:ok, []} ->
        {:error, :not_found}

      {:error, error} ->
        {:error, error}
    end
  end

  defp list_run_series_for_work(tenant_id, work_object_id) do
    RunSeries
    |> Ash.Query.set_tenant(tenant_id)
    |> Ash.Query.filter(work_object_id == ^work_object_id)
    |> Ash.read(actor: actor(tenant_id), domain: Mezzanine.Runs)
  end

  defp fetch_work_object(tenant_id, work_object_id) do
    WorkObject
    |> Ash.Query.set_tenant(tenant_id)
    |> Ash.Query.filter(id == ^work_object_id)
    |> Ash.read(actor: actor(tenant_id), domain: Mezzanine.Work)
    |> case do
      {:ok, [work_object]} -> {:ok, work_object}
      {:ok, []} -> {:error, :not_found}
      {:error, error} -> {:error, error}
    end
  end

  defp fetch_work_plan(_tenant_id, nil), do: {:error, :not_found}

  defp fetch_work_plan(tenant_id, plan_id) do
    WorkPlan
    |> Ash.Query.set_tenant(tenant_id)
    |> Ash.Query.filter(id == ^plan_id)
    |> Ash.read(actor: actor(tenant_id), domain: Mezzanine.Work)
    |> case do
      {:ok, [plan]} -> {:ok, plan}
      {:ok, []} -> {:error, :not_found}
      {:error, error} -> {:error, error}
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
      {:error, error} -> {:error, error}
    end
  end

  defp actor(tenant_id), do: %{tenant_id: tenant_id}
end
