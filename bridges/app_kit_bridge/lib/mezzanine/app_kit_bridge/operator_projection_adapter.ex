defmodule Mezzanine.AppKitBridge.OperatorProjectionAdapter do
  @moduledoc """
  `AppKit.OperatorSurface` backend implemented against Mezzanine audit and
  assurance services.
  """

  @behaviour AppKit.Core.Backends.OperatorBackend

  alias AppKit.Core.RunRef
  alias AppKit.RunGovernance
  alias Mezzanine.{Assurance, Audit}

  @doc """
  Projects operator-facing run status using Mezzanine audit and assurance
  records for the work associated with the given AppKit run reference.
  """
  @impl true
  def run_status(%RunRef{} = run_ref, attrs, opts) when is_map(attrs) do
    with {:ok, tenant_id} <- fetch_tenant_id(run_ref, attrs, opts),
         {:ok, work_object_id} <- fetch_work_object_id(run_ref, attrs),
         {:ok, audit_report} <- Audit.work_report(tenant_id, work_object_id),
         {:ok, gate_status} <- Assurance.gate_status(tenant_id, work_object_id) do
      {:ok,
       %{
         run_ref: run_ref,
         work_object_id: work_object_id,
         timeline: audit_report.timeline,
         audit_events: audit_report.audit_events,
         evidence_bundles: audit_report.evidence_bundles,
         gate_status: gate_status
       }}
    else
      {:error, reason} -> {:error, normalize_error(reason)}
    end
  end

  @doc """
  Records a review decision for the Mezzanine review unit associated with the
  given AppKit run reference.
  """
  @impl true
  def review_run(%RunRef{} = run_ref, evidence_attrs, opts) when is_map(evidence_attrs) do
    with {:ok, tenant_id} <- fetch_tenant_id(run_ref, evidence_attrs, opts),
         {:ok, review_unit_id} <- fetch_review_unit_id(run_ref, evidence_attrs),
         {:ok, program_id} <- fetch_program_id(run_ref, evidence_attrs, opts),
         {:ok, evidence} <- RunGovernance.evidence(evidence_attrs),
         state <- RunGovernance.review_state(evidence, opts),
         {:ok, decision} <- build_decision(run_ref, state, opts),
         {:ok, bridge_result} <-
           Assurance.record_decision(tenant_id, review_unit_id, %{
             program_id: program_id,
             decision: decision_to_assurance(state),
             actor_kind: :human,
             actor_ref: actor_ref(evidence_attrs, opts),
             reason: Keyword.get(opts, :reason),
             payload: %{
               summary: evidence.summary,
               details: evidence.details
             }
           }) do
      {:ok,
       %{decision: decision, review_unit: bridge_result.review_unit, bridge_result: bridge_result}}
    else
      {:error, reason} -> {:error, normalize_error(reason)}
    end
  end

  defp fetch_tenant_id(run_ref, attrs, opts) do
    case Keyword.get(opts, :tenant_id) || Map.get(attrs, :tenant_id) ||
           Map.get(attrs, "tenant_id") ||
           Map.get(run_ref.metadata, :tenant_id) || Map.get(run_ref.metadata, "tenant_id") do
      value when is_binary(value) -> {:ok, value}
      _ -> {:error, :missing_tenant_id}
    end
  end

  defp fetch_work_object_id(run_ref, attrs) do
    case Map.get(attrs, :work_object_id) || Map.get(attrs, "work_object_id") ||
           Map.get(run_ref.metadata, :work_object_id) ||
           Map.get(run_ref.metadata, "work_object_id") do
      value when is_binary(value) -> {:ok, value}
      _ -> {:error, :missing_work_object_id}
    end
  end

  defp fetch_review_unit_id(run_ref, attrs) do
    case Map.get(attrs, :review_unit_id) || Map.get(attrs, "review_unit_id") ||
           Map.get(run_ref.metadata, :review_unit_id) ||
           Map.get(run_ref.metadata, "review_unit_id") do
      value when is_binary(value) -> {:ok, value}
      _ -> {:error, :missing_review_unit_id}
    end
  end

  defp fetch_program_id(run_ref, attrs, opts) do
    case Keyword.get(opts, :program_id) || Map.get(attrs, :program_id) ||
           Map.get(attrs, "program_id") ||
           Map.get(run_ref.metadata, :program_id) || Map.get(run_ref.metadata, "program_id") do
      value when is_binary(value) -> {:ok, value}
      _ -> {:error, :missing_program_id}
    end
  end

  defp build_decision(run_ref, state, opts) do
    RunGovernance.decision(%{
      run_id: run_ref.run_id,
      state: state,
      reason: Keyword.get(opts, :reason)
    })
  end

  defp decision_to_assurance(:approved), do: :accept
  defp decision_to_assurance(:needs_changes), do: :reject

  defp actor_ref(attrs, opts) do
    Keyword.get(opts, :actor_ref) || Map.get(attrs, :actor_ref) || Map.get(attrs, "actor_ref") ||
      "operator"
  end

  defp normalize_error(:not_found), do: :bridge_not_found
  defp normalize_error(reason) when is_atom(reason), do: reason
  defp normalize_error(_reason), do: :bridge_failed
end
