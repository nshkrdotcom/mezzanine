defmodule Mezzanine.AppKitBridge.OperatorActionService do
  @moduledoc """
  Backend-oriented operator actions and review-decision writes.
  """

  alias AppKit.Core.RunRef
  alias AppKit.RunGovernance
  alias Mezzanine.Assurance
  alias Mezzanine.Control.Commands

  @supported_actions [:pause, :resume, :cancel, :replan, :grant_override]

  @spec apply_action(String.t(), Ecto.UUID.t(), atom() | String.t(), map(), map()) ::
          {:ok, map()} | {:error, term()}
  def apply_action(tenant_id, subject_id, action, params, actor)
      when is_binary(tenant_id) and is_binary(subject_id) and is_map(params) and is_map(actor) do
    with {:ok, action} <- normalize_action(action),
         {:ok, bridge_result} <- dispatch_action(action, tenant_id, subject_id, params, actor) do
      {:ok,
       %{
         status: :completed,
         action_ref: action_ref(subject_id, action),
         message: action_message(action),
         metadata: normalize_value(bridge_result)
       }}
    end
  end

  @spec review_run(RunRef.t(), map(), keyword()) :: {:ok, map()} | {:error, atom()}
  def review_run(%RunRef{} = run_ref, evidence_attrs, opts \\ [])
      when is_map(evidence_attrs) and is_list(opts) do
    with {:ok, result} <- record_review_decision(run_ref, evidence_attrs, opts) do
      {:ok,
       %{
         decision: result.metadata.decision,
         review_unit: result.metadata.review_unit,
         bridge_result: result.metadata.bridge_result
       }}
    end
  end

  @spec record_review_decision(RunRef.t(), map(), keyword()) :: {:ok, map()} | {:error, atom()}
  def record_review_decision(%RunRef{} = run_ref, evidence_attrs, opts \\ [])
      when is_map(evidence_attrs) and is_list(opts) do
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
       %{
         status: :completed,
         action_ref: review_action_ref(run_ref, state),
         message: review_message(state),
         metadata: %{
           decision: decision,
           review_unit: bridge_result.review_unit,
           bridge_result: bridge_result
         }
       }}
    else
      {:error, reason} -> {:error, normalize_error(reason)}
    end
  end

  defp dispatch_action(:pause, tenant_id, subject_id, params, actor) do
    Commands.pause_work(tenant_id, subject_id, actor_ref(actor, []), params)
  end

  defp dispatch_action(:resume, tenant_id, subject_id, params, actor) do
    Commands.resume_work(tenant_id, subject_id, actor_ref(actor, []), params)
  end

  defp dispatch_action(:cancel, tenant_id, subject_id, params, actor) do
    Commands.cancel_work(tenant_id, subject_id, actor_ref(actor, []), params)
  end

  defp dispatch_action(:replan, tenant_id, subject_id, params, actor) do
    Commands.request_replan(tenant_id, subject_id, actor_ref(actor, []), params)
  end

  defp dispatch_action(:grant_override, tenant_id, subject_id, params, actor) do
    Commands.override_grant_profile(
      tenant_id,
      subject_id,
      actor_ref(actor, []),
      grant_override_payload(params)
    )
  end

  defp normalize_action(action) when action in @supported_actions, do: {:ok, action}

  defp normalize_action(action) when is_binary(action) do
    case Enum.find(@supported_actions, &(Atom.to_string(&1) == action)) do
      nil -> {:error, :unsupported_action}
      normalized -> {:ok, normalized}
    end
  end

  defp normalize_action(_action), do: {:error, :unsupported_action}

  defp grant_override_payload(params) do
    Map.get(params, :grant_overrides) || Map.get(params, "grant_overrides") ||
      Map.get(params, :active_override_set) || Map.get(params, "active_override_set") || params
  end

  defp fetch_tenant_id(run_ref, attrs, opts) do
    case Keyword.get(opts, :tenant_id) || Map.get(attrs, :tenant_id) ||
           Map.get(attrs, "tenant_id") ||
           Map.get(run_ref.metadata, :tenant_id) || Map.get(run_ref.metadata, "tenant_id") do
      value when is_binary(value) -> {:ok, value}
      _ -> {:error, :missing_tenant_id}
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

  defp review_action_ref(run_ref, state) do
    work_object_id =
      Map.get(run_ref.metadata, :work_object_id) || Map.get(run_ref.metadata, "work_object_id")

    %{
      id: "#{work_object_id || run_ref.run_id}:review:#{state}",
      action_kind: review_action_kind(state),
      subject_ref:
        if(is_binary(work_object_id),
          do: %{id: work_object_id, subject_kind: "work_object"},
          else: nil
        )
    }
  end

  defp review_action_kind(:approved), do: "review_accept"
  defp review_action_kind(:needs_changes), do: "review_reject"

  defp review_message(:approved), do: "Review accepted"
  defp review_message(:needs_changes), do: "Review rejected"

  defp action_ref(subject_id, action) do
    %{
      id: "#{subject_id}:#{action}",
      action_kind: Atom.to_string(action),
      subject_ref: %{id: subject_id, subject_kind: "work_object"}
    }
  end

  defp action_message(:pause), do: "Work paused"
  defp action_message(:resume), do: "Work resumed"
  defp action_message(:cancel), do: "Work cancelled"
  defp action_message(:replan), do: "Replan requested"
  defp action_message(:grant_override), do: "Grant override applied"

  defp actor_ref(attrs, opts) do
    Keyword.get(opts, :actor_ref) || Map.get(attrs, :actor_ref) || Map.get(attrs, "actor_ref") ||
      Map.get(attrs, :id) || Map.get(attrs, "id") || "operator"
  end

  defp normalize_value(%DateTime{} = value), do: value
  defp normalize_value(%NaiveDateTime{} = value), do: value
  defp normalize_value(%_{} = value), do: value |> Map.from_struct() |> normalize_value()

  defp normalize_value(value) when is_map(value) do
    Map.new(value, fn {key, nested_value} -> {key, normalize_value(nested_value)} end)
  end

  defp normalize_value(value) when is_list(value), do: Enum.map(value, &normalize_value/1)
  defp normalize_value(value), do: value

  defp normalize_error(:not_found), do: :bridge_not_found
  defp normalize_error(reason) when is_atom(reason), do: reason
  defp normalize_error(_reason), do: :bridge_failed
end
