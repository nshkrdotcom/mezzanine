defmodule Mezzanine.AppKitBridge.ReviewActionService do
  @moduledoc """
  Backend-oriented review decisions for AppKit consumers.

  The service supports both generic review-queue decisions and the transitional
  `review_run/3` compatibility path used by the current operator bridge.
  """

  alias AppKit.Core.RunRef
  alias AppKit.RunGovernance
  alias Mezzanine.Assurance

  @supported_decisions [:accept, :reject, :waive, :escalate]

  @spec record_decision(String.t(), Ecto.UUID.t(), map(), keyword()) ::
          {:ok, map()} | {:error, term()}
  def record_decision(tenant_id, review_unit_id, attrs, opts \\ [])
      when is_binary(tenant_id) and is_binary(review_unit_id) and is_map(attrs) and
             is_list(opts) do
    attrs = Map.new(attrs)

    with {:ok, program_id} <- fetch_string(attrs, opts, :program_id),
         {:ok, decision} <- normalize_decision(map_value(attrs, :decision)),
         {:ok, bridge_result} <-
           dispatch_decision(tenant_id, review_unit_id, decision, attrs, opts, program_id) do
      {:ok,
       %{
         status: :completed,
         action_ref: action_ref(review_unit_id, decision, bridge_result),
         message: action_message(decision),
         metadata: normalize_value(bridge_result)
       }}
    else
      {:error, reason} -> {:error, normalize_error(reason)}
    end
  end

  @spec record_run_review(RunRef.t(), map(), keyword()) :: {:ok, map()} | {:error, atom()}
  def record_run_review(%RunRef{} = run_ref, evidence_attrs, opts \\ [])
      when is_map(evidence_attrs) and is_list(opts) do
    with {:ok, tenant_id} <- fetch_tenant_id(run_ref, evidence_attrs, opts),
         {:ok, review_unit_id} <- fetch_review_unit_id(run_ref, evidence_attrs),
         {:ok, evidence} <- RunGovernance.evidence(evidence_attrs),
         state <- RunGovernance.review_state(evidence, opts),
         {:ok, decision} <- build_decision(run_ref, state, opts),
         {:ok, result} <-
           record_decision(
             tenant_id,
             review_unit_id,
             %{
               program_id: fetch_program_id!(run_ref, evidence_attrs, opts),
               decision: decision_to_assurance(state),
               actor_ref: actor_ref(evidence_attrs, opts),
               reason: Keyword.get(opts, :reason),
               payload: %{
                 summary: evidence.summary,
                 details: evidence.details
               }
             },
             opts
           ) do
      {:ok,
       %{
         status: result.status,
         action_ref: result.action_ref,
         message: result.message,
         metadata:
           Map.merge(result.metadata, %{
             decision: decision,
             bridge_result: result.metadata
           })
       }}
    else
      {:error, reason} -> {:error, normalize_error(reason)}
    end
  end

  defp dispatch_decision(tenant_id, review_unit_id, :accept, attrs, opts, program_id) do
    Assurance.record_decision(tenant_id, review_unit_id, %{
      program_id: program_id,
      decision: :accept,
      actor_kind: :human,
      actor_ref: actor_ref(attrs, opts),
      reason: map_value(attrs, :reason),
      payload: map_value(attrs, :payload) || %{}
    })
  end

  defp dispatch_decision(tenant_id, review_unit_id, :reject, attrs, opts, program_id) do
    Assurance.record_decision(tenant_id, review_unit_id, %{
      program_id: program_id,
      decision: :reject,
      actor_kind: :human,
      actor_ref: actor_ref(attrs, opts),
      reason: map_value(attrs, :reason),
      payload: map_value(attrs, :payload) || %{}
    })
  end

  defp dispatch_decision(tenant_id, review_unit_id, :waive, attrs, opts, program_id) do
    Assurance.waive_review(tenant_id, review_unit_id, %{
      program_id: program_id,
      actor_ref: actor_ref(attrs, opts),
      reason: map_value(attrs, :reason) || "waived by operator",
      expires_at: map_value(attrs, :expires_at),
      conditions: map_value(attrs, :conditions) || []
    })
  end

  defp dispatch_decision(tenant_id, review_unit_id, :escalate, attrs, opts, program_id) do
    Assurance.escalate_review(tenant_id, review_unit_id, %{
      program_id: program_id,
      actor_ref: actor_ref(attrs, opts),
      reason: map_value(attrs, :reason),
      assigned_to: map_value(attrs, :assigned_to),
      priority: map_value(attrs, :priority) || :normal
    })
  end

  defp normalize_decision(decision) when decision in @supported_decisions, do: {:ok, decision}

  defp normalize_decision(decision) when is_binary(decision) do
    case Enum.find(@supported_decisions, &(Atom.to_string(&1) == decision)) do
      nil -> {:error, :unsupported_decision}
      normalized -> {:ok, normalized}
    end
  end

  defp normalize_decision(_decision), do: {:error, :unsupported_decision}

  defp fetch_tenant_id(run_ref, attrs, opts) do
    case Keyword.get(opts, :tenant_id) || map_value(attrs, :tenant_id) ||
           Map.get(run_ref.metadata, :tenant_id) || Map.get(run_ref.metadata, "tenant_id") do
      value when is_binary(value) -> {:ok, value}
      _ -> {:error, :missing_tenant_id}
    end
  end

  defp fetch_review_unit_id(run_ref, attrs) do
    case map_value(attrs, :review_unit_id) || Map.get(run_ref.metadata, :review_unit_id) ||
           Map.get(run_ref.metadata, "review_unit_id") do
      value when is_binary(value) -> {:ok, value}
      _ -> {:error, :missing_review_unit_id}
    end
  end

  defp fetch_program_id!(run_ref, attrs, opts) do
    case fetch_string(attrs, opts, :program_id) do
      {:ok, program_id} ->
        program_id

      {:error, _reason} ->
        case Map.get(run_ref.metadata, :program_id) || Map.get(run_ref.metadata, "program_id") do
          value when is_binary(value) -> value
          _ -> raise ArgumentError, "missing program_id for run review"
        end
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

  defp action_ref(review_unit_id, decision, bridge_result) do
    work_object_id = bridge_result.review_unit.work_object_id

    %{
      id: "#{review_unit_id}:#{decision}",
      action_kind: action_kind(decision),
      subject_ref:
        if(is_binary(work_object_id),
          do: %{id: work_object_id, subject_kind: "work_object"},
          else: nil
        )
    }
  end

  defp action_kind(:accept), do: "review_accept"
  defp action_kind(:reject), do: "review_reject"
  defp action_kind(:waive), do: "review_waive"
  defp action_kind(:escalate), do: "review_escalate"

  defp action_message(:accept), do: "Review accepted"
  defp action_message(:reject), do: "Review rejected"
  defp action_message(:waive), do: "Review waived"
  defp action_message(:escalate), do: "Review escalated"

  defp fetch_string(attrs, opts, key) do
    case Keyword.get(opts, key) || map_value(attrs, key) do
      value when is_binary(value) and value != "" -> {:ok, value}
      _ -> {:error, {:missing_required_field, key}}
    end
  end

  defp map_value(attrs, key), do: Map.get(attrs, key) || Map.get(attrs, Atom.to_string(key))

  defp actor_ref(attrs, opts) do
    Keyword.get(opts, :actor_ref) || map_value(attrs, :actor_ref) || map_value(attrs, :id) ||
      "operator"
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
  defp normalize_error({:missing_required_field, _field}), do: :bridge_failed
  defp normalize_error(reason) when is_atom(reason), do: reason
  defp normalize_error(_reason), do: :bridge_failed
end
