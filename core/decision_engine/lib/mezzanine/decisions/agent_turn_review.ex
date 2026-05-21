defmodule Mezzanine.Decisions.AgentTurnReview do
  @moduledoc """
  DecisionEngine adapter for agent-turn pending interactions.

  Agent turn state remains in `Mezzanine.AgentTurnEngine`; this module owns the
  durable DecisionEngine boundary for opening and resolving review records.
  """

  alias Mezzanine.AgentTurnEngine.{AgentPendingInteraction, PendingDecision}
  alias Mezzanine.DecisionCommands
  alias Mezzanine.Decisions.DecisionRecord

  @approved_values ["accept", "accepted", "allow", "approved", "approve"]
  @denied_values ["deny", "denied", "reject", "rejected"]
  @expired_values ["expire", "expired"]
  @cancelled_values ["cancel", "cancelled", "canceled"]

  @spec create_pending_interaction(AgentPendingInteraction.t(), map()) ::
          {:ok, DecisionRecord.t()} | {:error, term()}
  def create_pending_interaction(%AgentPendingInteraction{} = pending, attrs)
      when is_map(attrs) do
    DecisionCommands.create_pending(%{
      installation_id: fetch_required!(attrs, :installation_id),
      subject_id: fetch_required!(attrs, :subject_id),
      execution_id: map_value(attrs, :execution_id),
      decision_kind: Atom.to_string(pending.kind),
      required_by: pending.expires_at,
      trace_id: fetch_required!(attrs, :trace_id),
      causation_id: fetch_required!(attrs, :causation_id),
      actor_ref: actor_ref(attrs, pending)
    })
  end

  @spec to_pending_decision(DecisionRecord.t(), AgentPendingInteraction.t(), map()) ::
          {:ok, PendingDecision.t()} | {:error, term()}
  def to_pending_decision(
        %DecisionRecord{} = decision,
        %AgentPendingInteraction{} = pending,
        attrs
      )
      when is_map(attrs) do
    with {:ok, decision_value} <- pending_decision_value(decision),
         {:ok, decided_at} <- decided_at(decision, attrs) do
      PendingDecision.new(%{
        decision_ref: decision_ref(decision),
        pending_ref: pending.pending_ref,
        tenant_ref: pending.tenant_ref,
        actor_ref: pending.actor_ref,
        authority_ref: pending.authority_ref,
        authority_revision_ref: authority_revision_ref(attrs, decision),
        decision: decision_value,
        idempotency_key: idempotency_key(attrs, decision),
        decided_at: decided_at
      })
    end
  end

  @spec terminal_action(PendingDecision.t()) :: :accept | :reject | :expire
  def terminal_action(%PendingDecision{decision: :approved}), do: :accept
  def terminal_action(%PendingDecision{decision: :denied}), do: :reject
  def terminal_action(%PendingDecision{decision: :expired}), do: :expire
  def terminal_action(%PendingDecision{decision: :cancelled}), do: :reject

  @spec decision_ref(DecisionRecord.t()) :: String.t()
  def decision_ref(%DecisionRecord{id: id}), do: "decision://" <> to_string(id)

  defp pending_decision_value(%DecisionRecord{decision_value: value}) when is_binary(value) do
    normalized = value |> String.trim() |> String.downcase()

    cond do
      normalized in @approved_values -> {:ok, :approved}
      normalized in @denied_values -> {:ok, :denied}
      normalized in @expired_values -> {:ok, :expired}
      normalized in @cancelled_values -> {:ok, :cancelled}
      true -> {:error, {:invalid, :decision_value, :unsupported}}
    end
  end

  defp pending_decision_value(_decision), do: {:error, {:invalid, :decision_value, :required}}

  defp decided_at(%DecisionRecord{resolved_at: %DateTime{} = resolved_at}, _attrs),
    do: {:ok, resolved_at}

  defp decided_at(_decision, attrs) do
    case map_value(attrs, :decided_at) do
      %DateTime{} = decided_at -> {:ok, decided_at}
      _other -> {:error, {:invalid, :decided_at, :required}}
    end
  end

  defp authority_revision_ref(attrs, %DecisionRecord{} = decision) do
    map_value(attrs, :authority_revision_ref) ||
      "authority-revision://decision/#{decision.id}/#{decision.row_version}"
  end

  defp idempotency_key(attrs, %DecisionRecord{} = decision) do
    map_value(attrs, :idempotency_key) || "decision-record:#{decision.id}:#{decision.row_version}"
  end

  defp actor_ref(attrs, %AgentPendingInteraction{} = pending) do
    map_value(attrs, :actor_ref) || %{"actor_ref" => pending.actor_ref}
  end

  defp fetch_required!(attrs, key) do
    case map_value(attrs, key) do
      nil -> raise ArgumentError, "missing required #{key}"
      value -> value
    end
  end

  defp map_value(attrs, key) when is_atom(key),
    do: Map.get(attrs, key, Map.get(attrs, Atom.to_string(key)))
end
