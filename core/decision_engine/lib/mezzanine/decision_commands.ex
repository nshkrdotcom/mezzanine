defmodule Mezzanine.DecisionCommands do
  @moduledoc """
  Decision mutation facade over the decision-ledger owner actions.

  The command module intentionally does not own SQL over `decision_records` or
  `audit_facts`. Terminal transitions use the `DecisionRecord` Ash owner actions
  and their optimistic row-version lock.
  """

  alias Mezzanine.Audit.{AuditAppend, AuditQuery}
  alias Mezzanine.Decisions.DecisionRecord

  require Ash.Query

  @terminal_actions [:decide, :waive, :expire, :accept, :reject, :escalate]
  @conflict_outcomes [
    :duplicate_same_decision,
    :conflict_rejected,
    :stale_expiry,
    :stale_row_version,
    :unsupported_transition,
    :unique_constraint_conflict,
    :lock_conflict,
    :optimistic_lock_conflict
  ]

  @spec create_pending(map()) :: {:ok, DecisionRecord.t()} | {:error, term()}
  def create_pending(attrs) when is_map(attrs) do
    DecisionRecord.create_pending(%{
      installation_id: fetch_required!(attrs, :installation_id),
      subject_id: fetch_required!(attrs, :subject_id),
      execution_id: map_value(attrs, :execution_id),
      decision_kind: fetch_required!(attrs, :decision_kind),
      required_by: map_value(attrs, :required_by),
      trace_id: fetch_required!(attrs, :trace_id),
      causation_id: fetch_required!(attrs, :causation_id),
      actor_ref: normalize_map(fetch_required!(attrs, :actor_ref))
    })
  end

  @spec fetch_by_identity(map()) :: {:ok, DecisionRecord.t() | nil} | {:error, term()}
  def fetch_by_identity(attrs) when is_map(attrs) do
    installation_id = fetch_required!(attrs, :installation_id)
    subject_id = fetch_required!(attrs, :subject_id)
    execution_id = map_value(attrs, :execution_id)
    decision_kind = fetch_required!(attrs, :decision_kind)

    result =
      if is_nil(execution_id) do
        DecisionRecord.by_identity_without_execution(installation_id, subject_id, decision_kind)
      else
        DecisionRecord.by_identity(installation_id, subject_id, execution_id, decision_kind)
      end

    case result do
      {:ok, []} -> {:ok, nil}
      {:ok, [decision]} -> {:ok, decision}
      {:error, error} -> {:error, error}
    end
  end

  @spec decide(DecisionRecord.t() | String.t(), map()) ::
          {:ok, DecisionRecord.t()} | {:error, term()}
  def decide(decision_or_id, attrs) when is_map(attrs) do
    resolve_terminal(decision_or_id, :decide, attrs)
  end

  @spec accept(DecisionRecord.t() | String.t(), map()) ::
          {:ok, DecisionRecord.t()} | {:error, term()}
  def accept(decision_or_id, attrs) when is_map(attrs) do
    resolve_terminal(decision_or_id, :accept, attrs)
  end

  @spec reject(DecisionRecord.t() | String.t(), map()) ::
          {:ok, DecisionRecord.t()} | {:error, term()}
  def reject(decision_or_id, attrs) when is_map(attrs) do
    resolve_terminal(decision_or_id, :reject, attrs)
  end

  @spec waive(DecisionRecord.t() | String.t(), map()) ::
          {:ok, DecisionRecord.t()} | {:error, term()}
  def waive(decision_or_id, attrs) when is_map(attrs) do
    resolve_terminal(decision_or_id, :waive, attrs)
  end

  @spec escalate(DecisionRecord.t() | String.t(), map()) ::
          {:ok, DecisionRecord.t()} | {:error, term()}
  def escalate(decision_or_id, attrs) when is_map(attrs) do
    resolve_terminal(decision_or_id, :escalate, attrs)
  end

  @spec expire(DecisionRecord.t() | String.t(), map(), keyword()) ::
          {:ok, DecisionRecord.t()} | {:error, term()}
  def expire(decision_or_id, attrs, opts \\ []) when is_map(attrs) and is_list(opts) do
    resolve_terminal(decision_or_id, :expire, attrs, opts)
  end

  @spec resolve_terminal(DecisionRecord.t() | String.t(), atom() | String.t(), map(), keyword()) ::
          {:ok, DecisionRecord.t()} | {:error, term()}
  def resolve_terminal(decision_or_id, action, attrs, opts \\ [])
      when is_map(attrs) and is_list(opts) do
    action = normalize_terminal_action!(action)
    current_job_id = Keyword.get(opts, :current_job_id)

    with {:ok, decision} <- load_current_decision(decision_or_id),
         :ok <- ensure_pending_or_record_conflict(decision, action, attrs),
         :ok <- ensure_expected_row_version_or_record_conflict(decision, action, attrs),
         :ok <- reject_legacy_expiry_job_ref(decision, action, current_job_id) do
      decision
      |> apply_terminal_action(action, attrs)
      |> record_terminal_attempt(decision, action, attrs)
    end
  end

  defp normalize_terminal_action!(action) when is_atom(action) and action in @terminal_actions,
    do: action

  defp normalize_terminal_action!(action) when is_binary(action) do
    case String.downcase(String.trim(action)) do
      "decide" -> :decide
      "waive" -> :waive
      "expire" -> :expire
      "accept" -> :accept
      "reject" -> :reject
      "escalate" -> :escalate
      other -> raise(ArgumentError, "unsupported decision terminal action #{inspect(other)}")
    end
  end

  defp normalize_terminal_action!(action),
    do: raise(ArgumentError, "unsupported decision terminal action #{inspect(action)}")

  defp apply_terminal_action(%DecisionRecord{} = decision, action, attrs)
       when action in [:decide, :accept, :reject] do
    DecisionRecord.decide(decision, %{
      decision_value: requested_decision(action, attrs),
      reason: map_value(attrs, :reason),
      trace_id: fetch_required!(attrs, :trace_id),
      causation_id: fetch_required!(attrs, :causation_id),
      actor_ref: normalize_map(fetch_required!(attrs, :actor_ref))
    })
  end

  defp apply_terminal_action(%DecisionRecord{} = decision, :waive, attrs) do
    DecisionRecord.waive(decision, %{
      reason: map_value(attrs, :reason),
      trace_id: fetch_required!(attrs, :trace_id),
      causation_id: fetch_required!(attrs, :causation_id),
      actor_ref: normalize_map(fetch_required!(attrs, :actor_ref))
    })
  end

  defp apply_terminal_action(%DecisionRecord{} = decision, :expire, attrs) do
    DecisionRecord.expire(decision, %{
      reason: map_value(attrs, :reason),
      trace_id: fetch_required!(attrs, :trace_id),
      causation_id: fetch_required!(attrs, :causation_id),
      actor_ref: normalize_map(fetch_required!(attrs, :actor_ref))
    })
  end

  defp apply_terminal_action(%DecisionRecord{} = decision, :escalate, attrs) do
    DecisionRecord.escalate(decision, %{
      reason: fetch_required!(attrs, :reason),
      trace_id: fetch_required!(attrs, :trace_id),
      causation_id: fetch_required!(attrs, :causation_id),
      actor_ref: normalize_map(fetch_required!(attrs, :actor_ref))
    })
  end

  defp record_terminal_attempt(
         {:ok, %DecisionRecord{} = updated_decision},
         decision,
         action,
         attrs
       ) do
    with {:ok, _fact} <-
           append_terminal_attempt(decision, updated_decision, action, attrs, :accepted) do
      {:ok, updated_decision}
    end
  end

  defp record_terminal_attempt({:error, error}, decision, action, attrs) do
    observed_decision = reload_decision(decision)
    outcome = failed_terminal_outcome(error, observed_decision, action, attrs)

    with :ok <- append_losing_attempts(decision, observed_decision, action, attrs, outcome, error) do
      {:error, {:decision_terminal_resolution_failed, error, outcome}}
    end
  end

  defp load_current_decision(%DecisionRecord{id: id}), do: load_current_decision(id)

  defp load_current_decision(id) when is_binary(id) do
    case Ash.get(DecisionRecord, id, authorize?: false, domain: Mezzanine.Decisions) do
      {:ok, nil} -> {:error, {:decision_not_found, id}}
      {:ok, %DecisionRecord{} = decision} -> {:ok, decision}
      {:error, error} -> {:error, error}
    end
  end

  defp ensure_pending(%DecisionRecord{lifecycle_state: "pending"}), do: :ok

  defp ensure_pending(%DecisionRecord{lifecycle_state: lifecycle_state}),
    do: {:error, {:decision_not_pending, lifecycle_state}}

  defp ensure_pending_or_record_conflict(%DecisionRecord{} = decision, action, attrs) do
    case ensure_pending(decision) do
      :ok ->
        :ok

      {:error, error} ->
        outcome = failed_terminal_outcome(error, decision, action, attrs)

        with :ok <- append_losing_attempts(decision, decision, action, attrs, outcome, error) do
          {:error, {:decision_terminal_resolution_failed, error, outcome}}
        end
    end
  end

  defp ensure_expected_row_version_or_record_conflict(
         %DecisionRecord{} = decision,
         action,
         attrs
       ) do
    case expected_row_version_attr(attrs) do
      nil ->
        :ok

      expected_row_version when expected_row_version == decision.row_version ->
        :ok

      expected_row_version ->
        error = {:stale_row_version, expected_row_version, decision.row_version}

        with :ok <-
               append_losing_attempts(
                 decision,
                 decision,
                 action,
                 attrs,
                 :stale_row_version,
                 error
               ) do
          {:error, {:decision_terminal_resolution_failed, error, :stale_row_version}}
        end
    end
  end

  defp reject_legacy_expiry_job_ref(decision, :expire, current_job_id) do
    case Map.get(decision, :expiry_job_id) do
      nil ->
        :ok

      expiry_job_id ->
        {:error, {:legacy_decision_expiry_job_ref_present, expiry_job_id, current_job_id}}
    end
  end

  defp reject_legacy_expiry_job_ref(_decision, _action, _current_job_id), do: :ok

  defp append_terminal_attempt(
         %DecisionRecord{} = original_decision,
         %DecisionRecord{} = observed_decision,
         action,
         attrs,
         outcome,
         error \\ nil
       ) do
    AuditAppend.append_fact(
      %{
        installation_id: original_decision.installation_id,
        subject_id: original_decision.subject_id,
        execution_id: original_decision.execution_id,
        decision_id: original_decision.id,
        trace_id: fetch_required!(attrs, :trace_id),
        causation_id: fetch_required!(attrs, :causation_id),
        fact_kind: :decision_terminal_resolution_attempt,
        actor_ref: normalize_map(fetch_required!(attrs, :actor_ref)),
        payload:
          terminal_attempt_payload(
            original_decision,
            observed_decision,
            action,
            attrs,
            outcome,
            error
          ),
        occurred_at: attempted_at(attrs)
      },
      []
    )
  end

  defp append_losing_attempts(
         %DecisionRecord{} = original_decision,
         %DecisionRecord{} = observed_decision,
         action,
         attrs,
         outcome,
         error
       )
       when outcome in @conflict_outcomes do
    with {:ok, _terminal_fact} <-
           append_terminal_attempt(
             original_decision,
             observed_decision,
             action,
             attrs,
             outcome,
             error
           ),
         {:ok, _conflict_fact} <-
           append_conflict_attempt(
             original_decision,
             observed_decision,
             action,
             attrs,
             outcome,
             error
           ) do
      :ok
    end
  end

  defp append_losing_attempts(
         %DecisionRecord{} = original_decision,
         %DecisionRecord{} = observed_decision,
         action,
         attrs,
         outcome,
         error
       ) do
    with {:ok, _terminal_fact} <-
           append_terminal_attempt(
             original_decision,
             observed_decision,
             action,
             attrs,
             outcome,
             error
           ) do
      :ok
    end
  end

  defp append_conflict_attempt(
         %DecisionRecord{} = original_decision,
         %DecisionRecord{} = observed_decision,
         action,
         attrs,
         outcome,
         error
       ) do
    AuditAppend.append_fact(
      %{
        installation_id: original_decision.installation_id,
        subject_id: original_decision.subject_id,
        execution_id: original_decision.execution_id,
        decision_id: original_decision.id,
        trace_id: fetch_required!(attrs, :trace_id),
        causation_id: fetch_required!(attrs, :causation_id),
        fact_kind: :decision_conflict_attempt,
        actor_ref: normalize_map(fetch_required!(attrs, :actor_ref)),
        payload:
          original_decision
          |> terminal_attempt_payload(observed_decision, action, attrs, outcome, error)
          |> Map.merge(%{
            conflict_attempt?: true,
            terminal_attempt_fact_kind: "decision_terminal_resolution_attempt",
            conflict_error_class: Atom.to_string(outcome)
          }),
        occurred_at: attempted_at(attrs)
      },
      []
    )
  end

  defp terminal_attempt_payload(
         original_decision,
         observed_decision,
         action,
         attrs,
         outcome,
         error
       ) do
    %{
      attempt_id: terminal_attempt_id(original_decision, action, attrs),
      decision_id: original_decision.id,
      tenant_id: tenant_id(attrs),
      installation_id: original_decision.installation_id,
      subject_id: original_decision.subject_id,
      execution_id: original_decision.execution_id,
      decision_kind: original_decision.decision_kind,
      actor_ref: normalize_map(fetch_required!(attrs, :actor_ref)),
      requested_decision: requested_decision(action, attrs),
      reason: map_value(attrs, :reason),
      trace_id: fetch_required!(attrs, :trace_id),
      causation_id: fetch_required!(attrs, :causation_id),
      idempotency_key: idempotency_key(original_decision, action, attrs),
      observed_lifecycle_state: observed_decision.lifecycle_state,
      observed_decision_value: observed_decision.decision_value,
      expected_row_version: expected_row_version(original_decision, attrs),
      observed_row_version: observed_decision.row_version,
      winner_decision_id: winner_decision_id(observed_decision, outcome),
      outcome: Atom.to_string(outcome),
      attempted_at: DateTime.to_iso8601(attempted_at(attrs)),
      error: normalize_error(error)
    }
  end

  defp failed_terminal_outcome(
         {:decision_not_pending, _state},
         %DecisionRecord{} = observed_decision,
         action,
         attrs
       ) do
    cond do
      duplicate_same_terminal_decision?(observed_decision, action, attrs) ->
        :duplicate_same_decision

      action == :expire ->
        :stale_expiry

      true ->
        :conflict_rejected
    end
  end

  defp failed_terminal_outcome(error, _observed_decision, _action, _attrs),
    do: classify_terminal_error(error)

  defp classify_terminal_error(error) do
    error_text = error |> inspect() |> String.downcase()

    cond do
      String.contains?(error_text, ["unique", "constraint"]) ->
        :unique_constraint_conflict

      String.contains?(error_text, ["lock", "stale", "row_version"]) ->
        :lock_conflict

      String.contains?(error_text, ["unsupported", "invalid_transition"]) ->
        :unsupported_transition

      true ->
        :optimistic_lock_conflict
    end
  end

  defp same_terminal_decision?(%DecisionRecord{} = decision, action, attrs) do
    requested = requested_decision(action, attrs)

    cond do
      action == :escalate ->
        decision.lifecycle_state == "escalated"

      action == :waive ->
        decision.lifecycle_state == "waived" and decision.decision_value == requested

      action == :expire ->
        decision.lifecycle_state == "expired" and decision.decision_value == requested

      true ->
        decision.lifecycle_state == "resolved" and decision.decision_value == requested
    end
  end

  defp duplicate_same_terminal_decision?(%DecisionRecord{} = decision, action, attrs) do
    same_terminal_decision?(decision, action, attrs) and
      same_idempotency_terminal_retry?(decision, action, attrs)
  end

  defp same_idempotency_terminal_retry?(%DecisionRecord{} = decision, action, attrs) do
    current_idempotency_key = idempotency_key(decision, action, attrs)
    requested = requested_decision(action, attrs)

    decision
    |> accepted_terminal_attempts()
    |> Enum.any?(fn fact ->
      fact.payload["outcome"] == "accepted" and
        fact.payload["requested_decision"] == requested and
        fact.payload["idempotency_key"] == current_idempotency_key
    end)
  end

  defp accepted_terminal_attempts(%DecisionRecord{
         id: decision_id,
         installation_id: installation_id
       }) do
    AuditQuery.decision_terminal_resolution_attempts(installation_id, decision_id)
    |> case do
      {:ok, facts} -> facts
      {:error, _error} -> []
    end
  end

  defp requested_decision(:accept, _attrs), do: "accept"
  defp requested_decision(:reject, _attrs), do: "reject"
  defp requested_decision(:waive, _attrs), do: "waive"
  defp requested_decision(:expire, _attrs), do: "expired"
  defp requested_decision(:escalate, _attrs), do: "escalate"
  defp requested_decision(:decide, attrs), do: fetch_required!(attrs, :decision_value)

  defp reload_decision(%DecisionRecord{id: id} = decision) do
    case load_current_decision(id) do
      {:ok, %DecisionRecord{} = reloaded_decision} -> reloaded_decision
      {:error, _error} -> decision
    end
  end

  defp terminal_attempt_id(%DecisionRecord{id: id}, action, attrs) do
    map_value(attrs, :attempt_id) ||
      "decision-attempt:#{id}:#{action}:#{fetch_required!(attrs, :trace_id)}:#{fetch_required!(attrs, :causation_id)}"
  end

  defp idempotency_key(%DecisionRecord{id: id}, action, attrs) do
    map_value(attrs, :idempotency_key) ||
      "decision-terminal:#{id}:#{action}:#{fetch_required!(attrs, :causation_id)}"
  end

  defp tenant_id(attrs) do
    actor_ref = normalize_map(map_value(attrs, :actor_ref) || %{})

    map_value(attrs, :tenant_id) || Map.get(actor_ref, "tenant_id")
  end

  defp expected_row_version(%DecisionRecord{row_version: row_version}, attrs),
    do: map_value(attrs, :expected_row_version) || row_version

  defp expected_row_version_attr(attrs) do
    case map_value(attrs, :expected_row_version) do
      nil -> nil
      value when is_integer(value) -> value
      value when is_binary(value) -> String.to_integer(value)
    end
  end

  defp winner_decision_id(%DecisionRecord{id: id}, outcome)
       when outcome in [
              :duplicate_same_decision,
              :conflict_rejected,
              :stale_expiry,
              :stale_row_version,
              :unsupported_transition,
              :unique_constraint_conflict,
              :lock_conflict,
              :optimistic_lock_conflict
            ],
       do: id

  defp winner_decision_id(_decision, _outcome), do: nil

  defp attempted_at(attrs) do
    case map_value(attrs, :attempted_at) do
      %DateTime{} = attempted_at -> DateTime.truncate(attempted_at, :microsecond)
      _other -> DateTime.utc_now() |> DateTime.truncate(:microsecond)
    end
  end

  defp normalize_error(nil), do: nil
  defp normalize_error(error), do: inspect(error)

  defp fetch_required!(attrs, key) when is_map(attrs) do
    case map_value(attrs, key) do
      nil -> raise ArgumentError, "missing required decision attribute #{inspect(key)}"
      value -> value
    end
  end

  defp map_value(map, key) when is_map(map),
    do: Map.get(map, key) || Map.get(map, to_string(key))

  defp normalize_map(value) when is_map(value) do
    value
    |> Enum.map(fn {key, nested} -> {to_string(key), normalize_value(nested)} end)
    |> Map.new()
  end

  defp normalize_map(_other), do: %{}

  defp normalize_value(%DateTime{} = datetime), do: DateTime.to_iso8601(datetime)
  defp normalize_value(value) when is_map(value), do: normalize_map(value)
  defp normalize_value(value) when is_list(value), do: Enum.map(value, &normalize_value/1)
  defp normalize_value(value) when is_atom(value), do: Atom.to_string(value)
  defp normalize_value(value), do: value
end
