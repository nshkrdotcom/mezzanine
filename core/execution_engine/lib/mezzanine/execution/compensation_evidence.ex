defmodule Mezzanine.Execution.CompensationEvidence do
  @moduledoc """
  Phase 5 compensation retry, dead-letter, and operator action evidence.

  This module builds compact evidence events for owner-directed compensation.
  It does not dispatch or persist compensation by itself; callers attach the
  returned event to their owner-owned row, audit command, or workflow signal
  envelope.
  """

  @release_manifest_ref "phase5-v7-m02ae-compensation-retry-dead-letter-operator-evidence"

  @event_kinds [:retry_scheduled, :dead_lettered, :operator_action]
  @event_kind_by_string Map.new(@event_kinds, &{Atom.to_string(&1), &1})

  @common_required_fields [
    :compensation_ref,
    :source_context,
    :source_event_ref,
    :failed_step_ref,
    :tenant_id,
    :installation_id,
    :trace_id,
    :causation_id,
    :canonical_idempotency_key,
    :compensation_owner,
    :compensation_kind,
    :owner_command_or_signal,
    :attempt_ref,
    :attempt_number,
    :max_attempts,
    :retry_policy,
    :dead_letter_ref,
    :audit_or_evidence_ref,
    :release_manifest_ref
  ]

  @operator_required_fields [
    :operator_action_ref,
    :operator_actor_ref,
    :authority_decision_ref,
    :safe_action,
    :blast_radius
  ]

  @forbidden_target_kinds [
    "anonymous_callback",
    "callback",
    "local_mutation",
    "lifecycle_continuation_handler",
    "multi_context_rollback_callback"
  ]

  @forbidden_raw_fields [
    :raw_payload,
    :task_token,
    :temporal_history_event,
    :rollback_callback,
    :callback,
    "raw_payload",
    "task_token",
    "temporal_history_event",
    "rollback_callback",
    "callback"
  ]

  @spec profile() :: map()
  def profile do
    %{
      profile_name: "Mezzanine.CompensationEvidence.v1",
      owner_repo: :mezzanine,
      owner_package: :execution_engine,
      event_kinds: @event_kinds,
      common_required_fields: @common_required_fields,
      operator_required_fields: @operator_required_fields,
      forbidden_target_kinds: @forbidden_target_kinds,
      forbidden_raw_fields: Enum.map(@forbidden_raw_fields, &to_string/1) |> Enum.uniq(),
      retry_loop_policy: :max_attempts_required_and_enforced,
      hidden_rollback_policy: :forbidden_target_or_raw_callback_rejected,
      release_manifest_ref: @release_manifest_ref
    }
  end

  @spec event_kinds() :: [atom()]
  def event_kinds, do: @event_kinds

  @spec operator_required_fields() :: [atom()]
  def operator_required_fields, do: @operator_required_fields

  @spec release_manifest_ref() :: String.t()
  def release_manifest_ref, do: @release_manifest_ref

  @spec record(map()) :: {:ok, map()} | {:error, term()}
  def record(attrs) when is_map(attrs) do
    attrs = Map.new(attrs)

    with {:ok, event_kind} <- event_kind(attrs),
         :ok <- reject_raw_fields(attrs),
         :ok <- require_common_fields(attrs),
         :ok <- require_operator_fields(event_kind, attrs),
         {:ok, target} <- normalize_target(field_value(attrs, :owner_command_or_signal)),
         :ok <- reject_forbidden_target(target),
         :ok <- reject_forbidden_target_fields(target),
         {:ok, attempt_number} <- positive_integer(attrs, :attempt_number),
         {:ok, max_attempts} <- positive_integer(attrs, :max_attempts),
         :ok <- enforce_retry_loop_bound(event_kind, attempt_number, max_attempts) do
      {:ok, build_event(attrs, event_kind, target, attempt_number, max_attempts)}
    end
  end

  def record(_attrs), do: {:error, :invalid_compensation_evidence}

  @spec append_to_metadata(map() | nil, map()) :: map()
  def append_to_metadata(metadata, evidence) when is_map(evidence) do
    metadata = metadata || %{}
    existing = metadata_value(metadata, "compensation_evidence") || []

    Map.put(metadata, "compensation_evidence", existing ++ [evidence])
  end

  defp event_kind(attrs) do
    case normalize_event_kind(field_value(attrs, :event_kind)) do
      {:ok, kind} -> {:ok, kind}
      {:error, kind} -> {:error, {:invalid_compensation_evidence_event_kind, kind}}
    end
  end

  defp normalize_event_kind(kind) when kind in @event_kinds, do: {:ok, kind}

  defp normalize_event_kind(kind) when is_binary(kind) do
    case Map.fetch(@event_kind_by_string, kind) do
      {:ok, normalized_kind} -> {:ok, normalized_kind}
      :error -> {:error, kind}
    end
  end

  defp normalize_event_kind(kind), do: {:error, kind}

  defp reject_raw_fields(attrs) do
    present_raw_fields =
      @forbidden_raw_fields
      |> Enum.filter(&Map.has_key?(attrs, &1))
      |> Enum.map(&to_string/1)
      |> Enum.uniq()

    case present_raw_fields do
      [] -> :ok
      fields -> {:error, {:forbidden_compensation_evidence_raw_fields, fields}}
    end
  end

  defp require_common_fields(attrs) do
    missing =
      @common_required_fields
      |> Enum.reject(&present?(field_value(attrs, &1)))

    case missing do
      [] -> :ok
      fields -> {:error, {:missing_compensation_evidence_fields, fields}}
    end
  end

  defp require_operator_fields(:operator_action, attrs) do
    missing =
      @operator_required_fields
      |> Enum.reject(&present?(field_value(attrs, &1)))

    case missing do
      [] -> :ok
      fields -> {:error, {:missing_operator_action_evidence_fields, fields}}
    end
  end

  defp require_operator_fields(_event_kind, _attrs), do: :ok

  defp normalize_target(target) when is_map(target) do
    normalized = Map.new(target, fn {key, value} -> {to_string(key), value} end)

    if present?(Map.get(normalized, "kind")) do
      {:ok, normalized}
    else
      {:error, :missing_compensation_evidence_target_kind}
    end
  end

  defp normalize_target(_target), do: {:error, :missing_compensation_evidence_target}

  defp reject_forbidden_target(%{"kind" => kind}) when kind in @forbidden_target_kinds,
    do: {:error, {:forbidden_compensation_evidence_target_kind, kind}}

  defp reject_forbidden_target(_target), do: :ok

  defp reject_forbidden_target_fields(target) do
    present_fields =
      @forbidden_raw_fields
      |> Enum.map(&to_string/1)
      |> Enum.uniq()
      |> Enum.filter(&Map.has_key?(target, &1))

    case present_fields do
      [] -> :ok
      fields -> {:error, {:forbidden_compensation_evidence_target_fields, fields}}
    end
  end

  defp positive_integer(attrs, field) do
    value = field_value(attrs, field)

    if is_integer(value) and value > 0 do
      {:ok, value}
    else
      {:error, {:invalid_compensation_evidence_integer, field, value}}
    end
  end

  defp enforce_retry_loop_bound(:retry_scheduled, attempt_number, max_attempts)
       when attempt_number >= max_attempts,
       do: {:error, {:retry_attempt_exceeds_policy, attempt_number, max_attempts}}

  defp enforce_retry_loop_bound(_event_kind, _attempt_number, _max_attempts), do: :ok

  defp build_event(attrs, event_kind, target, attempt_number, max_attempts) do
    compensation_ref = field_value(attrs, :compensation_ref)
    attempt_ref = field_value(attrs, :attempt_ref)

    %{
      event_id: "compensation-evidence:#{compensation_ref}:#{event_kind}:#{attempt_ref}",
      event_kind: event_kind,
      compensation_ref: compensation_ref,
      source_context: field_value(attrs, :source_context),
      source_event_ref: field_value(attrs, :source_event_ref),
      failed_step_ref: field_value(attrs, :failed_step_ref),
      tenant_id: field_value(attrs, :tenant_id),
      installation_id: field_value(attrs, :installation_id),
      trace_id: field_value(attrs, :trace_id),
      causation_id: field_value(attrs, :causation_id),
      canonical_idempotency_key: field_value(attrs, :canonical_idempotency_key),
      compensation_owner: field_value(attrs, :compensation_owner),
      compensation_kind: field_value(attrs, :compensation_kind),
      owner_command_or_signal: target,
      attempt_ref: attempt_ref,
      attempt_number: attempt_number,
      max_attempts: max_attempts,
      retry_policy: field_value(attrs, :retry_policy),
      dead_letter_ref: field_value(attrs, :dead_letter_ref),
      failure_class: field_value(attrs, :failure_class),
      failure_reason: field_value(attrs, :failure_reason),
      next_attempt_at: field_value(attrs, :next_attempt_at),
      operator_action_ref: field_value(attrs, :operator_action_ref),
      operator_actor_ref: field_value(attrs, :operator_actor_ref),
      authority_decision_ref: field_value(attrs, :authority_decision_ref),
      safe_action: field_value(attrs, :safe_action),
      blast_radius: field_value(attrs, :blast_radius),
      audit_or_evidence_ref: field_value(attrs, :audit_or_evidence_ref),
      release_manifest_ref: field_value(attrs, :release_manifest_ref)
    }
    |> drop_nil_values()
  end

  defp field_value(attrs, field),
    do: Map.get(attrs, field) || Map.get(attrs, Atom.to_string(field))

  defp metadata_value(metadata, field),
    do: Map.get(metadata, field) || Map.get(metadata, String.to_atom(field))

  defp present?(value) when is_binary(value), do: String.trim(value) != ""
  defp present?(value) when is_map(value), do: map_size(value) > 0
  defp present?(value) when is_list(value), do: value != []
  defp present?(nil), do: false
  defp present?(_value), do: true

  defp drop_nil_values(map), do: Map.reject(map, fn {_key, value} -> is_nil(value) end)
end
