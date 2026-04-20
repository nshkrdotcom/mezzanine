defmodule Mezzanine.Execution.OwnerDirectedCompensation do
  @moduledoc """
  Phase 5 owner-directed compensation profile.

  This module is deliberately a source-owned field and validation profile, not
  a saga runner. Compensation remains a declared owner command or workflow
  signal/activity with evidence; `LifecycleContinuation` only records retry
  and dead-letter visibility.
  """

  @release_manifest_ref "phase5-v7-m02ab-owner-directed-compensation-profile"

  @required_fields [
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
    :precondition,
    :side_effect_scope,
    :retry_policy,
    :dead_letter_ref,
    :operator_action_ref,
    :audit_or_evidence_ref,
    :release_manifest_ref
  ]

  @compensation_kinds [
    :retry,
    :cancel,
    :revoke,
    :restore,
    :quarantine,
    :repair_projection,
    :operator_retry,
    :operator_waive
  ]

  @kind_by_string Map.new(@compensation_kinds, &{Atom.to_string(&1), &1})
  @operator_action_kinds [:operator_retry, :operator_waive]

  @owner_rules %{
    workflow_lifecycle: %{
      target: :temporal_signal_or_activity,
      allowed_kinds: [:retry, :cancel, :operator_retry],
      forbidden: :local_workflow_truth_mutation
    },
    execution_ledger: %{
      target: :execution_owner_command,
      allowed_kinds: [:retry, :repair_projection, :operator_retry, :operator_waive],
      forbidden: :workflow_lifecycle_truth_mutation
    },
    decision_ledger: %{
      target: :decision_owner_command,
      allowed_kinds: [:retry, :operator_retry, :operator_waive],
      forbidden: :execution_or_audit_truth_mutation
    },
    audit_evidence: %{
      target: :audit_owner_command,
      allowed_kinds: [:retry, :quarantine, :operator_retry],
      forbidden: :aggregate_state_mutation
    },
    archival_restore: %{
      target: :archival_owner_command,
      allowed_kinds: [:restore, :quarantine, :operator_retry],
      forbidden: :audit_or_workflow_truth_mutation
    },
    lower_side_effect_boundary: %{
      target: :lower_cancel_or_revoke_operation,
      allowed_kinds: [:cancel, :revoke, :operator_retry],
      forbidden: :local_projection_only_rollback
    }
  }

  @spec profile() :: map()
  def profile do
    %{
      profile_name: "Mezzanine.OwnerDirectedCompensation.v1",
      owner_repo: :mezzanine,
      owner_package: :execution_engine,
      required_fields: @required_fields,
      compensation_kinds: @compensation_kinds,
      operator_action_required_for: @operator_action_kinds,
      owner_rules: @owner_rules,
      lifecycle_continuation_role: :retry_dead_letter_visibility_only,
      forbidden_patterns: [
        :anonymous_callback_handler,
        :multi_context_rollback_callback,
        :unbounded_silent_retry_loop,
        :operator_break_glass_without_owner_command
      ],
      release_manifest_ref: @release_manifest_ref
    }
  end

  @spec required_fields() :: [atom()]
  def required_fields, do: @required_fields

  @spec compensation_kinds() :: [atom()]
  def compensation_kinds, do: @compensation_kinds

  @spec owner_rules() :: map()
  def owner_rules, do: @owner_rules

  @spec release_manifest_ref() :: String.t()
  def release_manifest_ref, do: @release_manifest_ref

  @spec validate(map()) :: :ok | {:error, [term()]}
  def validate(attrs) when is_map(attrs) do
    errors = missing_field_errors(attrs) ++ kind_errors(attrs) ++ operator_action_errors(attrs)

    case errors do
      [] -> :ok
      _ -> {:error, errors}
    end
  end

  def validate(_attrs), do: {:error, [:invalid_compensation_profile]}

  @spec operator_action_required?(atom() | String.t()) :: boolean()
  def operator_action_required?(kind) do
    case normalize_kind(kind) do
      {:ok, normalized_kind} -> normalized_kind in @operator_action_kinds
      {:error, _reason} -> false
    end
  end

  defp missing_field_errors(attrs) do
    @required_fields
    |> Enum.reject(&field_present?(attrs, &1))
    |> Enum.map(&{:missing_field, &1})
  end

  defp kind_errors(attrs) do
    case normalize_kind(field_value(attrs, :compensation_kind)) do
      {:ok, _kind} -> []
      {:error, reason} -> [{:invalid_compensation_kind, reason}]
    end
  end

  defp operator_action_errors(attrs) do
    kind = field_value(attrs, :compensation_kind)

    if operator_action_required?(kind) and not present?(field_value(attrs, :operator_action_ref)) do
      [:operator_action_ref_required]
    else
      []
    end
  end

  defp normalize_kind(kind) when kind in @compensation_kinds, do: {:ok, kind}

  defp normalize_kind(kind) when is_binary(kind) do
    case Map.fetch(@kind_by_string, kind) do
      {:ok, normalized_kind} -> {:ok, normalized_kind}
      :error -> {:error, kind}
    end
  end

  defp normalize_kind(kind), do: {:error, kind}

  defp field_present?(attrs, :operator_action_ref),
    do: Map.has_key?(attrs, :operator_action_ref) or Map.has_key?(attrs, "operator_action_ref")

  defp field_present?(attrs, field), do: present?(field_value(attrs, field))

  defp field_value(attrs, field),
    do: Map.get(attrs, field) || Map.get(attrs, Atom.to_string(field))

  defp present?(value) when is_binary(value), do: String.trim(value) != ""
  defp present?(value) when is_map(value), do: map_size(value) > 0
  defp present?(value) when is_list(value), do: value != []
  defp present?(nil), do: false
  defp present?(_value), do: true
end
