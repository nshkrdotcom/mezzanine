defmodule Mezzanine.Execution.BoundedContextRepairRouting do
  @moduledoc """
  Phase 5 bounded-context owner-command repair routing profile.

  Workflow lifecycle compensation is handled by the workflow runtime. This
  module covers the remaining bounded-context repairs and keeps
  `LifecycleContinuation` as retry/dead-letter visibility only.
  """

  alias Mezzanine.Execution.OwnerDirectedCompensation

  @release_manifest_ref "phase5-v7-m02ad-bounded-context-owner-command-repair-routing"

  @required_target_fields ["owner", "command", "idempotency_key"]
  @forbidden_target_kinds [
    "workflow_signal",
    "workflow_activity",
    "local_mutation",
    "lifecycle_continuation_handler"
  ]
  @forbidden_repair_owners [
    :workflow_lifecycle,
    :lifecycle_continuation,
    "workflow_lifecycle",
    "lifecycle_continuation",
    "LifecycleContinuation",
    "Mezzanine.Execution.LifecycleContinuation"
  ]

  @owner_routes %{
    execution_ledger: %{
      owner_package: :execution_engine,
      owner_command_module: "Mezzanine.Execution.ExecutionRecord",
      commands_by_kind: %{
        retry: ["record_retryable_failure"],
        repair_projection: ["repair_projection"],
        operator_retry: ["enqueue_dispatch"],
        operator_waive: ["record_operator_cancelled"]
      },
      command_versions: %{
        "record_retryable_failure" => "execution-record-retryable-failure.v1",
        "repair_projection" => "execution-record-projection-repair.v1",
        "enqueue_dispatch" => "execution-record-enqueue-dispatch.v1",
        "record_operator_cancelled" => "execution-record-operator-cancelled.v1"
      },
      invariant: :execution_record_identity_and_dispatch_projection,
      forbidden_cross_write: :workflow_lifecycle_truth_mutation
    },
    decision_ledger: %{
      owner_package: :decision_engine,
      owner_command_module: "Mezzanine.DecisionCommands",
      commands_by_kind: %{
        retry: ["resolve_terminal"],
        operator_retry: ["resolve_terminal"],
        operator_waive: ["waive"]
      },
      command_versions: %{
        "resolve_terminal" => "decision-commands-resolve-terminal.v1",
        "waive" => "decision-commands-waive.v1"
      },
      invariant: :decision_terminal_truth_and_conflict_evidence,
      forbidden_cross_write: :execution_or_audit_truth_mutation
    },
    audit_evidence: %{
      owner_package: :audit_engine,
      owner_command_module: "Mezzanine.Audit.AuditAppend",
      commands_by_kind: %{
        retry: ["append_fact"],
        quarantine: ["quarantine_audit_evidence"],
        operator_retry: ["append_fact"]
      },
      command_versions: %{
        "append_fact" => "audit-append-fact.v1",
        "quarantine_audit_evidence" => "audit-evidence-quarantine.v1"
      },
      invariant: :audit_fact_append_only_evidence,
      forbidden_cross_write: :aggregate_state_mutation
    },
    archival_restore: %{
      owner_package: :archival_engine,
      owner_command_module: "Mezzanine.Archival.RestoreAuditJoin",
      commands_by_kind: %{
        restore: ["restore_audit_join"],
        quarantine: ["quarantine_restore"],
        operator_retry: ["restore_audit_join"]
      },
      command_versions: %{
        "restore_audit_join" => "archival-restore-audit-join.v1",
        "quarantine_restore" => "archival-restore-quarantine.v1"
      },
      invariant: :archival_restore_join_and_quarantine_truth,
      forbidden_cross_write: :audit_or_workflow_truth_mutation
    },
    lower_side_effect_boundary: %{
      owner_package: :jido_integration_lower_truth,
      owner_command_module: "JidoIntegration.LowerSideEffects",
      commands_by_kind: %{
        cancel: ["cancel_lower_side_effect"],
        revoke: ["revoke_lower_side_effect"],
        operator_retry: ["retry_lower_side_effect"]
      },
      command_versions: %{
        "cancel_lower_side_effect" => "lower-side-effect-cancel.v1",
        "revoke_lower_side_effect" => "lower-side-effect-revoke.v1",
        "retry_lower_side_effect" => "lower-side-effect-retry.v1"
      },
      invariant: :lower_fact_and_side_effect_truth,
      forbidden_cross_write: :local_projection_only_rollback
    },
    projection_repair: %{
      owner_package: :workflow_runtime,
      owner_command_module: "Mezzanine.WorkflowRuntime.ProjectionReconciliation",
      commands_by_kind: %{
        repair_projection: ["repair_projection"],
        operator_retry: ["repair_projection"],
        operator_waive: ["waive_projection_repair"]
      },
      command_versions: %{
        "repair_projection" => "workflow-projection-repair.v1",
        "waive_projection_repair" => "workflow-projection-waive-repair.v1"
      },
      invariant: :workflow_projection_reconciliation_truth,
      forbidden_cross_write: :workflow_lifecycle_truth_mutation
    }
  }

  @owner_by_string Map.new(Map.keys(@owner_routes), &{Atom.to_string(&1), &1})
  @compensation_kinds OwnerDirectedCompensation.compensation_kinds()
  @kind_by_string Map.new(@compensation_kinds, &{Atom.to_string(&1), &1})

  @spec profile() :: map()
  def profile do
    %{
      profile_name: "Mezzanine.BoundedContextRepairRouting.v1",
      owner_repo: :mezzanine,
      owner_package: :execution_engine,
      route_kind: :bounded_context_owner_command,
      bounded_context_owners: Map.keys(@owner_routes),
      owner_routes: @owner_routes,
      required_target_fields: @required_target_fields,
      forbidden_target_kinds: @forbidden_target_kinds,
      forbidden_repair_owners: @forbidden_repair_owners,
      workflow_lifecycle_route_ref: :workflow_lifecycle_compensation_routing,
      lifecycle_continuation_role: :retry_dead_letter_visibility_only,
      release_manifest_ref: @release_manifest_ref
    }
  end

  @spec owner_routes() :: map()
  def owner_routes, do: @owner_routes

  @spec release_manifest_ref() :: String.t()
  def release_manifest_ref, do: @release_manifest_ref

  @spec route(map()) :: {:ok, map()} | {:error, term()}
  def route(attrs) when is_map(attrs) do
    with :ok <- validate_compensation_profile(attrs),
         {:ok, owner, route_rule} <- owner_route(attrs),
         {:ok, compensation_kind} <- compensation_kind(attrs),
         :ok <- ensure_kind_allowed(route_rule, compensation_kind),
         {:ok, target} <- owner_command_target(attrs),
         :ok <- ensure_target_owner(owner, route_rule, target),
         {:ok, command} <- ensure_command_allowed(owner, route_rule, compensation_kind, target) do
      {:ok, build_route(attrs, owner, route_rule, compensation_kind, command, target)}
    end
  end

  def route(_attrs), do: {:error, :invalid_bounded_context_repair}

  defp validate_compensation_profile(attrs) do
    case OwnerDirectedCompensation.validate(attrs) do
      :ok -> :ok
      {:error, errors} -> {:error, {:invalid_compensation_profile, errors}}
    end
  end

  defp owner_route(attrs) do
    owner = field_value(attrs, :compensation_owner)

    with {:ok, normalized_owner} <- normalize_owner(owner),
         {:ok, route_rule} <- fetch_owner_route(normalized_owner) do
      {:ok, normalized_owner, route_rule}
    end
  end

  defp fetch_owner_route(owner) do
    case Map.fetch(@owner_routes, owner) do
      {:ok, route_rule} -> {:ok, route_rule}
      :error -> {:error, {:unsupported_repair_owner, owner}}
    end
  end

  defp compensation_kind(attrs) do
    case normalize_kind(field_value(attrs, :compensation_kind)) do
      {:ok, kind} -> {:ok, kind}
      {:error, kind} -> {:error, {:invalid_compensation_kind, kind}}
    end
  end

  defp ensure_kind_allowed(route_rule, compensation_kind) do
    if Map.has_key?(route_rule.commands_by_kind, compensation_kind) do
      :ok
    else
      {:error, {:unsupported_repair_kind, compensation_kind}}
    end
  end

  defp owner_command_target(attrs) do
    case field_value(attrs, :owner_command_or_signal) do
      %{} = target ->
        with :ok <- require_target_fields(target),
             :ok <- ensure_owner_command_kind(target),
             :ok <- reject_lifecycle_continuation_module(target) do
          {:ok, target}
        end

      _missing ->
        {:error, :missing_owner_command_or_signal}
    end
  end

  defp ensure_owner_command_kind(target) do
    case target_value(target, "kind") do
      "owner_command" -> :ok
      kind when kind in @forbidden_target_kinds -> {:error, {:forbidden_repair_target_kind, kind}}
      other -> {:error, {:unsupported_repair_target_kind, other}}
    end
  end

  defp require_target_fields(target) do
    missing =
      @required_target_fields
      |> Enum.reject(&present?(target_value(target, &1)))

    case missing do
      [] -> :ok
      _ -> {:error, {:missing_owner_command_target_fields, missing}}
    end
  end

  defp ensure_target_owner(expected_owner, route_rule, target) do
    with {:ok, target_owner} <- normalize_owner(target_value(target, "owner")),
         :ok <- ensure_target_module(route_rule, target) do
      if target_owner == expected_owner do
        :ok
      else
        {:error, {:owner_command_target_mismatch, expected_owner, target_owner}}
      end
    end
  end

  defp ensure_target_module(route_rule, target) do
    case target_value(target, "owner_command_module") do
      nil ->
        :ok

      module_name when module_name == route_rule.owner_command_module ->
        :ok

      module_name when module_name in @forbidden_repair_owners ->
        {:error, {:forbidden_repair_owner, module_name}}

      module_name ->
        {:error, {:owner_command_module_mismatch, route_rule.owner_command_module, module_name}}
    end
  end

  defp reject_lifecycle_continuation_module(target) do
    case target_value(target, "owner_command_module") do
      "Mezzanine.Execution.LifecycleContinuation" ->
        {:error, {:forbidden_repair_owner, "Mezzanine.Execution.LifecycleContinuation"}}

      "LifecycleContinuation" ->
        {:error, {:forbidden_repair_owner, "LifecycleContinuation"}}

      _ ->
        :ok
    end
  end

  defp ensure_command_allowed(owner, route_rule, compensation_kind, target) do
    command = target_value(target, "command")
    allowed_commands = Map.fetch!(route_rule.commands_by_kind, compensation_kind)

    if command in allowed_commands do
      {:ok, command}
    else
      {:error, {:unsupported_owner_command, owner, compensation_kind, command, allowed_commands}}
    end
  end

  defp build_route(attrs, owner, route_rule, compensation_kind, command, target) do
    command_version =
      target_value(target, "command_version") ||
        Map.fetch!(route_rule.command_versions, command)

    owner_command =
      %{
        owner: Atom.to_string(owner),
        owner_package: route_rule.owner_package,
        owner_command_module: route_rule.owner_command_module,
        command: command,
        command_version: command_version,
        idempotency_key: target_value(target, "idempotency_key"),
        command_payload_ref: target_value(target, "command_payload_ref"),
        command_payload_hash: target_value(target, "command_payload_hash"),
        compensation_ref: field_value(attrs, :compensation_ref),
        compensation_kind: compensation_kind,
        source_context: field_value(attrs, :source_context),
        source_event_ref: field_value(attrs, :source_event_ref),
        failed_step_ref: field_value(attrs, :failed_step_ref),
        tenant_ref: field_value(attrs, :tenant_id),
        installation_ref: field_value(attrs, :installation_id),
        trace_id: field_value(attrs, :trace_id),
        causation_id: field_value(attrs, :causation_id),
        canonical_idempotency_key: field_value(attrs, :canonical_idempotency_key),
        precondition: field_value(attrs, :precondition),
        side_effect_scope: field_value(attrs, :side_effect_scope),
        retry_policy: field_value(attrs, :retry_policy),
        dead_letter_ref: field_value(attrs, :dead_letter_ref),
        operator_action_ref: field_value(attrs, :operator_action_ref),
        audit_or_evidence_ref: field_value(attrs, :audit_or_evidence_ref),
        release_manifest_ref: field_value(attrs, :release_manifest_ref) || @release_manifest_ref
      }
      |> drop_nil_values()

    %{
      route_kind: :bounded_context_owner_command,
      compensation_owner: owner,
      compensation_kind: compensation_kind,
      dispatch_authority: :owner_command_only,
      lifecycle_continuation_role: :retry_dead_letter_visibility_only,
      invariant: route_rule.invariant,
      forbidden_cross_write: route_rule.forbidden_cross_write,
      owner_command: owner_command,
      release_manifest_ref: @release_manifest_ref
    }
  end

  defp normalize_owner(owner) when owner in @forbidden_repair_owners,
    do: {:error, {:forbidden_repair_owner, owner}}

  defp normalize_owner(owner) when is_atom(owner) do
    if Map.has_key?(@owner_routes, owner) do
      {:ok, owner}
    else
      {:error, {:unsupported_repair_owner, owner}}
    end
  end

  defp normalize_owner(owner) when is_binary(owner) do
    case Map.fetch(@owner_by_string, owner) do
      {:ok, normalized_owner} -> {:ok, normalized_owner}
      :error -> {:error, {:unsupported_repair_owner, owner}}
    end
  end

  defp normalize_owner(owner), do: {:error, {:unsupported_repair_owner, owner}}

  defp normalize_kind(kind) when kind in @compensation_kinds, do: {:ok, kind}

  defp normalize_kind(kind) when is_binary(kind) do
    case Map.fetch(@kind_by_string, kind) do
      {:ok, normalized_kind} -> {:ok, normalized_kind}
      :error -> {:error, kind}
    end
  end

  defp normalize_kind(kind), do: {:error, kind}

  defp field_value(attrs, field),
    do: Map.get(attrs, field) || Map.get(attrs, Atom.to_string(field))

  defp target_value(target, field),
    do: Map.get(target, field) || Map.get(target, target_field_atom(field))

  defp target_field_atom("kind"), do: :kind
  defp target_field_atom("owner"), do: :owner
  defp target_field_atom("command"), do: :command
  defp target_field_atom("idempotency_key"), do: :idempotency_key
  defp target_field_atom("owner_command_module"), do: :owner_command_module
  defp target_field_atom("command_version"), do: :command_version
  defp target_field_atom("command_payload_ref"), do: :command_payload_ref
  defp target_field_atom("command_payload_hash"), do: :command_payload_hash

  defp present?(value) when is_binary(value), do: String.trim(value) != ""
  defp present?(nil), do: false
  defp present?(_value), do: true

  defp drop_nil_values(map), do: Map.reject(map, fn {_key, value} -> is_nil(value) end)
end
