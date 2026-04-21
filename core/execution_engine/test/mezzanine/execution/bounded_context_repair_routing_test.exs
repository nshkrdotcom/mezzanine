defmodule Mezzanine.Execution.BoundedContextRepairRoutingTest do
  use ExUnit.Case, async: true

  alias Mezzanine.Execution.BoundedContextRepairRouting

  test "profile names every bounded-context repair owner and excludes workflow lifecycle" do
    profile = BoundedContextRepairRouting.profile()

    assert Enum.sort(profile.bounded_context_owners) == [
             :archival_restore,
             :audit_evidence,
             :decision_ledger,
             :execution_ledger,
             :lower_side_effect_boundary,
             :projection_repair
           ]

    assert profile.route_kind == :bounded_context_owner_command
    assert profile.lifecycle_continuation_role == :retry_dead_letter_visibility_only
    assert profile.workflow_lifecycle_route_ref == :workflow_lifecycle_compensation_routing
    assert "workflow_signal" in profile.forbidden_target_kinds
    assert "lifecycle_continuation_handler" in profile.forbidden_target_kinds
    assert "Mezzanine.Execution.LifecycleContinuation" in profile.forbidden_repair_owners

    assert profile.release_manifest_ref ==
             "phase5-v7-m02ad-bounded-context-owner-command-repair-routing"
  end

  test "builds execution owner-command repair routes without raw payload fields" do
    assert {:ok, route} =
             BoundedContextRepairRouting.route(
               compensation_attrs(:execution_ledger, :repair_projection, "repair_projection")
             )

    assert route.route_kind == :bounded_context_owner_command
    assert route.compensation_owner == :execution_ledger
    assert route.compensation_kind == :repair_projection
    assert route.dispatch_authority == :owner_command_only
    assert route.lifecycle_continuation_role == :retry_dead_letter_visibility_only
    assert route.forbidden_cross_write == :workflow_lifecycle_truth_mutation

    assert route.owner_command.owner == "execution_ledger"
    assert route.owner_command.owner_package == :execution_engine
    assert route.owner_command.owner_command_module == "Mezzanine.Execution.ExecutionRecord"
    assert route.owner_command.command == "repair_projection"
    assert route.owner_command.command_version == "execution-record-projection-repair.v1"
    assert route.owner_command.idempotency_key == "idem:execution_ledger:repair_projection"
    assert route.owner_command.audit_or_evidence_ref == "audit:execution_ledger:repair_projection"
    refute Map.has_key?(route.owner_command, :raw_payload)
    refute Map.has_key?(route.owner_command, :task_token)
    refute Map.has_key?(route.owner_command, :temporal_history_event)
  end

  test "routes each non-workflow bounded context through its named owner command" do
    cases = [
      {:execution_ledger, :retry, "record_retryable_failure",
       "Mezzanine.Execution.ExecutionRecord"},
      {:decision_ledger, :operator_waive, "waive", "Mezzanine.DecisionCommands"},
      {:audit_evidence, :quarantine, "quarantine_audit_evidence", "Mezzanine.Audit.AuditAppend"},
      {:archival_restore, :restore, "restore_audit_join", "Mezzanine.Archival.RestoreAuditJoin"},
      {:lower_side_effect_boundary, :cancel, "cancel_lower_side_effect",
       "JidoIntegration.LowerSideEffects"},
      {:projection_repair, :repair_projection, "repair_projection",
       "Mezzanine.WorkflowRuntime.ProjectionReconciliation"}
    ]

    for {owner, kind, command, module_name} <- cases do
      assert {:ok, route} =
               BoundedContextRepairRouting.route(compensation_attrs(owner, kind, command))

      assert route.compensation_owner == owner
      assert route.owner_command.owner == Atom.to_string(owner)
      assert route.owner_command.command == command
      assert route.owner_command.owner_command_module == module_name
      assert route.owner_command.release_manifest_ref == "phase5_hardening_metrics[31]"
    end
  end

  test "operator repairs require operator action evidence before routing" do
    attrs =
      :decision_ledger
      |> compensation_attrs(:operator_retry, "resolve_terminal")
      |> Map.put(:operator_action_ref, nil)

    assert {:error, {:invalid_compensation_profile, [:operator_action_ref_required]}} =
             BoundedContextRepairRouting.route(attrs)

    assert {:ok, route} =
             attrs
             |> Map.put(:operator_action_ref, "operator-action:decision-retry")
             |> BoundedContextRepairRouting.route()

    assert route.compensation_kind == :operator_retry
    assert route.owner_command.operator_action_ref == "operator-action:decision-retry"
  end

  test "rejects workflow lifecycle, lifecycle continuation, local mutation, and wrong owner commands" do
    assert {:error, {:forbidden_repair_owner, "workflow_lifecycle"}} =
             :workflow_lifecycle
             |> compensation_attrs(:retry, "retry")
             |> BoundedContextRepairRouting.route()

    assert {:error, {:forbidden_repair_target_kind, "local_mutation"}} =
             :execution_ledger
             |> compensation_attrs(:repair_projection, "repair_projection")
             |> put_in([:owner_command_or_signal, :kind], "local_mutation")
             |> BoundedContextRepairRouting.route()

    assert {:error, {:forbidden_repair_owner, "Mezzanine.Execution.LifecycleContinuation"}} =
             :execution_ledger
             |> compensation_attrs(:repair_projection, "repair_projection")
             |> put_in(
               [:owner_command_or_signal, :owner_command_module],
               "Mezzanine.Execution.LifecycleContinuation"
             )
             |> BoundedContextRepairRouting.route()

    assert {:error, {:owner_command_target_mismatch, :execution_ledger, :decision_ledger}} =
             :execution_ledger
             |> compensation_attrs(:repair_projection, "repair_projection")
             |> put_in([:owner_command_or_signal, :owner], "decision_ledger")
             |> BoundedContextRepairRouting.route()

    assert {:error,
            {:unsupported_owner_command, :execution_ledger, :repair_projection, "append_fact",
             ["repair_projection"]}} =
             :execution_ledger
             |> compensation_attrs(:repair_projection, "append_fact")
             |> BoundedContextRepairRouting.route()
  end

  defp compensation_attrs(owner, kind, command) do
    owner_string = Atom.to_string(owner)
    kind_string = Atom.to_string(kind)

    %{
      compensation_ref: "compensation:#{owner_string}:#{kind_string}",
      source_context: "#{owner_string}:repair",
      source_event_ref: "event:#{owner_string}:#{kind_string}",
      failed_step_ref: "failed-step:#{owner_string}:#{kind_string}",
      tenant_id: "tenant-acme",
      installation_id: "installation-main",
      trace_id: "trace-#{owner_string}",
      causation_id: "cause-#{owner_string}",
      canonical_idempotency_key: "idem-root:#{owner_string}:#{kind_string}",
      compensation_owner: owner_string,
      compensation_kind: kind,
      owner_command_or_signal: %{
        kind: "owner_command",
        owner: owner_string,
        command: command,
        idempotency_key: "idem:#{owner_string}:#{kind_string}"
      },
      precondition: "owner precondition observed before repair",
      side_effect_scope: "#{owner_string} invariant only",
      retry_policy: %{max_attempts: 3, backoff_ms: 5_000},
      dead_letter_ref: "dead-letter:#{owner_string}:#{kind_string}",
      operator_action_ref: operator_action_ref(kind, owner_string),
      audit_or_evidence_ref: "audit:#{owner_string}:#{kind_string}",
      release_manifest_ref: "phase5_hardening_metrics[31]"
    }
  end

  defp operator_action_ref(kind, owner) when kind in [:operator_retry, :operator_waive],
    do: "operator-action:#{owner}:#{kind}"

  defp operator_action_ref(_kind, _owner), do: nil
end
