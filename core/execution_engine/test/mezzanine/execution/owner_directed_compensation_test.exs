defmodule Mezzanine.Execution.OwnerDirectedCompensationTest do
  use ExUnit.Case, async: true

  alias Mezzanine.Execution.OwnerDirectedCompensation

  test "profile declares the Phase 5 required compensation fields" do
    assert OwnerDirectedCompensation.required_fields() == [
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

    assert OwnerDirectedCompensation.compensation_kinds() == [
             :retry,
             :cancel,
             :revoke,
             :restore,
             :quarantine,
             :repair_projection,
             :operator_retry,
             :operator_waive
           ]

    profile = OwnerDirectedCompensation.profile()

    assert profile.lifecycle_continuation_role == :retry_dead_letter_visibility_only
    assert :anonymous_callback_handler in profile.forbidden_patterns
    assert profile.release_manifest_ref == "phase5-v7-m02ab-owner-directed-compensation-profile"
  end

  test "owner rules keep compensation routed through bounded-context owners" do
    owner_rules = OwnerDirectedCompensation.owner_rules()

    assert %{
             target: :temporal_signal_or_activity,
             forbidden: :local_workflow_truth_mutation
           } = owner_rules.workflow_lifecycle

    assert %{
             target: :audit_owner_command,
             forbidden: :aggregate_state_mutation
           } = owner_rules.audit_evidence

    assert %{
             target: :lower_cancel_or_revoke_operation,
             forbidden: :local_projection_only_rollback
           } = owner_rules.lower_side_effect_boundary
  end

  test "validates complete owner-command compensation field sets" do
    assert :ok =
             OwnerDirectedCompensation.validate(%{
               compensation_ref: "compensation:projection:trace-1",
               source_context: "workflow_projection_reconciliation",
               source_event_ref: "workflow-event:42",
               failed_step_ref: "projection_rebuild",
               tenant_id: "tenant-1",
               installation_id: "installation-1",
               trace_id: "trace-1",
               causation_id: "cause-1",
               canonical_idempotency_key: "trace-1:projection:repair",
               compensation_owner: "execution_ledger",
               compensation_kind: :repair_projection,
               owner_command_or_signal: %{
                 kind: "owner_command",
                 owner: "execution_ledger",
                 command: "repair_projection"
               },
               precondition: "Temporal compact state is newer than Postgres projection",
               side_effect_scope: "execution projection row only",
               retry_policy: %{max_attempts: 3, backoff_ms: 5_000},
               dead_letter_ref: "dead-letter:projection:trace-1",
               operator_action_ref: nil,
               audit_or_evidence_ref: "audit:projection-repair:trace-1",
               release_manifest_ref: "phase5_hardening_metrics[29]"
             })
  end

  test "operator compensation requires operator action evidence" do
    attrs =
      %{
        "compensation_ref" => "compensation:operator:trace-1",
        "source_context" => "lifecycle_continuation",
        "source_event_ref" => "continuation:abc",
        "failed_step_ref" => "owner_command_dispatch",
        "tenant_id" => "tenant-1",
        "installation_id" => "installation-1",
        "trace_id" => "trace-1",
        "causation_id" => "cause-1",
        "canonical_idempotency_key" => "trace-1:operator:retry",
        "compensation_owner" => "workflow_lifecycle",
        "compensation_kind" => "operator_retry",
        "owner_command_or_signal" => %{
          "kind" => "workflow_signal",
          "workflow_id" => "workflow-1",
          "signal" => "retry_compensation"
        },
        "precondition" => "continuation is dead-lettered",
        "side_effect_scope" => "workflow signal only",
        "retry_policy" => %{"max_attempts" => 1},
        "dead_letter_ref" => "dead-letter:continuation:abc",
        "operator_action_ref" => nil,
        "audit_or_evidence_ref" => "audit:operator-retry:trace-1",
        "release_manifest_ref" => "phase5_hardening_metrics[29]"
      }

    assert {:error, [:operator_action_ref_required]} =
             OwnerDirectedCompensation.validate(attrs)

    assert :ok =
             attrs
             |> Map.put("operator_action_ref", "operator-action:retry:trace-1")
             |> OwnerDirectedCompensation.validate()
  end

  test "missing fields and unknown compensation kinds fail closed" do
    assert {:error, errors} =
             OwnerDirectedCompensation.validate(%{
               compensation_ref: "compensation:bad",
               compensation_kind: :rollback_everything
             })

    assert {:invalid_compensation_kind, :rollback_everything} in errors
    assert {:missing_field, :owner_command_or_signal} in errors
    assert {:missing_field, :canonical_idempotency_key} in errors
  end
end
