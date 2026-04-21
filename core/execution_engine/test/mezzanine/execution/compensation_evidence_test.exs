defmodule Mezzanine.Execution.CompensationEvidenceTest do
  use ExUnit.Case, async: true

  alias Mezzanine.Execution.CompensationEvidence

  test "profile declares retry, dead-letter, and operator evidence gates" do
    profile = CompensationEvidence.profile()

    assert profile.event_kinds == [:retry_scheduled, :dead_lettered, :operator_action]
    assert profile.retry_loop_policy == :max_attempts_required_and_enforced
    assert profile.hidden_rollback_policy == :forbidden_target_or_raw_callback_rejected
    assert :operator_action_ref in profile.operator_required_fields
    assert "lifecycle_continuation_handler" in profile.forbidden_target_kinds
    assert "raw_payload" in profile.forbidden_raw_fields

    assert profile.release_manifest_ref ==
             "phase5-v7-m02ae-compensation-retry-dead-letter-operator-evidence"
  end

  test "records bounded retry evidence without raw payload fields" do
    assert {:ok, evidence} = CompensationEvidence.record(evidence_attrs(:retry_scheduled))

    assert evidence.event_kind == :retry_scheduled
    assert evidence.attempt_number == 1
    assert evidence.max_attempts == 3
    assert evidence.retry_policy == %{max_attempts: 3, backoff_ms: 5_000}
    assert evidence.owner_command_or_signal["kind"] == "owner_command"
    refute Map.has_key?(evidence, :raw_payload)
    refute Map.has_key?(evidence, :task_token)
    refute Map.has_key?(evidence, :temporal_history_event)

    assert {:error, {:retry_attempt_exceeds_policy, 3, 3}} =
             :retry_scheduled
             |> evidence_attrs()
             |> Map.put(:attempt_number, 3)
             |> CompensationEvidence.record()
  end

  test "records dead-letter evidence with failure reason and dead-letter ref" do
    assert {:ok, evidence} =
             :dead_lettered
             |> evidence_attrs()
             |> Map.merge(%{
               attempt_number: 3,
               failure_class: "retry_budget_exhausted",
               failure_reason: "dependency_unavailable"
             })
             |> CompensationEvidence.record()

    assert evidence.event_kind == :dead_lettered
    assert evidence.failure_class == "retry_budget_exhausted"
    assert evidence.failure_reason == "dependency_unavailable"
    assert evidence.dead_letter_ref == "dead-letter:compensation:123"
  end

  test "operator evidence is required before operator compensation records" do
    attrs = evidence_attrs(:operator_action, compensation_kind: :operator_retry)

    assert {:error, {:missing_operator_action_evidence_fields, fields}} =
             CompensationEvidence.record(attrs)

    assert :operator_action_ref in fields
    assert :operator_actor_ref in fields
    assert :authority_decision_ref in fields

    assert {:ok, evidence} =
             attrs
             |> Map.merge(operator_evidence())
             |> CompensationEvidence.record()

    assert evidence.event_kind == :operator_action
    assert evidence.compensation_kind == :operator_retry
    assert evidence.operator_action_ref == "operator-action:retry:123"
    assert evidence.authority_decision_ref == "authority-decision:retry:123"
  end

  test "hidden rollback callbacks and raw evidence payloads fail closed" do
    assert {:error,
            {:forbidden_compensation_evidence_target_kind, "lifecycle_continuation_handler"}} =
             :retry_scheduled
             |> evidence_attrs()
             |> put_in([:owner_command_or_signal, :kind], "lifecycle_continuation_handler")
             |> CompensationEvidence.record()

    assert {:error, {:forbidden_compensation_evidence_raw_fields, ["raw_payload"]}} =
             :dead_lettered
             |> evidence_attrs()
             |> Map.put(:raw_payload, %{secret: "not allowed"})
             |> CompensationEvidence.record()

    assert {:error, {:forbidden_compensation_evidence_target_fields, ["rollback_callback"]}} =
             :operator_action
             |> evidence_attrs(operator_evidence())
             |> put_in([:owner_command_or_signal, :rollback_callback], "hidden rollback")
             |> CompensationEvidence.record()
  end

  defp evidence_attrs(event_kind, overrides \\ []) do
    %{
      event_kind: event_kind,
      compensation_ref: "compensation:123",
      source_context: "execution_ledger",
      source_event_ref: "event:123",
      failed_step_ref: "dispatch:123",
      tenant_id: "tenant-1",
      installation_id: "installation-1",
      trace_id: "trace-123",
      causation_id: "cause-123",
      canonical_idempotency_key: "idem:123",
      compensation_owner: "execution_ledger",
      compensation_kind: :retry,
      owner_command_or_signal: %{
        kind: "owner_command",
        owner: "execution_ledger",
        command: "record_retryable_failure",
        idempotency_key: "idem:123"
      },
      attempt_ref: "attempt:123:1",
      attempt_number: 1,
      max_attempts: 3,
      retry_policy: %{max_attempts: 3, backoff_ms: 5_000},
      dead_letter_ref: "dead-letter:compensation:123",
      audit_or_evidence_ref: "audit:compensation:123",
      release_manifest_ref: "phase5_hardening_metrics[32]"
    }
    |> Map.merge(Map.new(overrides))
  end

  defp operator_evidence do
    %{
      operator_action_ref: "operator-action:retry:123",
      operator_actor_ref: "actor:operator:123",
      authority_decision_ref: "authority-decision:retry:123",
      safe_action: "retry owner command once",
      blast_radius: "execution ledger only"
    }
  end
end
