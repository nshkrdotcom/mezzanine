defmodule Mezzanine.ControlRoom.ResourcePressureContractTest do
  use ExUnit.Case, async: true

  alias Mezzanine.ControlRoom.QueuePressurePolicy
  alias Mezzanine.ControlRoom.RetryPosture

  test "builds queue pressure decisions with deterministic shedding evidence" do
    assert {:ok, decision} =
             QueuePressurePolicy.new(%{
               tenant_ref: "tenant-1",
               installation_ref: "inst-1",
               workspace_ref: "workspace-main",
               project_ref: "project-core",
               environment_ref: "prod",
               system_actor_ref: "system:queue-pressure",
               resource_ref: "queue:workflow-start-outbox",
               authority_packet_ref: "authz-packet-queue-1",
               permission_decision_ref: "decision-queue-1",
               idempotency_key: "queue-pressure:workflow-start-outbox:sample-1",
               trace_id: "trace:m11:065",
               correlation_id: "corr-queue-pressure",
               release_manifest_ref: "phase4-v6-milestone11",
               queue_name: "workflow_start_outbox",
               queue_ref: "queue://mezzanine/workflow_start_outbox",
               budget_ref: "budget://tenant-1/workflow-outbox",
               pressure_sample_ref: "pressure-sample:workflow-start-outbox:1",
               threshold_ref: "threshold:workflow-start-outbox:hard",
               pressure_class: :hard_pressure,
               current_depth: 250,
               max_depth: 200,
               admission_decision_ref: "admission:workflow-start-outbox:rejected",
               shed_decision: :shed,
               shed_reason: "queue_saturated",
               retry_after_ms: 30_000,
               operator_message_ref: "operator-message:queue-pressure:1"
             })

    assert decision.contract_name == "Mezzanine.QueuePressurePolicy.v1"
    assert decision.pressure_class == :hard_pressure
    assert decision.shed_decision == :shed
  end

  test "rejects queue pressure decisions without enterprise scope or valid retry delay" do
    assert {:error, {:missing_required_fields, fields}} =
             QueuePressurePolicy.new(%{
               tenant_ref: "tenant-1",
               installation_ref: "inst-1",
               system_actor_ref: "system:queue-pressure",
               authority_packet_ref: "authz-packet-queue-1",
               permission_decision_ref: "decision-queue-1",
               idempotency_key: "queue-pressure:workflow-start-outbox:sample-1",
               correlation_id: "corr-queue-pressure",
               release_manifest_ref: "phase4-v6-milestone11",
               queue_name: "workflow_start_outbox",
               queue_ref: "queue://mezzanine/workflow_start_outbox",
               budget_ref: "budget://tenant-1/workflow-outbox",
               pressure_sample_ref: "pressure-sample:workflow-start-outbox:1",
               threshold_ref: "threshold:workflow-start-outbox:hard",
               pressure_class: :hard_pressure,
               current_depth: 250,
               max_depth: 200,
               admission_decision_ref: "admission:workflow-start-outbox:rejected",
               shed_decision: :shed,
               shed_reason: "queue_saturated",
               retry_after_ms: 30_000,
               operator_message_ref: "operator-message:queue-pressure:1"
             })

    assert :workspace_ref in fields
    assert :trace_id in fields

    assert {:error, :invalid_queue_pressure_policy} =
             QueuePressurePolicy.new(%{valid_queue_pressure() | retry_after_ms: -1})
  end

  test "builds retry posture records for workflow and local-job failure paths" do
    assert {:ok, posture} =
             RetryPosture.new(%{
               tenant_ref: "tenant-1",
               installation_ref: "inst-1",
               workspace_ref: "workspace-main",
               project_ref: "project-core",
               environment_ref: "prod",
               system_actor_ref: "system:retry-posture",
               resource_ref: "workflow:expense-review",
               authority_packet_ref: "authz-packet-retry-1",
               permission_decision_ref: "decision-retry-1",
               idempotency_key: "retry-posture:expense-review:semantic-timeout",
               trace_id: "trace:m11:085",
               correlation_id: "corr-retry-posture",
               release_manifest_ref: "phase4-v6-milestone11",
               operation_ref: "operation:semantic-activity:score-expense",
               owner_repo: "mezzanine",
               producer_ref: "Mezzanine.WorkflowRuntime.ScoreExpenseActivity",
               consumer_ref: "Temporal.ActivityRetryPolicy",
               retry_class: :safe_idempotent,
               failure_class: "transient_timeout",
               max_attempts: 3,
               backoff_policy: %{
                 "strategy" => "exponential",
                 "initial_ms" => 1_000,
                 "max_ms" => 30_000
               },
               idempotency_scope: "operation_ref+idempotency_key",
               dead_letter_ref: "dead-letter:semantic-activity:score-expense",
               safe_action_code: "wait_for_retry_or_escalate_after_dead_letter",
               retry_after_ms: 1_000,
               operator_message_ref: "operator-message:retry-posture:1"
             })

    assert posture.contract_name == "Platform.RetryPosture.v1"
    assert posture.retry_class == :safe_idempotent
    assert posture.max_attempts == 3
  end

  test "rejects retry posture without idempotency or bounded retry policy" do
    assert {:error, {:missing_required_fields, fields}} =
             RetryPosture.new(%{
               tenant_ref: "tenant-1",
               installation_ref: "inst-1",
               system_actor_ref: "system:retry-posture",
               resource_ref: "workflow:expense-review",
               authority_packet_ref: "authz-packet-retry-1",
               permission_decision_ref: "decision-retry-1",
               trace_id: "trace:m11:085",
               correlation_id: "corr-retry-posture",
               release_manifest_ref: "phase4-v6-milestone11",
               operation_ref: "operation:semantic-activity:score-expense",
               owner_repo: "mezzanine",
               producer_ref: "Mezzanine.WorkflowRuntime.ScoreExpenseActivity",
               consumer_ref: "Temporal.ActivityRetryPolicy",
               retry_class: :safe_idempotent,
               failure_class: "transient_timeout",
               max_attempts: 3,
               backoff_policy: %{"strategy" => "exponential"},
               dead_letter_ref: "dead-letter:semantic-activity:score-expense",
               safe_action_code: "wait_for_retry_or_escalate_after_dead_letter"
             })

    assert :workspace_ref in fields
    assert :idempotency_key in fields
    assert :idempotency_scope in fields

    assert {:error, :invalid_retry_posture} =
             RetryPosture.new(%{valid_retry_posture() | max_attempts: -1})
  end

  defp valid_queue_pressure do
    %{
      tenant_ref: "tenant-1",
      installation_ref: "inst-1",
      workspace_ref: "workspace-main",
      project_ref: "project-core",
      environment_ref: "prod",
      system_actor_ref: "system:queue-pressure",
      resource_ref: "queue:workflow-start-outbox",
      authority_packet_ref: "authz-packet-queue-1",
      permission_decision_ref: "decision-queue-1",
      idempotency_key: "queue-pressure:workflow-start-outbox:sample-1",
      trace_id: "trace:m11:065",
      correlation_id: "corr-queue-pressure",
      release_manifest_ref: "phase4-v6-milestone11",
      queue_name: "workflow_start_outbox",
      queue_ref: "queue://mezzanine/workflow_start_outbox",
      budget_ref: "budget://tenant-1/workflow-outbox",
      pressure_sample_ref: "pressure-sample:workflow-start-outbox:1",
      threshold_ref: "threshold:workflow-start-outbox:hard",
      pressure_class: :hard_pressure,
      current_depth: 250,
      max_depth: 200,
      admission_decision_ref: "admission:workflow-start-outbox:rejected",
      shed_decision: :shed,
      shed_reason: "queue_saturated",
      retry_after_ms: 30_000,
      operator_message_ref: "operator-message:queue-pressure:1"
    }
  end

  defp valid_retry_posture do
    %{
      tenant_ref: "tenant-1",
      installation_ref: "inst-1",
      workspace_ref: "workspace-main",
      project_ref: "project-core",
      environment_ref: "prod",
      system_actor_ref: "system:retry-posture",
      resource_ref: "workflow:expense-review",
      authority_packet_ref: "authz-packet-retry-1",
      permission_decision_ref: "decision-retry-1",
      idempotency_key: "retry-posture:expense-review:semantic-timeout",
      trace_id: "trace:m11:085",
      correlation_id: "corr-retry-posture",
      release_manifest_ref: "phase4-v6-milestone11",
      operation_ref: "operation:semantic-activity:score-expense",
      owner_repo: "mezzanine",
      producer_ref: "Mezzanine.WorkflowRuntime.ScoreExpenseActivity",
      consumer_ref: "Temporal.ActivityRetryPolicy",
      retry_class: :safe_idempotent,
      failure_class: "transient_timeout",
      max_attempts: 3,
      backoff_policy: %{"strategy" => "exponential"},
      idempotency_scope: "operation_ref+idempotency_key",
      dead_letter_ref: "dead-letter:semantic-activity:score-expense",
      safe_action_code: "wait_for_retry_or_escalate_after_dead_letter"
    }
  end
end
