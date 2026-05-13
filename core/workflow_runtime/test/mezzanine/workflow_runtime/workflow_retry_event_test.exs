defmodule Mezzanine.WorkflowRuntime.WorkflowRetryEventTest do
  use ExUnit.Case, async: true

  alias Mezzanine.WorkflowRuntime.WorkflowRetryEvent

  test "normal continuation retry uses the fixed post-exit continuation delay" do
    assert {:ok, event} = WorkflowRetryEvent.normal_continuation_retry(attrs())

    assert event.event_kind == :normal_continuation_retry
    assert event.retry_class == "normal_continuation"
    assert event.safe_action == "retry_after_continuation_delay"
    assert event.allowed?
    refute event.terminal?
    assert event.backoff_ms == 1_000
    assert event.event_id =~ "workflow.retry.continuation"
    assert event.retry_token == WorkflowRetryEvent.retry_token(event)
  end

  test "abnormal retry keeps backoff as explicit retry guidance" do
    assert {:ok, event} =
             attrs(%{"backoff_ms" => 12_500})
             |> WorkflowRetryEvent.abnormal_backoff_retry()

    assert event.event_kind == :abnormal_backoff_retry
    assert event.retry_class == "abnormal_backoff"
    assert event.safe_action == "retry_after_backoff"
    assert event.allowed?
    refute event.terminal?
    assert event.backoff_ms == 12_500
  end

  test "retry slot exhaustion is a terminal denied retry event" do
    assert {:ok, event} = WorkflowRetryEvent.retry_slot_exhausted(attrs())

    assert event.event_kind == :retry_slot_exhausted
    assert event.retry_class == "retry_slot_exhausted"
    assert event.safe_action == "surface_to_operator"
    assert event.denial_class == "retry_budget_exhausted"
    refute event.allowed?
    assert event.terminal?
  end

  test "terminal retry denial preserves the denial class" do
    assert {:ok, event} =
             attrs(%{"denial_class" => "operator_hold"})
             |> WorkflowRetryEvent.terminal_retry_denial()

    assert event.event_kind == :terminal_retry_denial
    assert event.retry_class == "terminal_retry_denial"
    assert event.safe_action == "deny_retry"
    assert event.denial_class == "operator_hold"
    refute event.allowed?
    assert event.terminal?
  end

  test "string event names and string-key attrs normalize without dynamic atom creation" do
    assert {:ok, event} =
             attrs(%{
               "event_kind" => "workflow.retry.backoff",
               "retry_class" => "abnormal_backoff",
               "safe_action" => "retry_after_backoff",
               "terminal?" => false,
               "allowed?" => true,
               "backoff_ms" => 7_000
             })
             |> WorkflowRetryEvent.new()

    assert event.event_kind == :abnormal_backoff_retry
    assert event.workflow_id == "tenant:t-1:resource:work-object-1"
    assert event.allowed?
    refute event.terminal?
    assert event.backoff_ms == 7_000
  end

  test "retry token guard accepts current state and rejects stale attempts" do
    assert {:ok, event} = WorkflowRetryEvent.normal_continuation_retry(attrs())

    assert :ok = WorkflowRetryEvent.guard_retry_token(event, current())

    assert {:error, {:stale_retry_token, stale}} =
             WorkflowRetryEvent.guard_retry_token(event, current(attempt: 3))

    assert stale.expected == WorkflowRetryEvent.retry_token(event)
    assert stale.got == WorkflowRetryEvent.retry_token(current(attempt: 3))

    assert {:error, {:stale_retry_token, _stale}} =
             WorkflowRetryEvent.guard_retry_token(
               event,
               current(workflow_version: "agent-run.v2")
             )

    assert {:error, {:stale_retry_token, _stale}} =
             WorkflowRetryEvent.guard_retry_token(event, current(idempotency_key: "idem-2"))
  end

  test "required fields fail closed" do
    assert {:error, {:missing_required_retry_event_field, :workflow_id}} =
             attrs()
             |> Map.delete(:workflow_id)
             |> WorkflowRetryEvent.normal_continuation_retry()

    assert {:error, {:unknown_retry_event_kind, "workflow.retry.unknown"}} =
             attrs(%{
               "event_kind" => "workflow.retry.unknown",
               "retry_class" => "unknown",
               "safe_action" => "deny_retry",
               "terminal?" => true,
               "allowed?" => false
             })
             |> WorkflowRetryEvent.new()
  end

  defp current(overrides \\ %{}) do
    Map.merge(
      %{
        workflow_id: "tenant:t-1:resource:work-object-1",
        workflow_version: "agent-run.v1",
        attempt: 2,
        retry_slot: 1,
        idempotency_key: "idem-1"
      },
      Map.new(overrides)
    )
  end

  defp attrs(overrides \\ %{}) do
    Map.merge(
      %{
        workflow_id: "tenant:t-1:resource:work-object-1",
        workflow_run_id: "workflow-run-1",
        workflow_type: "agent_run",
        workflow_version: "agent-run.v1",
        attempt: 2,
        retry_slot: 1,
        max_retry_slots: 3,
        idempotency_key: "idem-1",
        reason: "transient_runtime_unavailable",
        occurred_at: ~U[2026-05-08 12:00:00Z],
        metadata: %{source: "test"}
      },
      Map.new(overrides)
    )
  end
end
