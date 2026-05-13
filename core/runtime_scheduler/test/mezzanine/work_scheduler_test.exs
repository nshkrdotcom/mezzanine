defmodule Mezzanine.WorkSchedulerTest do
  use ExUnit.Case, async: true

  alias Mezzanine.WorkScheduler

  @now ~U[2026-05-10 21:45:00Z]

  test "orders candidates and enforces global, state, and worker capacity" do
    candidates = [
      candidate("subject-3", priority: 2, created_at: ~U[2026-05-10 10:02:00Z], state: "doing"),
      candidate("subject-1", priority: 1, created_at: ~U[2026-05-10 10:01:00Z], state: "todo"),
      candidate("subject-2", priority: 1, created_at: ~U[2026-05-10 10:00:00Z], state: "todo"),
      candidate("subject-4", priority: nil, created_at: ~U[2026-05-10 09:00:00Z], state: "todo"),
      candidate("subject-5",
        priority: 3,
        created_at: ~U[2026-05-10 10:03:00Z],
        state: "todo",
        assigned_to_worker: false
      ),
      candidate("subject-6",
        priority: 3,
        created_at: ~U[2026-05-10 10:04:00Z],
        state: "done",
        terminal?: true
      )
    ]

    assert {:ok, plan} =
             WorkScheduler.plan_tick(%{
               now: @now,
               candidates: candidates,
               running: [],
               capacity: %{
                 global: 2,
                 states: %{"todo" => 1, "doing" => 1},
                 workers: %{"worker-a" => 2}
               }
             })

    assert Enum.map(plan.candidates, & &1.subject_id) == [
             "subject-2",
             "subject-1",
             "subject-3",
             "subject-5",
             "subject-6",
             "subject-4"
           ]

    assert Enum.map(plan.events, &{&1.event_kind, &1.subject_id, &1.reason}) == [
             {"work.claimed", "subject-2", "slot_available"},
             {"capacity.slot_exhausted", "subject-1", "state_capacity_exhausted"},
             {"work.claimed", "subject-3", "slot_available"},
             {"claim.reassignment_denied", "subject-5", "assigned_to_other_worker"},
             {"cancel.terminal_source", "subject-6", "terminal_source"},
             {"capacity.slot_exhausted", "subject-4", "global_capacity_exhausted"}
           ]
  end

  test "derives global capacity from agent config when capacity override is absent" do
    candidates = [
      candidate("subject-1", priority: 1, created_at: ~U[2026-05-10 10:00:00Z], state: "todo"),
      candidate("subject-2", priority: 1, created_at: ~U[2026-05-10 10:01:00Z], state: "todo")
    ]

    running = [
      candidate("subject-running",
        priority: 1,
        created_at: ~U[2026-05-10 09:00:00Z],
        state: "todo"
      )
    ]

    assert {:ok, plan} =
             WorkScheduler.plan_tick(%{
               now: @now,
               agent: %{max_concurrent_agents: 2},
               candidates: candidates,
               running: running
             })

    assert plan.capacity.configured.global == 2

    assert Enum.map(plan.events, &{&1.event_kind, &1.subject_id, &1.reason}) == [
             {"work.claimed", "subject-1", "slot_available"},
             {"capacity.slot_exhausted", "subject-2", "global_capacity_exhausted"}
           ]

    assert {:ok, override_plan} =
             WorkScheduler.plan_tick(%{
               now: @now,
               agent: %{max_concurrent_agents: 1},
               capacity: %{global: 3},
               candidates: candidates,
               running: running
             })

    assert override_plan.capacity.configured.global == 3

    assert Enum.map(override_plan.events, &{&1.event_kind, &1.subject_id, &1.reason}) == [
             {"work.claimed", "subject-1", "slot_available"},
             {"work.claimed", "subject-2", "slot_available"}
           ]
  end

  test "derives state capacity from agent config when state override is absent" do
    candidates = [
      candidate("subject-1", priority: 1, created_at: ~U[2026-05-10 10:00:00Z], state: "Todo"),
      candidate("subject-2",
        priority: 1,
        created_at: ~U[2026-05-10 10:01:00Z],
        state: "In Progress"
      )
    ]

    running = [
      candidate("subject-running",
        priority: 1,
        created_at: ~U[2026-05-10 09:00:00Z],
        state: "todo"
      )
    ]

    assert {:ok, plan} =
             WorkScheduler.plan_tick(%{
               now: @now,
               agent: %{
                 max_concurrent_agents: 3,
                 max_concurrent_agents_by_state: %{"Todo" => 1}
               },
               candidates: candidates,
               running: running
             })

    assert plan.capacity.configured.states == %{"todo" => 1}

    assert Enum.map(plan.events, &{&1.event_kind, &1.subject_id, &1.reason}) == [
             {"capacity.slot_exhausted", "subject-1", "state_capacity_exhausted"},
             {"work.claimed", "subject-2", "slot_available"}
           ]

    assert {:ok, override_plan} =
             WorkScheduler.plan_tick(%{
               now: @now,
               agent: %{
                 max_concurrent_agents: 3,
                 max_concurrent_agents_by_state: %{"Todo" => 1}
               },
               capacity: %{states: %{"todo" => 2}},
               candidates: [
                 candidate("subject-3",
                   priority: 1,
                   created_at: ~U[2026-05-10 10:02:00Z],
                   state: "Todo"
                 ),
                 candidate("subject-4",
                   priority: 1,
                   created_at: ~U[2026-05-10 10:03:00Z],
                   state: "Todo"
                 )
               ],
               running: running
             })

    assert override_plan.capacity.configured.states == %{"todo" => 2}

    assert Enum.map(override_plan.events, &{&1.event_kind, &1.subject_id, &1.reason}) == [
             {"work.claimed", "subject-3", "slot_available"},
             {"capacity.slot_exhausted", "subject-4", "state_capacity_exhausted"}
           ]
  end

  test "enforces candidate eligibility rules before claiming work" do
    candidates = [
      candidate("subject-missing-title",
        priority: 1,
        created_at: ~U[2026-05-10 10:00:00Z],
        state: "Todo",
        title: nil
      ),
      candidate("subject-inactive",
        priority: 1,
        created_at: ~U[2026-05-10 10:01:00Z],
        state: "Backlog"
      ),
      candidate("subject-terminal",
        priority: 1,
        created_at: ~U[2026-05-10 10:02:00Z],
        state: "Done"
      ),
      candidate("subject-claimed",
        priority: 1,
        created_at: ~U[2026-05-10 10:03:00Z],
        state: "Todo"
      ),
      candidate("subject-running",
        priority: 1,
        created_at: ~U[2026-05-10 10:04:00Z],
        state: "Todo"
      ),
      candidate("subject-blocked",
        priority: 1,
        created_at: ~U[2026-05-10 10:05:00Z],
        state: "Todo",
        blocked_by: [%{identifier: "LIN-999", state: "In Progress"}]
      ),
      candidate("subject-ready-terminal-blocker",
        priority: 1,
        created_at: ~U[2026-05-10 10:06:00Z],
        state: "Todo",
        blocked_by: [%{identifier: "LIN-998", state: "Done"}]
      ),
      candidate("subject-second-todo",
        priority: 1,
        created_at: ~U[2026-05-10 10:07:00Z],
        state: "Todo"
      ),
      candidate("subject-ready-progress",
        priority: 1,
        created_at: ~U[2026-05-10 10:08:00Z],
        state: "In Progress"
      ),
      candidate("subject-over-global",
        priority: 1,
        created_at: ~U[2026-05-10 10:09:00Z],
        state: "In Progress"
      )
    ]

    assert {:ok, plan} =
             WorkScheduler.plan_tick(%{
               now: @now,
               active_states: ["Todo", "In Progress"],
               terminal_states: ["Done", "Canceled"],
               claimed: ["subject-claimed"],
               candidates: candidates,
               running: [
                 candidate("subject-running",
                   priority: 1,
                   created_at: ~U[2026-05-10 09:00:00Z],
                   state: "Review"
                 )
               ],
               capacity: %{
                 global: 3,
                 states: %{"todo" => 1, "in progress" => 2}
               }
             })

    assert Enum.map(plan.events, &{&1.event_kind, &1.subject_id, &1.reason}) == [
             {"work.skipped", "subject-missing-title", "missing_required_fields"},
             {"work.skipped", "subject-inactive", "source_state_not_dispatchable"},
             {"cancel.terminal_source", "subject-terminal", "terminal_source"},
             {"work.skipped", "subject-claimed", "already_claimed"},
             {"work.skipped", "subject-running", "already_running"},
             {"work.skipped", "subject-blocked", "non_terminal_dependency"},
             {"work.claimed", "subject-ready-terminal-blocker", "slot_available"},
             {"capacity.slot_exhausted", "subject-second-todo", "state_capacity_exhausted"},
             {"work.claimed", "subject-ready-progress", "slot_available"},
             {"capacity.slot_exhausted", "subject-over-global", "global_capacity_exhausted"}
           ]
  end

  test "emits continuation, cancel, stale retry, and stall evidence" do
    active_execution = %{
      execution_id: "execution-1",
      subject_id: "subject-1",
      workflow_id: "workflow-1",
      workflow_version: "v1",
      attempt: 2,
      retry_token: "retry-current",
      idempotency_key: "idem-1",
      last_lower_activity_at: ~U[2026-05-10 21:40:00Z]
    }

    assert {:ok, continuation} =
             WorkScheduler.continuation_check(%{
               execution: active_execution,
               source: %{source_visible?: true, active?: true, terminal?: false}
             })

    assert continuation.event_kind == "continuation.required"
    assert continuation.safe_action == "next_turn"

    assert {:ok, terminal_cancel} =
             WorkScheduler.continuation_check(%{
               execution: active_execution,
               source: %{source_visible?: true, active?: true, terminal?: true}
             })

    assert terminal_cancel.event_kind == "cancel.terminal_source"
    assert terminal_cancel.safe_action == "cancel_lower_cleanup_and_complete"
    assert terminal_cancel.cleanup_required?

    assert {:ok, non_active_cancel} =
             WorkScheduler.continuation_check(%{
               execution: active_execution,
               source: %{source_visible?: true, active?: false, terminal?: false}
             })

    assert non_active_cancel.event_kind == "cancel.non_active_source"
    assert non_active_cancel.safe_action == "cancel_lower_and_block"

    assert {:ok, missing_source_cancel} =
             WorkScheduler.continuation_check(%{
               execution: active_execution,
               source: %{source_visible?: false}
             })

    assert missing_source_cancel.event_kind == "cancel.missing_source"
    assert missing_source_cancel.safe_action == "cancel_lower_and_quarantine"

    assert {:ok, stale_retry} =
             WorkScheduler.retry_decision(%{
               execution: active_execution,
               retry: %{retry_token: "retry-old", attempt: 1, idempotency_key: "idem-1"}
             })

    assert stale_retry.event_kind == "retry.stale_token_ignored"
    assert stale_retry.safe_action == "ignore_retry"
    assert stale_retry.reason == "stale_retry_token"

    assert {:ok, stall} =
             WorkScheduler.stall_check(%{
               execution: active_execution,
               now: @now,
               stall_timeout_ms: 120_000
             })

    assert stall.event_kind == "stall.detected"
    assert stall.safe_action == "retry_or_cancel"
    assert stall.reason == "lower_activity_timeout"
  end

  test "emits tick and abnormal retry backoff cap evidence" do
    execution = %{
      execution_id: "execution-1",
      subject_id: "subject-1",
      workflow_id: "workflow-1",
      workflow_version: "v1",
      attempt: 2,
      retry_token: "retry-current",
      idempotency_key: "idem-1"
    }

    assert {:ok, startup_tick} = WorkScheduler.tick_event(now: @now, tick_kind: :startup)
    assert startup_tick.event_kind == "scheduler.startup_tick"
    assert startup_tick.safe_action == "run_admission_tick"

    assert {:ok, refresh_tick} = WorkScheduler.tick_event(now: @now, tick_kind: :manual_refresh)
    assert refresh_tick.event_kind == "scheduler.manual_refresh_tick"

    assert {:ok, retry} =
             WorkScheduler.backoff_decision(%{
               execution: execution,
               now: @now,
               retry_base_ms: 1_000,
               max_delay_ms: 3_000,
               max_attempts: 5,
               failure: "agent exited"
             })

    assert retry.event_kind == "retry.abnormal_backoff_scheduled"
    assert retry.safe_action == "schedule_retry"
    assert retry.delay_ms == 3_000
    assert retry.due_at == DateTime.add(@now, 3_000, :millisecond)

    assert {:ok, retry_cap} =
             WorkScheduler.backoff_decision(%{
               execution: execution,
               now: @now,
               max_attempts: 2
             })

    assert retry_cap.event_kind == "retry.backoff_cap_reached"
    assert retry_cap.safe_action == "terminal_failure"
  end

  defp candidate(subject_id, opts) do
    %{
      subject_id: subject_id,
      identifier: String.replace(subject_id, "subject", "LIN"),
      title: Keyword.get(opts, :title, "Work #{subject_id}"),
      priority: Keyword.fetch!(opts, :priority),
      created_at: Keyword.fetch!(opts, :created_at),
      state: Keyword.fetch!(opts, :state),
      worker_id: "worker-a",
      source_visible?: true,
      active?: true,
      blocked?: false,
      blocked_by: Keyword.get(opts, :blocked_by, []),
      assigned_to_worker: Keyword.get(opts, :assigned_to_worker, true),
      terminal?: Keyword.get(opts, :terminal?, false)
    }
  end
end
