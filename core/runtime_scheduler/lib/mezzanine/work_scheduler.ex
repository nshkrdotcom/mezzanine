defmodule Mezzanine.WorkScheduler do
  @moduledoc """
  Pure scheduling decisions for the headless runtime scheduler boundary.

  This module owns tick, ordering, capacity admission, continuation, retry,
  backoff, cancel, reassignment, and stall decisions. Durable execution updates
  remain in the execution engine; this boundary emits the stable decisions that
  those writers can persist.
  """

  @max_priority 4
  @normalizable_keys [
    :active?,
    :attempt,
    :assigned_to_worker,
    :blocked?,
    :candidates,
    :capacity,
    :created_at,
    :delay_ms,
    :due_at,
    :execution,
    :execution_id,
    :failure,
    :global,
    :identifier,
    :idempotency_key,
    :last_lower_activity_at,
    :max_attempts,
    :max_delay_ms,
    :now,
    :priority,
    :reason,
    :retry,
    :retry_base_ms,
    :retry_token,
    :running,
    :source,
    :source_state,
    :source_visible?,
    :stall_timeout_ms,
    :state,
    :states,
    :subject_id,
    :terminal?,
    :tick_kind,
    :worker_host,
    :worker_id,
    :workers,
    :workflow_id,
    :workflow_version
  ]
  @key_lookup Map.new(@normalizable_keys, &{Atom.to_string(&1), &1})

  @spec plan_tick(map() | keyword()) :: {:ok, map()} | {:error, term()}
  def plan_tick(attrs) when is_map(attrs) or is_list(attrs) do
    attrs = normalize(attrs)
    now = value(attrs, :now) || DateTime.utc_now()
    capacity = normalize_capacity(value(attrs, :capacity) || %{})
    candidates = value(attrs, :candidates) |> List.wrap() |> Enum.map(&normalize/1)
    running = value(attrs, :running) |> List.wrap() |> Enum.map(&normalize/1)

    sorted_candidates = Enum.sort_by(candidates, &candidate_sort_key/1)
    counters = capacity_counters(running)

    {events, final_counters} =
      Enum.map_reduce(sorted_candidates, counters, fn candidate, counters ->
        {event, counters} = decide_candidate(candidate, counters, capacity, now)
        {event, counters}
      end)

    {:ok,
     %{
       scheduler: __MODULE__,
       candidates: sorted_candidates,
       events: events,
       claims: Enum.filter(events, &(&1.event_kind == "work.claimed")),
       capacity: %{
         configured: capacity,
         used: final_counters
       },
       decided_at: now
     }}
  end

  @spec continuation_check(map() | keyword()) :: {:ok, map()} | {:error, term()}
  def continuation_check(attrs) when is_map(attrs) or is_list(attrs) do
    attrs = normalize(attrs)
    execution = attrs |> value(:execution) |> normalize()
    source = attrs |> value(:source) |> normalize()
    now = value(attrs, :now) || DateTime.utc_now()

    event =
      cond do
        value(source, :source_visible?) == false ->
          evidence_event("cancel.missing_source", execution, now,
            reason: "missing_source",
            safe_action: "cancel_lower_and_quarantine"
          )

        value(source, :active?) == false ->
          evidence_event("cancel.non_active_source", execution, now,
            reason: "non_active_source",
            safe_action: "cancel_lower_and_block"
          )

        truthy?(value(source, :terminal?)) ->
          evidence_event("cancel.terminal_source", execution, now,
            reason: "terminal_source",
            safe_action: "cancel_lower_cleanup_and_complete",
            cleanup_required?: true
          )

        true ->
          evidence_event("continuation.required", execution, now,
            reason: "source_still_active",
            safe_action: "next_turn"
          )
      end

    {:ok, event}
  end

  @spec retry_decision(map() | keyword()) :: {:ok, map()} | {:error, term()}
  def retry_decision(attrs) when is_map(attrs) or is_list(attrs) do
    attrs = normalize(attrs)
    execution = attrs |> value(:execution) |> normalize()
    retry = attrs |> value(:retry) |> normalize()
    now = value(attrs, :now) || DateTime.utc_now()

    if stale_retry?(execution, retry) do
      {:ok,
       evidence_event("retry.stale_token_ignored", execution, now,
         reason: "stale_retry_token",
         safe_action: "ignore_retry",
         retry_token: retry_token(execution),
         attempted_retry_token: retry_token(retry)
       )}
    else
      {:ok,
       evidence_event("retry.accepted", execution, now,
         reason: "retry_token_current",
         safe_action: "retry_dispatch",
         retry_token: retry_token(execution)
       )}
    end
  end

  @spec backoff_decision(map() | keyword()) :: {:ok, map()} | {:error, term()}
  def backoff_decision(attrs) when is_map(attrs) or is_list(attrs) do
    attrs = normalize(attrs)
    execution = attrs |> value(:execution) |> normalize()
    now = value(attrs, :now) || DateTime.utc_now()
    attempt = value(execution, :attempt) || 0
    max_attempts = value(attrs, :max_attempts)

    if is_integer(max_attempts) and attempt >= max_attempts do
      {:ok,
       evidence_event("retry.backoff_cap_reached", execution, now,
         reason: "retry_cap_reached",
         safe_action: "terminal_failure",
         failure: value(attrs, :failure)
       )}
    else
      delay_ms = backoff_delay_ms(attrs, attempt)
      due_at = DateTime.add(now, delay_ms, :millisecond)

      {:ok,
       evidence_event("retry.abnormal_backoff_scheduled", execution, now,
         reason: value(attrs, :reason) || "abnormal_exit",
         safe_action: "schedule_retry",
         delay_ms: delay_ms,
         due_at: due_at,
         failure: value(attrs, :failure)
       )}
    end
  end

  @spec stall_check(map() | keyword()) :: {:ok, map()} | :ok | {:error, term()}
  def stall_check(attrs) when is_map(attrs) or is_list(attrs) do
    attrs = normalize(attrs)
    execution = attrs |> value(:execution) |> normalize()
    now = value(attrs, :now) || DateTime.utc_now()
    timeout_ms = value(attrs, :stall_timeout_ms) || 0

    case value(execution, :last_lower_activity_at) do
      %DateTime{} = last_activity ->
        if DateTime.diff(now, last_activity, :millisecond) >= timeout_ms do
          {:ok,
           evidence_event("stall.detected", execution, now,
             reason: "lower_activity_timeout",
             safe_action: "retry_or_cancel",
             stall_timeout_ms: timeout_ms,
             last_lower_activity_at: last_activity
           )}
        else
          :ok
        end

      _other ->
        :ok
    end
  end

  @spec tick_event(map() | keyword()) :: {:ok, map()} | {:error, term()}
  def tick_event(attrs) when is_map(attrs) or is_list(attrs) do
    attrs = normalize(attrs)
    now = value(attrs, :now) || DateTime.utc_now()
    tick_kind = attrs |> value(:tick_kind) |> normalize_tick_kind()

    {:ok,
     %{
       event_kind: "scheduler.#{tick_kind}_tick",
       event_ref:
         "work-scheduler://scheduler.#{tick_kind}_tick/#{DateTime.to_unix(now, :microsecond)}",
       reason: "#{tick_kind}_requested",
       safe_action: "run_admission_tick",
       occurred_at: now,
       tick_kind: tick_kind
     }}
  end

  defp decide_candidate(candidate, counters, capacity, now) do
    cond do
      value(candidate, :source_visible?) == false ->
        {candidate_event("cancel.missing_source", candidate, now,
           reason: "missing_source",
           safe_action: "cancel_lower_and_quarantine"
         ), counters}

      value(candidate, :active?) == false ->
        {candidate_event("cancel.non_active_source", candidate, now,
           reason: "non_active_source",
           safe_action: "cancel_lower_and_block"
         ), counters}

      truthy?(value(candidate, :terminal?)) ->
        {candidate_event("cancel.terminal_source", candidate, now,
           reason: "terminal_source",
           safe_action: "cancel_lower_cleanup_and_complete",
           cleanup_required?: true
         ), counters}

      value(candidate, :assigned_to_worker) == false ->
        {candidate_event("claim.reassignment_denied", candidate, now,
           reason: "assigned_to_other_worker",
           safe_action: "deny_reassignment"
         ), counters}

      truthy?(value(candidate, :blocked?)) ->
        {candidate_event("work.skipped", candidate, now,
           reason: "blocked_by_source",
           safe_action: "skip_dispatch"
         ), counters}

      true ->
        case capacity_exhausted(candidate, counters, capacity) do
          nil ->
            {candidate_event("work.claimed", candidate, now,
               reason: "slot_available",
               safe_action: "dispatch_workflow"
             ), increment_counters(counters, candidate)}

          reason ->
            {candidate_event("capacity.slot_exhausted", candidate, now,
               reason: reason,
               safe_action: "defer_candidate"
             ), counters}
        end
    end
  end

  defp capacity_exhausted(candidate, counters, capacity) do
    state = state(candidate)
    worker_id = worker_id(candidate)

    cond do
      limit_reached?(counters.global, value(capacity, :global)) ->
        "global_capacity_exhausted"

      limit_reached?(Map.get(counters.states, state, 0), map_get(value(capacity, :states), state)) ->
        "state_capacity_exhausted"

      limit_reached?(
        Map.get(counters.workers, worker_id, 0),
        map_get(value(capacity, :workers), worker_id)
      ) ->
        "worker_capacity_exhausted"

      true ->
        nil
    end
  end

  defp limit_reached?(_count, nil), do: false
  defp limit_reached?(count, limit) when is_integer(limit), do: count >= limit
  defp limit_reached?(_count, _limit), do: false

  defp capacity_counters(running) do
    Enum.reduce(running, %{global: 0, states: %{}, workers: %{}}, &increment_counters(&2, &1))
  end

  defp increment_counters(counters, item) do
    state = state(item)
    worker_id = worker_id(item)

    %{
      counters
      | global: counters.global + 1,
        states: Map.update(counters.states, state, 1, &(&1 + 1)),
        workers: Map.update(counters.workers, worker_id, 1, &(&1 + 1))
    }
  end

  defp normalize_capacity(capacity) do
    capacity
    |> normalize()
    |> Map.update(:states, %{}, &string_key_map/1)
    |> Map.update(:workers, %{}, &string_key_map/1)
  end

  defp string_key_map(map) when is_map(map) do
    Map.new(map, fn {key, value} -> {to_string(key), value} end)
  end

  defp string_key_map(_other), do: %{}

  defp candidate_sort_key(candidate) do
    {
      priority_sort(value(candidate, :priority)),
      created_at_sort(value(candidate, :created_at)),
      to_string(value(candidate, :identifier) || value(candidate, :subject_id) || ""),
      to_string(value(candidate, :subject_id) || "")
    }
  end

  defp priority_sort(priority) when is_integer(priority) and priority in 1..@max_priority,
    do: priority

  defp priority_sort(_priority), do: @max_priority + 1

  defp created_at_sort(%DateTime{} = value), do: DateTime.to_unix(value, :microsecond)

  defp created_at_sort(value) when is_binary(value) do
    case DateTime.from_iso8601(value) do
      {:ok, datetime, _offset} -> created_at_sort(datetime)
      _other -> created_at_sort(nil)
    end
  end

  defp created_at_sort(_value), do: 253_402_300_799_999_999

  defp stale_retry?(execution, retry) do
    retry_token(execution) != retry_token(retry)
  end

  defp backoff_delay_ms(attrs, attempt) do
    base_ms = value(attrs, :retry_base_ms) || 10_000
    max_delay_ms = value(attrs, :max_delay_ms)
    delay_ms = base_ms * Integer.pow(2, max(attempt, 0))

    if is_integer(max_delay_ms), do: min(delay_ms, max_delay_ms), else: delay_ms
  end

  defp normalize_tick_kind(kind) when kind in [:startup, :admission, :manual_refresh] do
    Atom.to_string(kind)
  end

  defp normalize_tick_kind(kind) when kind in ["startup", "admission", "manual_refresh"], do: kind

  defp normalize_tick_kind(_other), do: "admission"

  defp retry_token(attrs) do
    attrs = normalize(attrs)

    %{
      workflow_id: value(attrs, :workflow_id),
      workflow_version: value(attrs, :workflow_version),
      attempt: value(attrs, :attempt),
      retry_token: value(attrs, :retry_token),
      idempotency_key: value(attrs, :idempotency_key)
    }
  end

  defp candidate_event(event_kind, candidate, now, opts) do
    event_kind
    |> evidence_event(candidate, now, opts)
    |> Map.put(:state, state(candidate))
    |> Map.put(:worker_id, worker_id(candidate))
  end

  defp evidence_event(event_kind, attrs, now, opts) do
    attrs = normalize(attrs)

    %{
      event_kind: event_kind,
      event_ref: event_ref(event_kind, attrs, now),
      subject_id: value(attrs, :subject_id),
      execution_id: value(attrs, :execution_id),
      workflow_id: value(attrs, :workflow_id),
      workflow_version: value(attrs, :workflow_version),
      attempt: value(attrs, :attempt),
      reason: Keyword.fetch!(opts, :reason),
      safe_action: Keyword.fetch!(opts, :safe_action),
      occurred_at: now,
      retry_token: Keyword.get(opts, :retry_token),
      attempted_retry_token: Keyword.get(opts, :attempted_retry_token),
      delay_ms: Keyword.get(opts, :delay_ms),
      due_at: Keyword.get(opts, :due_at),
      failure: Keyword.get(opts, :failure),
      cleanup_required?: Keyword.get(opts, :cleanup_required?),
      stall_timeout_ms: Keyword.get(opts, :stall_timeout_ms),
      last_lower_activity_at: Keyword.get(opts, :last_lower_activity_at)
    }
    |> compact_map()
  end

  defp event_ref(event_kind, attrs, now) do
    subject_id = value(attrs, :subject_id) || value(attrs, :execution_id) || "unknown"
    micros = DateTime.to_unix(now, :microsecond)
    "work-scheduler://#{event_kind}/#{subject_id}/#{micros}"
  end

  defp state(item), do: to_string(value(item, :state) || value(item, :source_state) || "unknown")

  defp worker_id(item),
    do: to_string(value(item, :worker_id) || value(item, :worker_host) || "local")

  defp normalize(nil), do: %{}
  defp normalize(attrs) when is_list(attrs), do: attrs |> Map.new() |> normalize()
  defp normalize(%_{} = attrs), do: attrs |> Map.from_struct() |> normalize()

  defp normalize(%{} = attrs) do
    Map.new(attrs, fn {key, value} -> {normalize_key(key), value} end)
  end

  defp normalize(other), do: other

  defp normalize_key(key) when is_atom(key), do: key

  defp normalize_key(key) when is_binary(key), do: Map.get(@key_lookup, key, key)

  defp normalize_key(key), do: key

  defp value(nil, _key), do: nil
  defp value(%_{} = struct, key), do: struct |> Map.from_struct() |> value(key)

  defp value(map, key) when is_map(map) do
    case Map.fetch(map, key) do
      {:ok, value} -> value
      :error -> Map.get(map, Atom.to_string(key))
    end
  end

  defp value(_value, _key), do: nil

  defp map_get(map, key) when is_map(map) do
    case Map.fetch(map, key) do
      {:ok, value} -> value
      :error -> Map.get(map, to_string(key))
    end
  end

  defp map_get(_value, _key), do: nil

  defp truthy?(value), do: value in [true, "true", 1, "1"]

  defp compact_map(map) do
    map
    |> Enum.reject(fn {_key, value} -> is_nil(value) end)
    |> Map.new()
  end
end
