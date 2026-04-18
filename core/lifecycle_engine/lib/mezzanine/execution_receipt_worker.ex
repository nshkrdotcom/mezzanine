defmodule Mezzanine.ExecutionReceiptWorker do
  @moduledoc """
  Idempotent receipt worker that converges accepted executions to terminal
  substrate truth and re-enters lifecycle evaluation from typed execution
  outcomes.
  """

  use Oban.Worker,
    queue: :receipt,
    max_attempts: 20,
    unique: [
      period: :infinity,
      states: [:available, :scheduled, :executing, :retryable],
      keys: [:execution_id, :receipt_id]
    ]

  alias Ecto.Adapters.SQL
  alias Ecto.Multi
  alias Ecto.UUID
  alias Mezzanine.Audit.AuditFact
  alias Mezzanine.Execution.{ExecutionRecord, Repo}
  alias Mezzanine.JobOutbox
  alias Mezzanine.JoinAdvanceWorker
  alias Mezzanine.LifecycleEvaluator
  alias Mezzanine.ParallelBarrier
  alias Mezzanine.Telemetry

  @failure_kind_by_string %{
    "transient_failure" => :transient_failure,
    "timeout" => :timeout,
    "infrastructure_error" => :infrastructure_error,
    "auth_error" => :auth_error,
    "semantic_failure" => :semantic_failure,
    "fatal_error" => :fatal_error
  }
  @subject_status_sql """
  SELECT status
  FROM subject_records
  WHERE id = $1::uuid
  LIMIT 1
  """

  @spec unique_declaration() :: keyword()
  def unique_declaration do
    [
      period: :infinity,
      states: [:available, :scheduled, :executing, :retryable],
      keys: [:execution_id, :receipt_id]
    ]
  end

  @impl Oban.Worker
  def perform(%Oban.Job{args: %{"execution_id" => execution_id, "outcome" => outcome}} = job) do
    with {:ok, execution} <- Ash.get(ExecutionRecord, execution_id),
         {:ok, normalized_outcome} <- normalize_outcome(outcome),
         {:ok, validated_outcome} <- validate_lifecycle_hints(execution, normalized_outcome) do
      handle_terminal_receipt(execution, validated_outcome, job)
    else
      {:error, error} -> {:error, error}
    end
  end

  defp handle_terminal_receipt(
         %ExecutionRecord{} = execution,
         normalized_outcome,
         %Oban.Job{} = job
       ) do
    if cancelled_subject?(execution.subject_id) do
      record_post_cancel_receipt(execution, normalized_outcome, job)
    else
      persist_and_continue(execution, normalized_outcome, job)
    end
  end

  defp persist_and_continue(%ExecutionRecord{} = execution, normalized_outcome, %Oban.Job{} = job) do
    with {:ok, terminal_execution} <- persist_outcome(execution, normalized_outcome, job),
         {:ok, _follow_up_result} <-
           continue_after_terminal_execution(terminal_execution, normalized_outcome, job) do
      :ok
    else
      {:error, error} -> {:error, error}
    end
  end

  defp persist_outcome(%ExecutionRecord{} = execution, _outcome, _job)
       when execution.dispatch_state in [:completed, :failed, :rejected, :cancelled],
       do: {:ok, execution}

  defp persist_outcome(%ExecutionRecord{} = execution, outcome, %Oban.Job{} = job) do
    attrs = %{
      receipt_id: outcome.receipt_id,
      lower_receipt: outcome.lower_receipt,
      normalized_outcome: outcome.normalized_outcome,
      artifact_refs: outcome.artifact_refs,
      trace_id: execution.trace_id,
      causation_id: causation_id(job),
      actor_ref: actor_ref(job)
    }

    case outcome.status do
      :ok ->
        ExecutionRecord.record_completed(execution, attrs)

      :error ->
        ExecutionRecord.record_failed_outcome(
          execution,
          Map.put(attrs, :failure_kind, outcome.failure_kind || :fatal_error)
        )

      :cancelled ->
        ExecutionRecord.record_cancelled_outcome(execution, attrs)
    end
  end

  defp continue_after_terminal_execution(
         %ExecutionRecord{barrier_id: barrier_id} = execution,
         _outcome,
         %Oban.Job{} = job
       )
       when is_binary(barrier_id) do
    now = DateTime.utc_now() |> DateTime.truncate(:microsecond)

    barrier_id
    |> ParallelBarrier.completion_multi(execution.id, now: now)
    |> Multi.run(:join_advance_job, fn _repo, %{parallel_barrier_progress: progress} ->
      maybe_enqueue_join_advance(progress, execution, job, now)
    end)
    |> Repo.transaction()
    |> case do
      {:ok, %{parallel_barrier_progress: progress}} ->
        {:ok, %{action: :barrier_progress, progress: progress}}

      {:error, _step, error, _changes} ->
        {:error, error}
    end
  end

  defp continue_after_terminal_execution(
         %ExecutionRecord{} = execution,
         outcome,
         %Oban.Job{} = job
       ) do
    advance_lifecycle(execution, outcome, job)
  end

  defp advance_lifecycle(%ExecutionRecord{} = execution, outcome, %Oban.Job{} = job) do
    case lifecycle_trigger(execution, outcome) do
      nil ->
        {:ok, %{action: :noop, reason: :no_lifecycle_trigger}}

      trigger ->
        LifecycleEvaluator.advance(
          execution.subject_id,
          trigger: trigger,
          trace_id: execution.trace_id,
          execution_id: execution.id,
          causation_id: causation_id(job),
          actor_ref: actor_ref(job)
        )
        |> case do
          {:ok, result} -> continue_after_state_transition(execution, result, outcome, job)
          {:error, error} -> {:error, error}
        end
    end
  end

  defp maybe_enqueue_join_advance(progress, %ExecutionRecord{} = execution, _job, now) do
    if progress.closed_by_me do
      case JobOutbox.enqueue(
             :join,
             JoinAdvanceWorker,
             %{
               subject_id: execution.subject_id,
               barrier_id: execution.barrier_id
             },
             scheduled_at: now
           ) do
        {:ok, job_ref} ->
          emit_join_advance_enqueued(progress, execution, job_ref)
          {:ok, job_ref}

        {:error, error} ->
          {:error, {:join_advance_enqueue_failed, error}}
      end
    else
      {:ok, :not_closer}
    end
  end

  defp continue_after_state_transition(
         execution,
         %{action: :advanced_state},
         outcome,
         %Oban.Job{} = job
       ) do
    LifecycleEvaluator.advance(
      execution.subject_id,
      trace_id: execution.trace_id,
      causation_id: "#{causation_id(job)}:post-trigger",
      actor_ref: actor_ref(job),
      supersedes_execution_id: supersedes_execution_id(outcome, execution),
      supersession_reason: supersession_reason(outcome)
    )
  end

  defp continue_after_state_transition(_execution, result, _outcome, _job), do: {:ok, result}

  defp lifecycle_trigger(execution, %{status: :ok}),
    do: {:execution_completed, execution.recipe_ref}

  defp lifecycle_trigger(execution, %{status: :error, failure_kind: nil}),
    do: {:execution_failed, execution.recipe_ref}

  defp lifecycle_trigger(execution, %{status: :error, failure_kind: failure_kind}),
    do: {:execution_failed, execution.recipe_ref, failure_kind}

  defp lifecycle_trigger(_execution, %{status: :cancelled}), do: nil

  defp normalize_outcome(outcome) when is_map(outcome) do
    outcome = normalize_map(outcome)

    with {:ok, receipt_id} <- fetch_non_empty(outcome, "receipt_id"),
         {:ok, status} <- normalize_status(Map.get(outcome, "status")),
         {:ok, lower_receipt} <- fetch_map(outcome, "lower_receipt"),
         {:ok, normalized_outcome} <- fetch_map(outcome, "normalized_outcome") do
      {:ok,
       %{
         receipt_id: receipt_id,
         status: status,
         lower_receipt: lower_receipt,
         normalized_outcome: normalized_outcome,
         lifecycle_hints: normalize_map(Map.get(outcome, "lifecycle_hints", %{})),
         failure_kind: normalize_failure_kind(Map.get(outcome, "failure_kind")),
         artifact_refs: normalize_artifact_refs(Map.get(outcome, "artifact_refs", [])),
         observed_at: normalize_datetime(Map.get(outcome, "observed_at"))
       }}
    end
  end

  defp normalize_outcome(_other), do: {:error, :invalid_execution_outcome}

  defp fetch_non_empty(map, key) do
    case Map.get(map, key) do
      value when is_binary(value) and byte_size(value) > 0 -> {:ok, value}
      _other -> {:error, {:missing_required_field, key}}
    end
  end

  defp fetch_map(map, key) do
    case Map.get(map, key) do
      value when is_map(value) -> {:ok, normalize_map(value)}
      _other -> {:error, {:missing_required_map, key}}
    end
  end

  defp normalize_status("ok"), do: {:ok, :ok}
  defp normalize_status("error"), do: {:ok, :error}
  defp normalize_status("cancelled"), do: {:ok, :cancelled}
  defp normalize_status(status) when status in [:ok, :error, :cancelled], do: {:ok, status}
  defp normalize_status(_other), do: {:error, :invalid_outcome_status}

  defp normalize_failure_kind(nil), do: nil
  defp normalize_failure_kind(value) when is_atom(value), do: value

  defp normalize_failure_kind(value) when is_binary(value) do
    Map.get(@failure_kind_by_string, value, :fatal_error)
  end

  defp normalize_failure_kind(_other), do: :fatal_error

  defp normalize_artifact_refs(values) when is_list(values) do
    values
    |> Enum.filter(&is_binary/1)
    |> Enum.uniq()
  end

  defp normalize_artifact_refs(_other), do: []

  defp normalize_datetime(%DateTime{} = datetime), do: datetime

  defp normalize_datetime(value) when is_binary(value) do
    case DateTime.from_iso8601(value) do
      {:ok, datetime, _offset} -> datetime
      _other -> DateTime.utc_now()
    end
  end

  defp normalize_datetime(_other), do: DateTime.utc_now()

  defp validate_lifecycle_hints(%ExecutionRecord{} = execution, outcome) do
    required_hints = required_lifecycle_hints(execution)

    if required_hints == [] or outcome.status == :cancelled do
      {:ok, outcome}
    else
      present_hints =
        outcome.lifecycle_hints
        |> Map.keys()
        |> Enum.map(&to_string/1)
        |> Enum.sort()

      missing_hints = required_hints -- present_hints

      if missing_hints == [] do
        {:ok, outcome}
      else
        {:ok, semantic_failure_for_missing_hints(outcome, missing_hints)}
      end
    end
  end

  defp required_lifecycle_hints(%ExecutionRecord{intent_snapshot: intent_snapshot}) do
    intent_snapshot
    |> Map.get("required_lifecycle_hints", [])
    |> normalize_hint_list()
  end

  defp semantic_failure_for_missing_hints(outcome, missing_hints) do
    %{
      outcome
      | status: :error,
        failure_kind: :semantic_failure,
        normalized_outcome: %{
          "reason" => "missing_required_hint",
          "missing_keys" => missing_hints,
          "reported_status" => Atom.to_string(outcome.status)
        }
    }
  end

  defp normalize_hint_list(values) when is_list(values) do
    values
    |> Enum.map(&to_string/1)
    |> Enum.reject(&(&1 == ""))
    |> Enum.uniq()
    |> Enum.sort()
  end

  defp normalize_hint_list(_other), do: []

  defp actor_ref(%Oban.Job{} = job) do
    %{kind: :execution_receipt_worker, job_id: job.id, queue: job.queue}
  end

  defp causation_id(%Oban.Job{} = job),
    do: "execution-receipt-worker:job:#{job.id}:attempt:#{job.attempt}"

  defp supersedes_execution_id(%{status: :error, failure_kind: :semantic_failure}, execution),
    do: execution.id

  defp supersedes_execution_id(_outcome, _execution), do: nil

  defp supersession_reason(%{status: :error, failure_kind: :semantic_failure}),
    do: :retry_semantic

  defp supersession_reason(_outcome), do: nil

  defp normalize_map(value) when is_map(value) do
    value
    |> Enum.map(fn {key, nested} -> {to_string(key), normalize_value(nested)} end)
    |> Map.new()
  end

  defp normalize_map(_other), do: %{}

  defp normalize_value(%DateTime{} = datetime), do: DateTime.to_iso8601(datetime)

  defp normalize_value(value) when is_map(value) do
    value
    |> Enum.map(fn {key, nested} -> {to_string(key), normalize_value(nested)} end)
    |> Map.new()
  end

  defp normalize_value(value) when is_list(value), do: Enum.map(value, &normalize_value/1)
  defp normalize_value(value) when is_atom(value), do: Atom.to_string(value)
  defp normalize_value(value), do: value

  defp cancelled_subject?(subject_id) do
    case SQL.query(Repo, @subject_status_sql, [dump_uuid!(subject_id)]) do
      {:ok, %{rows: [["cancelled"]]}} -> true
      _other -> false
    end
  end

  defp record_post_cancel_receipt(%ExecutionRecord{} = execution, outcome, %Oban.Job{} = job) do
    with {:ok, _fact} <-
           AuditFact.record(%{
             installation_id: execution.installation_id,
             subject_id: execution.subject_id,
             execution_id: execution.id,
             trace_id: execution.trace_id,
             causation_id: causation_id(job),
             fact_kind: :post_cancel_receipt,
             actor_ref: actor_ref(job),
             payload: %{
               receipt_id: outcome.receipt_id,
               status: outcome.status,
               normalized_outcome: outcome.normalized_outcome
             },
             occurred_at: outcome.observed_at
           }),
         {:ok, _warning} <-
           AuditFact.record(%{
             installation_id: execution.installation_id,
             subject_id: execution.subject_id,
             execution_id: execution.id,
             trace_id: execution.trace_id,
             causation_id: "#{causation_id(job)}:warning",
             fact_kind: :reconciliation_warning,
             actor_ref: actor_ref(job),
             payload: %{
               reason: "late_receipt_after_cancel",
               receipt_id: outcome.receipt_id
             },
             occurred_at: outcome.observed_at
           }) do
      emit_post_cancel_receipt(execution, outcome)
      :ok
    else
      {:error, error} -> {:error, error}
    end
  end

  defp dump_uuid!(uuid), do: UUID.dump!(uuid)

  defp emit_join_advance_enqueued(progress, execution, job_ref) do
    Telemetry.emit(
      [:join, :advance, :enqueued],
      %{count: 1},
      %{
        barrier_id: progress.barrier_id,
        subject_id: progress.subject_id,
        execution_id: execution.id,
        trace_id: execution.trace_id,
        join_step_ref: progress.join_step_ref,
        expected_children: progress.expected_children,
        completed_children: progress.completed_children,
        barrier_status: progress.status,
        queue: job_ref.queue,
        job_id: job_ref.job_id
      }
    )
  end

  defp emit_post_cancel_receipt(execution, outcome) do
    Telemetry.emit(
      [:receipt, :post_cancel],
      %{count: 1},
      %{
        trace_id: execution.trace_id,
        subject_id: execution.subject_id,
        execution_id: execution.id,
        installation_id: execution.installation_id,
        receipt_id: outcome.receipt_id,
        status: outcome.status
      }
    )
  end
end
