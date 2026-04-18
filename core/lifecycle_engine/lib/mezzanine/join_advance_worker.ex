defmodule Mezzanine.JoinAdvanceWorker do
  @moduledoc """
  Durable join worker that closes ready barriers and re-enters lifecycle through
  explicit `{:join_completed, join_step_ref}` triggers.
  """

  use Oban.Worker,
    queue: :join,
    max_attempts: 20,
    unique: [
      period: :infinity,
      states: [:available, :scheduled, :executing, :retryable],
      keys: [:subject_id, :barrier_id]
    ]

  alias Ecto.Multi
  alias Mezzanine.Execution.Repo
  alias Mezzanine.LifecycleEvaluator
  alias Mezzanine.ParallelBarrier
  alias Mezzanine.Telemetry

  @spec unique_declaration() :: keyword()
  def unique_declaration do
    [
      period: :infinity,
      states: [:available, :scheduled, :executing, :retryable],
      keys: [:subject_id, :barrier_id]
    ]
  end

  @impl Oban.Worker
  def perform(%Oban.Job{args: %{"subject_id" => subject_id, "barrier_id" => barrier_id}} = job) do
    now = DateTime.utc_now() |> DateTime.truncate(:microsecond)
    started_at = System.monotonic_time()

    with {:ok, barrier} <- ParallelBarrier.fetch(barrier_id),
         :ok <- ensure_subject_match(barrier, subject_id),
         {:ok, lifecycle_multi} <-
           LifecycleEvaluator.advance_transaction(
             subject_id,
             trigger: {:join_completed, barrier.join_step_ref},
             trace_id: barrier.trace_id,
             causation_id: causation_id(job),
             actor_ref: actor_ref(job),
             now: now
           ) do
      close_barrier_and_advance(barrier_id, barrier, lifecycle_multi, now, job, started_at)
    else
      :error -> {:error, {:barrier_not_found, barrier_id}}
      {:error, error} -> {:error, error}
    end
  end

  defp ensure_subject_match(%ParallelBarrier{subject_id: subject_id}, subject_id), do: :ok

  defp ensure_subject_match(%ParallelBarrier{subject_id: actual_subject_id}, expected_subject_id) do
    {:error, {:barrier_subject_mismatch, expected_subject_id, actual_subject_id}}
  end

  defp actor_ref(%Oban.Job{} = job) do
    %{kind: :join_advance_worker, job_id: job.id, queue: job.queue}
  end

  defp causation_id(%Oban.Job{} = job),
    do: "join-advance-worker:job:#{job.id}:attempt:#{job.attempt}"

  defp close_barrier_and_advance(barrier_id, barrier, lifecycle_multi, now, job, started_at) do
    barrier_id
    |> ParallelBarrier.close_ready_multi(now: now)
    |> Multi.merge(fn %{parallel_barrier_close: progress} ->
      if progress.duplicate? do
        Multi.new()
      else
        lifecycle_multi
      end
    end)
    |> Repo.transaction()
    |> handle_join_advance_transaction(barrier, job, started_at)
  end

  defp handle_join_advance_transaction(
         {:ok, %{parallel_barrier_close: progress}},
         barrier,
         job,
         started_at
       ) do
    emit_join_advance(progress, barrier, job, started_at)
    :ok
  end

  defp handle_join_advance_transaction(
         {:error, _step, error, _changes},
         _barrier,
         _job,
         _started_at
       ) do
    {:error, error}
  end

  defp emit_join_advance(progress, barrier, job, started_at) do
    metadata =
      %{
        barrier_id: progress.barrier_id,
        subject_id: progress.subject_id,
        trace_id: progress.trace_id,
        join_step_ref: progress.join_step_ref,
        expected_children: progress.expected_children,
        completed_children: progress.completed_children,
        barrier_status: progress.status,
        job_id: job.id,
        queue: job.queue
      }

    if progress.duplicate? do
      Telemetry.emit([:join, :advance, :idempotent_drop], %{count: 1}, metadata)
    else
      Telemetry.emit(
        [:barrier, :close],
        %{count: 1, contention_ms: Telemetry.monotonic_duration_ms(started_at)},
        Map.put(metadata, :barrier_key, barrier.barrier_key)
      )
    end
  end
end
