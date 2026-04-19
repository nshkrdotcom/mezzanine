defmodule Mezzanine.LifecycleContinuationWorker do
  @moduledoc """
  Duplicate-safe Oban worker for post-commit lifecycle continuations.
  """

  use Oban.Worker,
    queue: :lifecycle,
    max_attempts: 1,
    unique: [
      period: :infinity,
      states: [:available, :scheduled, :executing, :retryable],
      keys: [:continuation_id]
    ]

  alias Mezzanine.Execution.LifecycleContinuation
  alias Mezzanine.LifecycleEvaluator

  @spec unique_declaration() :: keyword()
  def unique_declaration do
    [
      period: :infinity,
      states: [:available, :scheduled, :executing, :retryable],
      keys: [:continuation_id]
    ]
  end

  @impl Oban.Worker
  def perform(%Oban.Job{args: %{"continuation_id" => continuation_id}}) do
    LifecycleContinuation.process(continuation_id,
      handler: &advance_lifecycle/1,
      max_attempts: 3
    )
    |> case do
      {:ok, _continuation} -> :ok
      {:error, reason} -> {:error, reason}
    end
  end

  defp advance_lifecycle(%LifecycleContinuation{} = continuation) do
    LifecycleEvaluator.advance(
      continuation.subject_id,
      trigger: trigger(continuation.target_transition),
      trace_id: continuation.trace_id,
      execution_id: continuation.execution_id,
      causation_id: "lifecycle-continuation:#{continuation.continuation_id}",
      actor_ref: %{kind: :lifecycle_continuation_worker},
      expected_installation_revision:
        Map.get(continuation.metadata || %{}, "installation_revision")
    )
    |> case do
      {:ok, _result} -> :ok
      {:error, {:stale_installation_revision, _detail}} -> {:error, :dependency_unavailable}
      {:error, {:invalid_transition, _detail}} -> {:error, :invalid_transition}
      {:error, {:no_transition, _detail}} -> {:error, :invalid_transition}
      {:error, reason} -> {:error, reason}
    end
  end

  defp trigger(target_transition) when is_binary(target_transition) do
    case String.split(target_transition, ":", parts: 3) do
      ["execution_completed", recipe_ref] ->
        {:execution_completed, recipe_ref}

      ["execution_failed", recipe_ref, failure_kind] ->
        {:execution_failed, recipe_ref, failure_kind}

      ["join_completed", join_step_ref] ->
        {:join_completed, join_step_ref}

      _other ->
        target_transition
    end
  end
end
