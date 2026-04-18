defmodule Mezzanine.DecisionExpiryWorker do
  @moduledoc """
  Delayed SLA expiry worker for pending decisions.
  """

  use Oban.Worker,
    queue: :decision_expiry,
    max_attempts: 20,
    unique: [
      period: :infinity,
      states: [:available, :scheduled, :executing, :retryable],
      keys: [:decision_id]
    ]

  alias Mezzanine.DecisionCommands

  @spec unique_declaration() :: keyword()
  def unique_declaration do
    [
      period: :infinity,
      states: [:available, :scheduled, :executing, :retryable],
      keys: [:decision_id]
    ]
  end

  @impl Oban.Worker
  def perform(%Oban.Job{args: %{"decision_id" => decision_id}} = job) do
    case DecisionCommands.expire(
           decision_id,
           %{
             trace_id: "decision-expiry-worker:#{decision_id}",
             causation_id: causation_id(job),
             actor_ref: actor_ref(job),
             reason: "decision_sla_expired",
             now: DateTime.utc_now()
           },
           current_job_id: job.id
         ) do
      {:ok, _decision} ->
        :ok

      {:error, {:decision_not_pending, _lifecycle_state}} ->
        :discard

      {:error, {:decision_not_found, _decision_id}} ->
        :discard

      {:error, error} ->
        {:error, error}
    end
  end

  defp actor_ref(job) do
    %{
      kind: :decision_expiry_worker,
      job_id: job.id,
      queue: job.queue
    }
  end

  defp causation_id(job), do: "decision-expiry-worker:job:#{job.id}:attempt:#{job.attempt}"
end
