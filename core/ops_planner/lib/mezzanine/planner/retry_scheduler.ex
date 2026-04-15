defmodule Mezzanine.Planner.RetryScheduler do
  @moduledoc """
  Pure retry timing derivation.
  """

  @spec next_retry(map(), map(), DateTime.t()) :: {:ok, map()} | {:error, :retry_exhausted}
  def next_retry(failure_state, retry_profile, now \\ DateTime.utc_now()) do
    attempts = Map.get(failure_state, :attempts, Map.get(failure_state, "attempts", 0))
    max_attempts = Map.get(retry_profile, :max_attempts, 1)
    strategy = Map.get(retry_profile, :strategy, :none)

    if attempts >= max_attempts do
      {:error, :retry_exhausted}
    else
      delay_ms = delay_ms(strategy, attempts + 1, retry_profile)

      {:ok,
       %{
         attempt: attempts + 1,
         delay_ms: delay_ms,
         due_at: DateTime.add(now, delay_ms, :millisecond)
       }}
    end
  end

  defp delay_ms(:none, _attempt, _profile), do: 0

  defp delay_ms(:linear, attempt, profile) do
    initial = Map.get(profile, :initial_backoff_ms, 0)
    max_backoff = Map.get(profile, :max_backoff_ms, initial * attempt)
    min(initial * attempt, max_backoff)
  end

  defp delay_ms(:exponential, attempt, profile) do
    initial = Map.get(profile, :initial_backoff_ms, 0)
    max_backoff = Map.get(profile, :max_backoff_ms, initial)
    min(initial * trunc(:math.pow(2, max(attempt - 1, 0))), max_backoff)
  end

  defp delay_ms(_strategy, _attempt, _profile), do: 0
end
