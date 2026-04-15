defmodule Mezzanine.Scheduler.StallDetector do
  @moduledoc """
  Identifies long-running runs that have crossed the stall threshold.
  """

  require Ash.Query

  alias Mezzanine.Runs.Run

  @spec stalled_runs(String.t(), non_neg_integer(), DateTime.t()) ::
          {:ok, [struct()]} | {:error, term()}
  def stalled_runs(tenant_id, threshold_ms, now \\ DateTime.utc_now())
      when is_integer(threshold_ms) do
    actor = %{tenant_id: tenant_id}

    with {:ok, running_runs} <-
           Run
           |> Ash.Query.set_tenant(tenant_id)
           |> Ash.Query.filter(status == :running and not is_nil(started_at))
           |> Ash.read(actor: actor, domain: Mezzanine.Runs) do
      {:ok,
       Enum.filter(running_runs, fn run ->
         DateTime.diff(now, run.started_at, :millisecond) >= threshold_ms
       end)}
    end
  end
end
