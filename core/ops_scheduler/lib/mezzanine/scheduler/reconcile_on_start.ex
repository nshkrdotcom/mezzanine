defmodule Mezzanine.Scheduler.ReconcileOnStart do
  @moduledoc """
  Reconstructs scheduler-visible durable state on process start.
  """

  require Ash.Query

  alias Mezzanine.Runs.Run
  alias Mezzanine.Scheduler.StallDetector

  @default_stall_threshold_ms 900_000

  @spec reconcile(String.t(), DateTime.t(), keyword()) :: {:ok, map()} | {:error, term()}
  def reconcile(tenant_id, now \\ DateTime.utc_now(), opts \\ []) do
    actor = %{tenant_id: tenant_id}
    stall_threshold_ms = Keyword.get(opts, :stall_threshold_ms, @default_stall_threshold_ms)

    with {:ok, scheduled_runs} <- runs_with_status(actor, tenant_id, :scheduled),
         {:ok, running_runs} <- runs_with_status(actor, tenant_id, :running),
         {:ok, stalled_runs} <- StallDetector.stalled_runs(tenant_id, stall_threshold_ms, now) do
      {:ok,
       %{
         scheduled_runs: scheduled_runs,
         running_runs: running_runs,
         stalled_runs: stalled_runs
       }}
    end
  end

  defp runs_with_status(actor, tenant_id, status) do
    Run
    |> Ash.Query.set_tenant(tenant_id)
    |> Ash.Query.filter(status == ^status)
    |> Ash.read(actor: actor, domain: Mezzanine.Runs)
  end
end
