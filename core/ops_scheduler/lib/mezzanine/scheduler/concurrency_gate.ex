defmodule Mezzanine.Scheduler.ConcurrencyGate do
  @moduledoc """
  Enforces scheduler-side concurrency limits from durable run state.
  """

  require Ash.Query

  alias Mezzanine.Runs.Run

  @spec allow_dispatch?(String.t(), non_neg_integer()) :: {:ok, boolean()} | {:error, term()}
  def allow_dispatch?(tenant_id, max_concurrent_runs) when is_integer(max_concurrent_runs) do
    actor = %{tenant_id: tenant_id}

    case Run
         |> Ash.Query.set_tenant(tenant_id)
         |> Ash.Query.filter(status == :scheduled or status == :running)
         |> Ash.read(actor: actor, domain: Mezzanine.Runs) do
      {:ok, runs} -> {:ok, length(runs) < max(max_concurrent_runs, 0)}
      {:error, error} -> {:error, error}
    end
  end
end
