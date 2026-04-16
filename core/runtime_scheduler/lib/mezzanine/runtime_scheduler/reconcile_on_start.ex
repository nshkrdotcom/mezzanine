defmodule Mezzanine.RuntimeScheduler.ReconcileOnStart do
  @moduledoc """
  Requeues dispatches that were claimed but not durably classified before the
  runtime restarted.
  """

  alias Ecto.Adapters.SQL
  alias Mezzanine.Execution.{ExecutionRecord, Repo}

  @default_actor_ref %{kind: :runtime_scheduler, phase: :reconcile_on_start}

  @type summary :: %{
          recovered_count: non_neg_integer(),
          recovered_execution_ids: [Ecto.UUID.t()]
        }

  @spec reconcile(String.t(), DateTime.t(), keyword()) :: {:ok, summary()} | {:error, term()}
  def reconcile(installation_id, now \\ DateTime.utc_now(), opts \\ [])
      when is_binary(installation_id) do
    actor_ref = Keyword.get(opts, :actor_ref, @default_actor_ref)

    with {:ok, execution_ids} <- candidate_execution_ids(installation_id),
         {:ok, recovered} <- recover_candidates(execution_ids, now, actor_ref) do
      {:ok,
       %{
         recovered_count: length(recovered),
         recovered_execution_ids: Enum.map(recovered, & &1.id)
       }}
    end
  end

  defp recover_candidates(execution_ids, now, actor_ref) do
    Enum.reduce_while(execution_ids, {:ok, []}, fn execution_id, {:ok, recovered} ->
      with {:ok, execution} <- Ash.get(ExecutionRecord, execution_id),
           {:ok, updated_execution} <-
             ExecutionRecord.record_restart_recovery(
               execution,
               restart_recovery_attrs(execution, now, actor_ref)
             ) do
        {:cont, {:ok, [updated_execution | recovered]}}
      else
        {:error, error} -> {:halt, {:error, error}}
      end
    end)
    |> case do
      {:ok, recovered} -> {:ok, Enum.reverse(recovered)}
      {:error, error} -> {:error, error}
    end
  end

  defp candidate_execution_ids(installation_id) do
    case SQL.query(Repo, candidate_query(), [installation_id]) do
      {:ok, %{rows: rows}} -> {:ok, Enum.map(rows, fn [execution_id] -> execution_id end)}
      {:error, error} -> {:error, error}
    end
  end

  defp restart_recovery_attrs(execution, now, actor_ref) do
    %{
      next_dispatch_at: now,
      last_dispatch_error_payload: %{
        "reason" => "dispatcher_restarted",
        "recovered_at" => DateTime.to_iso8601(now),
        "previous_dispatch_state" => Atom.to_string(execution.dispatch_state)
      },
      trace_id: execution.trace_id,
      causation_id: "restart-recovery:#{execution.id}:#{DateTime.to_unix(now, :microsecond)}",
      actor_ref: actor_ref
    }
  end

  defp candidate_query do
    """
    SELECT e.id
    FROM execution_records AS e
    INNER JOIN dispatch_outbox_entries AS o ON o.execution_id = e.id
    WHERE e.installation_id = $1
      AND e.dispatch_state = 'dispatching'
      AND o.status = 'dispatching'
    ORDER BY o.available_at ASC, o.inserted_at ASC, o.id ASC
    """
  end
end
