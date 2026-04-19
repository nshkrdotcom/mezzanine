defmodule Mezzanine.RuntimeScheduler.ReconcileOnStart do
  @moduledoc """
  Records Temporal workflow handoffs for executions stranded in `:dispatching`
  when the runtime restarts and claims receipt-reconciliation waves without
  reintroducing Oban saga queues.
  """

  alias Ecto.Adapters.SQL
  alias Mezzanine.Execution.{ExecutionRecord, Repo}
  alias Mezzanine.Telemetry

  @default_actor_ref %{kind: :runtime_scheduler, phase: :reconcile_on_start}

  @type summary :: %{
          dispatch_recovered_count: non_neg_integer(),
          dispatch_recovered_execution_ids: [Ecto.UUID.t()],
          reconcile_handoff_count: non_neg_integer(),
          reconcile_handoff_execution_ids: [Ecto.UUID.t()]
        }

  @claim_reconcile_wave_sql """
  UPDATE execution_records
  SET last_reconcile_wave_id = $2,
      updated_at = $3
  WHERE id = $1::uuid
    AND dispatch_state = 'awaiting_receipt'
    AND COALESCE(last_reconcile_wave_id, '') <> $2
  RETURNING id
  """

  @spec reconcile(String.t(), DateTime.t(), keyword()) :: {:ok, summary()} | {:error, term()}
  def reconcile(installation_id, now \\ DateTime.utc_now(), opts \\ [])
      when is_binary(installation_id) do
    actor_ref = Keyword.get(opts, :actor_ref, @default_actor_ref)
    wave_id = Keyword.get(opts, :wave_id, startup_wave_id(now))

    with {:ok, candidates} <- candidate_executions(installation_id) do
      recover_candidates(candidates, now, actor_ref, wave_id)
    end
  end

  defp recover_candidates(candidates, now, actor_ref, wave_id) do
    summary = %{
      dispatch_recovered_count: 0,
      dispatch_recovered_execution_ids: [],
      reconcile_handoff_count: 0,
      reconcile_handoff_execution_ids: []
    }

    Enum.reduce_while(candidates, {:ok, summary}, fn candidate, {:ok, summary} ->
      execution_id = candidate.id

      with {:ok, execution} <- Ash.get(ExecutionRecord, execution_id),
           {:ok, updated_summary} <-
             recover_candidate(
               execution,
               candidate.dispatch_state,
               now,
               actor_ref,
               wave_id,
               summary
             ) do
        {:cont, {:ok, updated_summary}}
      else
        {:error, error} -> {:halt, {:error, error}}
      end
    end)
    |> case do
      {:ok, recovered_summary} ->
        {:ok,
         %{
           recovered_summary
           | dispatch_recovered_execution_ids:
               Enum.reverse(recovered_summary.dispatch_recovered_execution_ids),
             reconcile_handoff_execution_ids:
               Enum.reverse(recovered_summary.reconcile_handoff_execution_ids)
         }}

      {:error, error} ->
        {:error, error}
    end
  end

  defp recover_candidate(execution, "dispatching", now, actor_ref, _wave_id, summary) do
    with {:ok, updated_execution} <-
           ExecutionRecord.record_restart_recovery(
             execution,
             restart_recovery_attrs(execution, now, actor_ref)
           ),
         {:ok, _workflow_handoff} <-
           ExecutionRecord.enqueue_dispatch(updated_execution, scheduled_at: now) do
      Telemetry.emit(
        [:dispatch, :ambiguous],
        %{count: 1},
        %{
          trace_id: updated_execution.trace_id,
          subject_id: updated_execution.subject_id,
          execution_id: updated_execution.id,
          submission_dedupe_key: updated_execution.submission_dedupe_key,
          tenant_id: updated_execution.tenant_id,
          installation_id: updated_execution.installation_id,
          previous_dispatch_state: execution.dispatch_state
        }
      )

      {:ok,
       %{
         summary
         | dispatch_recovered_count: summary.dispatch_recovered_count + 1,
           dispatch_recovered_execution_ids: [
             updated_execution.id | summary.dispatch_recovered_execution_ids
           ]
       }}
    end
  end

  defp recover_candidate(execution, "awaiting_receipt", now, _actor_ref, wave_id, summary) do
    with {:ok, claimed?} <- claim_and_enqueue_reconcile(execution.id, wave_id, now) do
      emit_startup_reconcile(execution, wave_id, claimed?)
      {:ok, reconcile_summary(summary, execution.id, claimed?)}
    end
  end

  defp candidate_executions(installation_id) do
    case SQL.query(Repo, candidate_query(), [installation_id]) do
      {:ok, %{rows: rows}} ->
        {:ok,
         Enum.map(rows, fn [execution_id, dispatch_state] ->
           %{id: execution_id, dispatch_state: dispatch_state}
         end)}

      {:error, error} ->
        {:error, error}
    end
  end

  defp restart_recovery_attrs(execution, now, actor_ref) do
    %{
      next_dispatch_at: now,
      last_dispatch_error_payload: %{
        "reason" => "dispatch_worker_restarted",
        "recovered_at" => DateTime.to_iso8601(now),
        "previous_dispatch_state" => Atom.to_string(execution.dispatch_state)
      },
      trace_id: execution.trace_id,
      causation_id: "restart-recovery:#{execution.id}:#{DateTime.to_unix(now, :microsecond)}",
      actor_ref: actor_ref
    }
  end

  defp claim_and_enqueue_reconcile(execution_id, wave_id, now) do
    dumped_execution_id = Ecto.UUID.dump!(execution_id)

    Repo.transaction(fn ->
      case SQL.query(Repo, @claim_reconcile_wave_sql, [dumped_execution_id, wave_id, now]) do
        {:ok, %{rows: [[_claimed_execution_id]]}} ->
          temporal_receipt_reconcile_handoff!(execution_id, now)

        {:ok, %{rows: []}} ->
          false

        {:error, error} ->
          Repo.rollback(error)
      end
    end)
  end

  defp temporal_receipt_reconcile_handoff!(execution_id, _now) do
    # The execution workflow owns lower receipt reconciliation after M31. The
    # scheduler only claims the startup wave so duplicate nodes do not all emit
    # the same recovery intent.
    if is_binary(execution_id), do: true, else: Repo.rollback(:invalid_execution_id)
  end

  defp reconcile_summary(summary, execution_id, claimed?) do
    %{
      summary
      | reconcile_handoff_count: summary.reconcile_handoff_count + if(claimed?, do: 1, else: 0),
        reconcile_handoff_execution_ids: [execution_id | summary.reconcile_handoff_execution_ids]
    }
  end

  defp startup_wave_id(%DateTime{} = now) do
    now
    |> DateTime.truncate(:second)
    |> DateTime.to_iso8601()
  end

  defp candidate_query do
    """
    SELECT id, dispatch_state
    FROM execution_records
    WHERE installation_id = $1
      AND dispatch_state IN ('dispatching', 'awaiting_receipt')
    ORDER BY inserted_at ASC, id ASC
    """
  end

  defp emit_startup_reconcile(%ExecutionRecord{} = execution, wave_id, true) do
    Telemetry.emit(
      [:startup, :reconcile, :handoff_recorded],
      %{count: 1},
      %{
        event_name: "startup.reconcile.handoff_recorded",
        trace_id: execution.trace_id,
        subject_id: execution.subject_id,
        execution_id: execution.id,
        submission_dedupe_key: execution.submission_dedupe_key,
        tenant_id: execution.tenant_id,
        installation_id: execution.installation_id,
        wave_id: wave_id
      }
    )
  end

  defp emit_startup_reconcile(%ExecutionRecord{} = execution, wave_id, false) do
    Telemetry.emit(
      [:startup, :reconcile, :unique_drop],
      %{count: 1},
      %{
        event_name: "startup.reconcile.unique_drop",
        trace_id: execution.trace_id,
        subject_id: execution.subject_id,
        execution_id: execution.id,
        submission_dedupe_key: execution.submission_dedupe_key,
        tenant_id: execution.tenant_id,
        installation_id: execution.installation_id,
        wave_id: wave_id
      }
    )
  end
end
