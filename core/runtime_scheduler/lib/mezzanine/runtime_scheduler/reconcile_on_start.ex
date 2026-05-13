defmodule Mezzanine.RuntimeScheduler.ReconcileOnStart do
  @moduledoc """
  Records Temporal workflow handoffs for executions stranded in in-flight
  dispatch states when the runtime restarts and claims receipt-reconciliation
  waves without reintroducing Oban saga queues.
  """

  alias Ecto.Adapters.SQL
  alias Mezzanine.Execution.{DispatchState, ExecutionRecord, Repo}
  alias Mezzanine.Telemetry
  alias Mezzanine.WorkspaceEngine.{Cleanup, WorkspaceRecord}

  require Logger

  @default_actor_ref %{kind: :runtime_scheduler, phase: :reconcile_on_start}
  @in_flight_states DispatchState.in_flight_state_strings()
  @accepted_active_states DispatchState.accepted_active_state_strings()
  @startup_candidate_states DispatchState.startup_reconcile_candidate_state_strings()

  @type summary :: %{
          dispatch_recovered_count: non_neg_integer(),
          dispatch_recovered_execution_ids: [Ecto.UUID.t()],
          reconcile_handoff_count: non_neg_integer(),
          reconcile_handoff_execution_ids: [Ecto.UUID.t()],
          last_terminal_cleanup_at: DateTime.t(),
          terminal_cleanup_status: String.t(),
          terminal_cleanup_candidate_count: non_neg_integer(),
          terminal_cleanup_cleaned_count: non_neg_integer(),
          terminal_cleanup_skipped_count: non_neg_integer(),
          terminal_cleanup_failed_count: non_neg_integer(),
          terminal_cleanup_receipt_refs: [String.t()],
          terminal_cleanup_failures: [map()],
          terminal_cleanup_fetch_failed?: boolean()
        }

  @claim_reconcile_wave_sql """
  UPDATE execution_records
  SET last_reconcile_wave_id = $2,
      updated_at = $3
  WHERE id = $1::uuid
    AND dispatch_state = ANY($4)
    AND COALESCE(last_reconcile_wave_id, '') <> $2
  RETURNING id
  """

  @spec reconcile(String.t(), DateTime.t(), keyword()) :: {:ok, summary()} | {:error, term()}
  def reconcile(installation_id, now \\ DateTime.utc_now(), opts \\ [])
      when is_binary(installation_id) do
    actor_ref = Keyword.get(opts, :actor_ref, @default_actor_ref)
    wave_id = Keyword.get(opts, :wave_id, startup_wave_id(now))

    with {:ok, terminal_cleanup_summary} <-
           run_terminal_cleanup(installation_id, now, opts),
         {:ok, candidates} <- candidate_executions(installation_id) do
      recover_candidates(candidates, now, actor_ref, wave_id, terminal_cleanup_summary)
    end
  end

  defp recover_candidates(candidates, now, actor_ref, wave_id, terminal_cleanup_summary) do
    summary =
      Map.merge(terminal_cleanup_summary, %{
        dispatch_recovered_count: 0,
        dispatch_recovered_execution_ids: [],
        reconcile_handoff_count: 0,
        reconcile_handoff_execution_ids: []
      })

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

  defp recover_candidate(execution, dispatch_state, now, actor_ref, _wave_id, summary)
       when dispatch_state in @in_flight_states do
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

  defp recover_candidate(execution, dispatch_state, now, _actor_ref, wave_id, summary)
       when dispatch_state in @accepted_active_states do
    with {:ok, claimed?} <- claim_and_enqueue_reconcile(execution.id, wave_id, now) do
      emit_startup_reconcile(execution, wave_id, claimed?)
      {:ok, reconcile_summary(summary, execution.id, claimed?)}
    end
  end

  defp candidate_executions(installation_id) do
    case SQL.query(Repo, candidate_query(), [installation_id, @startup_candidate_states]) do
      {:ok, %{rows: rows}} ->
        {:ok,
         Enum.map(rows, fn [execution_id, dispatch_state] ->
           %{id: execution_id, dispatch_state: dispatch_state}
         end)}

      {:error, error} ->
        {:error, error}
    end
  end

  defp run_terminal_cleanup(installation_id, now, opts) do
    case terminal_cleanup_candidates(installation_id, opts) do
      {:ok, candidates} ->
        summary = cleanup_terminal_candidates(candidates, now, opts)
        emit_terminal_cleanup_completed(installation_id, summary)
        {:ok, summary}

      {:error, reason} ->
        Logger.warning(
          "Skipping startup terminal workspace cleanup; failed to fetch terminal candidates: #{inspect(reason)}"
        )

        summary = terminal_cleanup_fetch_failed_summary(now, reason)
        emit_terminal_cleanup_fetch_failed(installation_id, reason, summary)
        {:ok, summary}
    end
  end

  defp terminal_cleanup_candidates(installation_id, opts) do
    if Keyword.has_key?(opts, :terminal_cleanup_candidates) do
      opts
      |> Keyword.get(:terminal_cleanup_candidates)
      |> normalize_candidates()
    else
      fetch_terminal_cleanup_candidates(
        installation_id,
        Keyword.get(opts, :terminal_cleanup_fetcher)
      )
    end
  end

  defp fetch_terminal_cleanup_candidates(_installation_id, nil), do: {:ok, []}

  defp fetch_terminal_cleanup_candidates(installation_id, fetcher)
       when is_function(fetcher, 1) do
    with {:ok, candidates} <- fetcher.(installation_id) do
      normalize_candidates(candidates)
    end
  end

  defp fetch_terminal_cleanup_candidates(_installation_id, _other),
    do: {:error, :invalid_terminal_cleanup_fetcher}

  defp normalize_candidates(candidates) do
    {:ok, candidates |> List.wrap() |> Enum.map(&normalize_candidate/1)}
  end

  defp cleanup_terminal_candidates(candidates, now, opts) do
    cleanup_fun = Keyword.get(opts, :workspace_cleanup_fun, &default_workspace_cleanup/1)

    empty =
      %{
        last_terminal_cleanup_at: now,
        terminal_cleanup_status: "completed",
        terminal_cleanup_candidate_count: length(candidates),
        terminal_cleanup_cleaned_count: 0,
        terminal_cleanup_skipped_count: 0,
        terminal_cleanup_failed_count: 0,
        terminal_cleanup_receipt_refs: [],
        terminal_cleanup_failures: [],
        terminal_cleanup_fetch_failed?: false
      }

    candidates
    |> Enum.reduce(empty, &cleanup_terminal_candidate(&1, &2, cleanup_fun))
    |> finalize_terminal_cleanup_summary()
  end

  defp cleanup_terminal_candidate(candidate, summary, cleanup_fun)
       when is_function(cleanup_fun, 1) do
    case cleanup_fun.(candidate) do
      {:ok, receipt} ->
        record_cleanup_receipt(summary, normalize_receipt(receipt, candidate))

      {:error, reason} ->
        record_cleanup_failure(summary, candidate, reason)

      other ->
        record_cleanup_failure(summary, candidate, {:invalid_cleanup_result, other})
    end
  rescue
    exception ->
      record_cleanup_failure(summary, candidate, {:exception, exception.__struct__})
  end

  defp record_cleanup_receipt(summary, receipt) do
    summary
    |> Map.update!(:terminal_cleanup_receipt_refs, fn refs ->
      maybe_append(refs, receipt.receipt_ref)
    end)
    |> increment_cleanup_count(receipt.status)
  end

  defp increment_cleanup_count(summary, status)
       when status in [:removed, "removed", :cleaned, "cleaned"] do
    Map.update!(summary, :terminal_cleanup_cleaned_count, &(&1 + 1))
  end

  defp increment_cleanup_count(summary, _status) do
    Map.update!(summary, :terminal_cleanup_skipped_count, &(&1 + 1))
  end

  defp record_cleanup_failure(summary, candidate, reason) do
    failure = %{
      identifier: candidate.identifier,
      workspace_ref: candidate.workspace_ref,
      reason: reason
    }

    summary
    |> Map.update!(:terminal_cleanup_failed_count, &(&1 + 1))
    |> Map.update!(:terminal_cleanup_failures, &(&1 ++ [failure]))
  end

  defp finalize_terminal_cleanup_summary(summary) do
    status =
      if summary.terminal_cleanup_failed_count > 0, do: "warning", else: "completed"

    %{summary | terminal_cleanup_status: status}
  end

  defp terminal_cleanup_fetch_failed_summary(now, reason) do
    %{
      last_terminal_cleanup_at: now,
      terminal_cleanup_status: "fetch_failed",
      terminal_cleanup_candidate_count: 0,
      terminal_cleanup_cleaned_count: 0,
      terminal_cleanup_skipped_count: 0,
      terminal_cleanup_failed_count: 0,
      terminal_cleanup_receipt_refs: [],
      terminal_cleanup_failures: [],
      terminal_cleanup_fetch_failed?: true,
      terminal_cleanup_error: reason
    }
  end

  defp default_workspace_cleanup(candidate) do
    case candidate.workspace_record do
      %WorkspaceRecord{} = workspace ->
        Cleanup.remove(workspace)

      _other ->
        {:ok,
         %{
           receipt_ref:
             "cleanup-receipt://#{candidate.identifier || candidate.workspace_ref || "unknown"}/skipped",
           workspace_ref: candidate.workspace_ref,
           status: "skipped",
           reason: "workspace_cleanup_not_configured",
           path_redacted?: true
         }}
    end
  end

  defp normalize_candidate(%_{} = candidate),
    do: candidate |> Map.from_struct() |> normalize_candidate()

  defp normalize_candidate(candidate) when is_list(candidate),
    do: candidate |> Map.new() |> normalize_candidate()

  defp normalize_candidate(%{} = candidate) do
    %{
      subject_id: value(candidate, :subject_id),
      source_ref: value(candidate, :source_ref),
      identifier: value(candidate, :identifier) || value(candidate, :source_identifier),
      workspace_ref: value(candidate, :workspace_ref),
      workspace_record: workspace_record(value(candidate, :workspace_record))
    }
  end

  defp normalize_candidate(candidate) do
    %{
      subject_id: nil,
      source_ref: nil,
      identifier: to_string(candidate),
      workspace_ref: nil,
      workspace_record: nil
    }
  end

  defp workspace_record(%WorkspaceRecord{} = record), do: record
  defp workspace_record(_other), do: nil

  defp normalize_receipt(%_{} = receipt, candidate),
    do: receipt |> Map.from_struct() |> normalize_receipt(candidate)

  defp normalize_receipt(receipt, candidate) when is_list(receipt),
    do: receipt |> Map.new() |> normalize_receipt(candidate)

  defp normalize_receipt(%{} = receipt, candidate) do
    %{
      receipt_ref: value(receipt, :receipt_ref),
      workspace_ref: value(receipt, :workspace_ref) || candidate.workspace_ref,
      status: value(receipt, :status) || "skipped"
    }
  end

  defp normalize_receipt(_receipt, candidate) do
    %{receipt_ref: nil, workspace_ref: candidate.workspace_ref, status: "skipped"}
  end

  defp maybe_append(values, nil), do: values
  defp maybe_append(values, value), do: values ++ [value]

  defp value(map, key) when is_map(map),
    do: Map.get(map, key) || Map.get(map, Atom.to_string(key))

  defp emit_terminal_cleanup_completed(installation_id, summary) do
    Telemetry.emit(
      [:startup, :terminal_cleanup, :completed],
      %{count: summary.terminal_cleanup_candidate_count},
      %{
        event_name: "startup.terminal_cleanup.completed",
        installation_id: installation_id,
        cleanup_status: summary.terminal_cleanup_status,
        cleaned_count: summary.terminal_cleanup_cleaned_count,
        skipped_count: summary.terminal_cleanup_skipped_count,
        failed_count: summary.terminal_cleanup_failed_count,
        receipt_refs: summary.terminal_cleanup_receipt_refs
      }
    )
  end

  defp emit_terminal_cleanup_fetch_failed(installation_id, reason, summary) do
    Telemetry.emit(
      [:startup, :terminal_cleanup, :fetch_failed],
      %{count: 1},
      %{
        event_name: "startup.terminal_cleanup.fetch_failed",
        installation_id: installation_id,
        cleanup_status: summary.terminal_cleanup_status,
        reason: reason
      }
    )
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
      case SQL.query(Repo, @claim_reconcile_wave_sql, [
             dumped_execution_id,
             wave_id,
             now,
             @accepted_active_states
           ]) do
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
      AND dispatch_state = ANY($2)
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
