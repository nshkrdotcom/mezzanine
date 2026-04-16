defmodule Mezzanine.Execution.Dispatcher do
  @moduledoc """
  Durable single-owner dispatcher for lowering outbox rows.
  """

  alias Ecto.Adapters.SQL
  alias Mezzanine.Audit.{ExecutionLineage, ExecutionLineageStore}
  alias Mezzanine.Execution.{ExecutionRecord, Repo}

  @default_retry_delay_ms 30_000

  @type dispatch_result ::
          {:accepted, map()}
          | {:rejected, map()}
          | {:semantic_failure, map()}
          | {:error, {:retryable, term(), map()}}
          | {:error, {:terminal, term(), map()}}
          | {:error, {:semantic_failure, map()}}
          | {:error, term()}

  @spec dispatch_next(keyword()) ::
          {:ok, :empty}
          | {:ok, %{classification: atom(), execution: ExecutionRecord.t()}}
          | {:error, term()}
  def dispatch_next(opts \\ []) do
    now = Keyword.get(opts, :now, DateTime.utc_now())

    case claim_due_outbox(now) do
      {:ok, nil} ->
        {:ok, :empty}

      {:ok, claimed} ->
        submit_fun = Keyword.fetch!(opts, :submit_fun)
        actor_ref = Keyword.get(opts, :actor_ref, %{kind: :dispatcher})
        retry_delay_ms = Keyword.get(opts, :retry_delay_ms, @default_retry_delay_ms)

        claimed
        |> build_dispatch_claim()
        |> submit_fun.()
        |> classify_dispatch_result()
        |> persist_dispatch_result(claimed, actor_ref, now, retry_delay_ms, opts)

      {:error, error} ->
        {:error, error}
    end
  end

  @spec reconcile_result(ExecutionRecord.t() | Ecto.UUID.t(), term(), keyword()) ::
          {:ok, %{classification: atom(), execution: ExecutionRecord.t()}} | {:error, term()}
  def reconcile_result(execution_or_id, result, opts \\ []) do
    with {:ok, execution} <- load_execution(execution_or_id),
         {:semantic_failure, payload} <- classify_reconcile_result(result),
         {:ok, updated_execution} <-
           ExecutionRecord.record_semantic_failure(execution, %{
             lower_receipt: payload["lower_receipt"] || execution.lower_receipt,
             last_dispatch_error_payload: Map.drop(payload, ["lower_receipt"]),
             trace_id: Keyword.get(opts, :trace_id, execution.trace_id),
             causation_id: Keyword.get(opts, :causation_id, execution.causation_id),
             actor_ref: Keyword.get(opts, :actor_ref, %{kind: :reconciler})
           }) do
      {:ok, %{classification: :semantic_failure, execution: updated_execution}}
    else
      {:unsupported, unexpected} -> {:error, {:unsupported_reconcile_result, unexpected}}
      {:error, error} -> {:error, error}
    end
  end

  defp persist_dispatch_result(
         {:accepted, submission_ref, lower_receipt},
         claimed,
         actor_ref,
         _now,
         _retry_delay_ms,
         opts
       ) do
    with {:ok, execution} <- load_execution(claimed.execution_id),
         {:ok, updated_execution} <-
           ExecutionRecord.record_accepted(execution, %{
             submission_ref: submission_ref,
             lower_receipt: lower_receipt,
             trace_id: Keyword.get(opts, :trace_id, execution.trace_id),
             causation_id: Keyword.get(opts, :causation_id, execution.causation_id),
             actor_ref: actor_ref
           }),
         {:ok, _lineage} <-
           store_lineage_update(updated_execution, claimed, submission_ref, lower_receipt) do
      {:ok, %{classification: :accepted, execution: updated_execution}}
    end
  end

  defp persist_dispatch_result(
         {:retryable, error_kind, error_payload},
         claimed,
         actor_ref,
         now,
         retry_delay_ms,
         opts
       ) do
    with {:ok, execution} <- load_execution(claimed.execution_id),
         retry_at <- DateTime.add(now, retry_delay_ms, :millisecond),
         {:ok, updated_execution} <-
           ExecutionRecord.record_retryable_failure(execution, %{
             last_dispatch_error_kind: error_kind,
             last_dispatch_error_payload: error_payload,
             next_dispatch_at: retry_at,
             trace_id: Keyword.get(opts, :trace_id, execution.trace_id),
             causation_id: Keyword.get(opts, :causation_id, execution.causation_id),
             actor_ref: actor_ref
           }) do
      {:ok, %{classification: :retryable_failure, execution: updated_execution}}
    end
  end

  defp persist_dispatch_result(
         {:terminal, terminal_reason, error_payload},
         claimed,
         actor_ref,
         _now,
         _retry_delay_ms,
         opts
       ) do
    with {:ok, execution} <- load_execution(claimed.execution_id),
         {:ok, updated_execution} <-
           ExecutionRecord.record_terminal_rejection(execution, %{
             terminal_rejection_reason: terminal_reason,
             last_dispatch_error_payload: error_payload,
             trace_id: Keyword.get(opts, :trace_id, execution.trace_id),
             causation_id: Keyword.get(opts, :causation_id, execution.causation_id),
             actor_ref: actor_ref
           }) do
      {:ok, %{classification: :terminal_rejection, execution: updated_execution}}
    end
  end

  defp persist_dispatch_result(
         {:semantic_failure, payload},
         claimed,
         actor_ref,
         _now,
         _retry_delay_ms,
         opts
       ) do
    with {:ok, execution} <- load_execution(claimed.execution_id),
         {:ok, updated_execution} <-
           ExecutionRecord.record_semantic_failure(execution, %{
             lower_receipt: payload["lower_receipt"] || execution.lower_receipt,
             last_dispatch_error_payload: Map.drop(payload, ["lower_receipt"]),
             trace_id: Keyword.get(opts, :trace_id, execution.trace_id),
             causation_id: Keyword.get(opts, :causation_id, execution.causation_id),
             actor_ref: actor_ref
           }) do
      {:ok, %{classification: :semantic_failure, execution: updated_execution}}
    end
  end

  defp claim_due_outbox(now) do
    Repo.transaction(fn ->
      with {:ok, %{rows: [row], columns: columns}} <- SQL.query(Repo, claim_query(), [now, now]),
           claimed <- to_claimed_row(columns, row),
           {:ok, _result} <-
             SQL.query(Repo, mark_execution_dispatching_query(), [
               dump_uuid!(claimed.execution_id),
               now
             ]) do
        claimed
      else
        {:ok, %{rows: []}} -> nil
        {:error, error} -> Repo.rollback(error)
      end
    end)
  end

  defp claim_query do
    """
    WITH claimable AS (
      SELECT id
      FROM dispatch_outbox_entries
      WHERE status IN ('pending', 'pending_retry')
        AND available_at <= $1
      ORDER BY available_at ASC, inserted_at ASC, id ASC
      FOR UPDATE SKIP LOCKED
      LIMIT 1
    )
    UPDATE dispatch_outbox_entries
    SET status = 'dispatching',
        updated_at = $2
    WHERE id IN (SELECT id FROM claimable)
    RETURNING id,
              execution_id,
              installation_id,
              subject_id,
              trace_id,
              causation_id,
              dispatch_envelope,
              submission_dedupe_key,
              compiled_pack_revision,
              binding_snapshot
    """
  end

  defp mark_execution_dispatching_query do
    """
    UPDATE execution_records
    SET dispatch_state = 'dispatching',
        updated_at = $2
    WHERE id = $1::uuid
    """
  end

  defp build_dispatch_claim(claimed) do
    %{
      execution_id: claimed.execution_id,
      installation_id: claimed.installation_id,
      subject_id: claimed.subject_id,
      trace_id: claimed.trace_id,
      causation_id: claimed.causation_id,
      outbox_id: claimed.outbox_id,
      submission_dedupe_key: claimed.submission_dedupe_key,
      compiled_pack_revision: claimed.compiled_pack_revision,
      binding_snapshot: claimed.binding_snapshot,
      dispatch_envelope: claimed.dispatch_envelope
    }
  end

  defp classify_dispatch_result({:accepted, payload}) do
    payload = normalize_map(payload)
    submission_ref = Map.get(payload, "submission_ref", %{})
    lower_receipt = Map.get(payload, "lower_receipt", %{})
    {:accepted, submission_ref, lower_receipt}
  end

  defp classify_dispatch_result({:rejected, payload}) do
    payload = normalize_map(payload)
    {:terminal, Map.get(payload, "reason", "terminal_rejection"), Map.delete(payload, "reason")}
  end

  defp classify_dispatch_result({:semantic_failure, payload}) do
    {:semantic_failure, normalize_map(payload)}
  end

  defp classify_dispatch_result({:error, {:retryable, error_kind, payload}}) do
    {:retryable, normalize_reason(error_kind), normalize_map(payload)}
  end

  defp classify_dispatch_result({:error, {:terminal, terminal_reason, payload}}) do
    {:terminal, normalize_reason(terminal_reason), normalize_map(payload)}
  end

  defp classify_dispatch_result({:error, {:semantic_failure, payload}}) do
    {:semantic_failure, normalize_map(payload)}
  end

  defp classify_dispatch_result({:error, error}) do
    {:retryable, normalize_reason(error), %{"error" => normalize_value(error)}}
  end

  defp classify_dispatch_result(other) do
    {:retryable, "unexpected_dispatch_result", %{"result" => normalize_value(other)}}
  end

  defp classify_reconcile_result({:semantic_failure, payload}),
    do: {:semantic_failure, normalize_map(payload)}

  defp classify_reconcile_result({:error, {:semantic_failure, payload}}),
    do: {:semantic_failure, normalize_map(payload)}

  defp classify_reconcile_result(other), do: {:unsupported, other}

  defp load_execution(%ExecutionRecord{} = execution), do: {:ok, execution}

  defp load_execution(execution_id) when is_binary(execution_id) do
    Ash.get(ExecutionRecord, execution_id)
  end

  defp store_lineage_update(execution, claimed, submission_ref, lower_receipt) do
    existing_lineage =
      case ExecutionLineageStore.fetch(execution.id) do
        {:ok, lineage} -> Map.from_struct(lineage)
        {:error, _error} -> base_lineage_attrs(execution, claimed)
      end

    existing_lineage
    |> Map.merge(%{
      trace_id: execution.trace_id,
      causation_id: execution.causation_id,
      installation_id: execution.installation_id,
      subject_id: execution.subject_id,
      execution_id: execution.id,
      dispatch_outbox_entry_id: claimed.outbox_id,
      citadel_submission_id: submission_ref["id"] || submission_ref["submission_id"],
      ji_submission_key:
        lower_receipt["ji_submission_key"] || submission_ref["ji_submission_key"] ||
          claimed.submission_dedupe_key,
      lower_run_id: lower_receipt["run_id"],
      lower_attempt_id: lower_receipt["attempt_id"],
      artifact_refs:
        List.wrap(lower_receipt["artifact_refs"] || existing_lineage[:artifact_refs] || [])
    })
    |> ExecutionLineage.new!()
    |> ExecutionLineageStore.store()
  end

  defp base_lineage_attrs(execution, claimed) do
    %{
      trace_id: execution.trace_id,
      causation_id: execution.causation_id,
      installation_id: execution.installation_id,
      subject_id: execution.subject_id,
      execution_id: execution.id,
      dispatch_outbox_entry_id: claimed.outbox_id,
      artifact_refs: []
    }
  end

  defp to_claimed_row(columns, row) do
    columns
    |> Enum.zip(row)
    |> Map.new()
    |> then(fn claimed ->
      %{
        outbox_id: normalize_uuid(claimed["id"]),
        execution_id: normalize_uuid(claimed["execution_id"]),
        installation_id: claimed["installation_id"],
        subject_id: normalize_uuid(claimed["subject_id"]),
        trace_id: claimed["trace_id"],
        causation_id: claimed["causation_id"],
        dispatch_envelope: normalize_map(claimed["dispatch_envelope"]),
        submission_dedupe_key: claimed["submission_dedupe_key"],
        compiled_pack_revision: claimed["compiled_pack_revision"],
        binding_snapshot: normalize_map(claimed["binding_snapshot"])
      }
    end)
  end

  defp normalize_reason(reason) when is_binary(reason), do: reason
  defp normalize_reason(reason) when is_atom(reason), do: Atom.to_string(reason)
  defp normalize_reason(reason), do: inspect(reason)

  defp dump_uuid!(value) when is_binary(value) do
    case Ecto.UUID.dump(value) do
      {:ok, uuid} -> uuid
      :error -> value
    end
  end

  defp dump_uuid!(value), do: value

  defp normalize_uuid(value) when is_binary(value) do
    case Ecto.UUID.cast(value) do
      {:ok, uuid} ->
        uuid

      :error ->
        case Ecto.UUID.load(value) do
          {:ok, uuid} -> uuid
          :error -> value
        end
    end
  end

  defp normalize_uuid(value), do: value

  defp normalize_key(key) when is_binary(key), do: key
  defp normalize_key(key) when is_atom(key), do: Atom.to_string(key)
  defp normalize_key(key), do: inspect(key)

  defp normalize_map(value) when is_map(value) or is_list(value), do: normalize_value(value)
  defp normalize_map(nil), do: %{}
  defp normalize_map(other), do: %{"value" => normalize_value(other)}

  defp normalize_value(%DateTime{} = value), do: DateTime.to_iso8601(value)
  defp normalize_value(%_{} = value), do: value |> Map.from_struct() |> normalize_value()

  defp normalize_value(value) when is_map(value) do
    value
    |> Enum.map(fn {key, nested} -> {normalize_key(key), normalize_value(nested)} end)
    |> Map.new()
  end

  defp normalize_value(value) when is_list(value), do: Enum.map(value, &normalize_value/1)
  defp normalize_value(value) when is_atom(value), do: Atom.to_string(value)
  defp normalize_value(value), do: value
end
