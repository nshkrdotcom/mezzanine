defmodule Mezzanine.ExecutionDispatchWorker do
  @moduledoc """
  Acceptance-only dispatch worker that always checks lower dedupe state before
  attempting a fresh submission.
  """

  use Oban.Worker,
    queue: :dispatch,
    max_attempts: 20,
    unique: [
      period: :infinity,
      states: [:available, :scheduled, :executing, :retryable],
      keys: [:execution_id]
    ]

  alias Ecto.Adapters.SQL
  alias Ecto.UUID
  alias Mezzanine.Execution.ExecutionRecord
  alias Mezzanine.Execution.Repo
  alias Mezzanine.JobOutbox
  alias Mezzanine.Leasing
  alias Mezzanine.LowerGateway
  alias Mezzanine.LowerGatewayCircuit
  alias Mezzanine.Telemetry

  @default_retry_delay_ms 30_000
  @paused_snooze_ms 86_400_000
  @subject_lock_sql """
  SELECT pg_advisory_xact_lock(hashtext('mezzanine.subject:' || $1))
  """
  @subject_status_sql """
  SELECT status
  FROM subject_records
  WHERE id = $1::uuid
  FOR UPDATE
  """

  @type dispatch_state ::
          :ok | :discard | {:snooze, pos_integer()} | {:error, term()}

  @spec unique_declaration() :: keyword()
  def unique_declaration do
    [
      period: :infinity,
      states: [:available, :scheduled, :executing, :retryable],
      keys: [:execution_id]
    ]
  end

  @impl Oban.Worker
  def perform(
        %Oban.Job{id: job_id, attempt: attempt, args: %{"execution_id" => execution_id}} = job
      ) do
    with {:ok, claim_result} <- claim_execution(execution_id, job_id, attempt) do
      case claim_result do
        :paused ->
          JobOutbox.snooze_response(@paused_snooze_ms)

        :terminal ->
          :ok

        %ExecutionRecord{} = active_execution ->
          dispatch_or_resume(active_execution, job)

        {:error, error} ->
          {:error, error}
      end
    end
  end

  defp claim_execution(execution_id, job_id, attempt) do
    Repo.transaction(fn ->
      with {:ok, execution} <- Ash.get(ExecutionRecord, execution_id),
           :ok <- lock_subject(execution.subject_id),
           {:ok, subject_status} <- load_subject_status(execution.subject_id) do
        claim_execution(execution, subject_status, job_id, attempt)
      else
        {:error, error} -> Repo.rollback(error)
      end
    end)
    |> case do
      {:ok, result} -> {:ok, result}
      {:error, error} -> {:error, error}
    end
  end

  defp claim_execution(
         %ExecutionRecord{dispatch_state: state},
         _subject_status,
         _job_id,
         _attempt
       )
       when state in [
              :awaiting_receipt,
              :running,
              :completed,
              :failed,
              :rejected,
              :stalled,
              :cancelled
            ],
       do: :terminal

  defp claim_execution(%ExecutionRecord{} = execution, "cancelled", _job_id, _attempt) do
    maybe_mark_operator_cancelled(execution)
  end

  defp claim_execution(%ExecutionRecord{} = _execution, "paused", _job_id, _attempt), do: :paused

  defp claim_execution(
         %ExecutionRecord{dispatch_state: :dispatching} = execution,
         _subject_status,
         _job_id,
         _attempt
       ),
       do: execution

  defp claim_execution(%ExecutionRecord{} = execution, _subject_status, job_id, attempt) do
    case ExecutionRecord.mark_dispatching(execution, %{
           trace_id: execution.trace_id,
           causation_id: causation_id(job_id, attempt)
         }) do
      {:ok, marked_execution} -> marked_execution
      {:error, error} -> Repo.rollback(error)
    end
  end

  defp dispatch_or_resume(%ExecutionRecord{} = execution, %Oban.Job{} = job) do
    case LowerGatewayCircuit.permit(execution.tenant_id, execution.installation_id) do
      :allow ->
        do_dispatch_or_resume(execution, job)

      {:snooze, delay_ms} ->
        JobOutbox.snooze_response(delay_ms)
    end
  end

  defp do_dispatch_or_resume(%ExecutionRecord{} = execution, %Oban.Job{} = job) do
    lookup_started_at = System.monotonic_time()

    case LowerGateway.lookup_submission(execution.submission_dedupe_key, execution.tenant_id) do
      {:accepted, payload} ->
        lookup_latency_ms = Telemetry.monotonic_duration_ms(lookup_started_at)

        with {:ok, _circuit} <-
               LowerGatewayCircuit.record_success(execution.tenant_id, execution.installation_id) do
          record_accepted(execution, payload, job,
            dispatch_source: :lookup,
            latency_ms: lookup_latency_ms
          )
        end

      {:rejected, payload} ->
        Telemetry.emit(
          [:dispatch, :lookup, :rejected],
          %{count: 1, latency_ms: Telemetry.monotonic_duration_ms(lookup_started_at)},
          dispatch_metadata(execution, %{dispatch_source: :lookup})
        )

        with {:ok, _circuit} <-
               LowerGatewayCircuit.record_success(execution.tenant_id, execution.installation_id) do
          payload
          |> normalize_lookup_rejection()
          |> persist_dispatch_result(execution, job)
        end

      :never_seen ->
        Telemetry.emit(
          [:dispatch, :lookup, :miss],
          %{count: 1, latency_ms: Telemetry.monotonic_duration_ms(lookup_started_at)},
          dispatch_metadata(execution, %{dispatch_source: :lookup})
        )

        dispatch_started_at = System.monotonic_time()

        execution
        |> build_dispatch_claim()
        |> LowerGateway.dispatch()
        |> normalize_dispatch_result()
        |> record_circuit_result(execution)
        |> persist_dispatch_result(
          execution,
          job,
          dispatch_source: :dispatch,
          latency_ms: Telemetry.monotonic_duration_ms(dispatch_started_at)
        )

      {:expired, %DateTime{} = last_seen_at} ->
        Telemetry.emit(
          [:dispatch, :lookup, :expired],
          %{count: 1, latency_ms: Telemetry.monotonic_duration_ms(lookup_started_at)},
          dispatch_metadata(execution, %{
            dispatch_source: :lookup,
            last_seen_at: last_seen_at
          })
        )

        with {:ok, _circuit} <-
               LowerGatewayCircuit.record_success(execution.tenant_id, execution.installation_id) do
          record_lookup_expired(execution, last_seen_at, job)
        end

      {:error, error} ->
        Telemetry.emit(
          [:dispatch, :lookup, :error],
          %{count: 1, latency_ms: Telemetry.monotonic_duration_ms(lookup_started_at)},
          dispatch_metadata(execution, %{
            dispatch_source: :lookup,
            error: normalize_value(error)
          })
        )

        with {:ok, _circuit} <-
               LowerGatewayCircuit.record_failure(execution.tenant_id, execution.installation_id) do
          retry_dispatch(execution, "lookup_failed", %{"error" => normalize_value(error)}, job)
        end
    end
  end

  defp record_circuit_result({:accepted, _, _} = result, execution) do
    record_circuit_success(result, execution)
  end

  defp record_circuit_result({:terminal, _, _} = result, execution) do
    record_circuit_success(result, execution)
  end

  defp record_circuit_result({:semantic_failure, _} = result, execution) do
    record_circuit_success(result, execution)
  end

  defp record_circuit_result({:retryable, _, _} = result, execution) do
    record_circuit_failure(result, execution)
  end

  defp record_circuit_success(result, execution) do
    case LowerGatewayCircuit.record_success(execution.tenant_id, execution.installation_id) do
      {:ok, _circuit} -> result
      {:error, error} -> {:circuit_error, error}
    end
  end

  defp record_circuit_failure(result, execution) do
    case LowerGatewayCircuit.record_failure(execution.tenant_id, execution.installation_id) do
      {:ok, _circuit} -> result
      {:error, error} -> {:circuit_error, error}
    end
  end

  defp persist_dispatch_result({:circuit_error, error}, _execution, _job, _opts),
    do: {:error, error}

  defp persist_dispatch_result({:accepted, submission_ref, lower_receipt}, execution, job, opts) do
    record_accepted(
      execution,
      %{submission_ref: submission_ref, lower_receipt: lower_receipt},
      job,
      opts
    )
  end

  defp persist_dispatch_result({:retryable, error_kind, error_payload}, execution, job, _opts) do
    retry_dispatch(execution, error_kind, error_payload, job)
  end

  defp persist_dispatch_result({:terminal, terminal_reason, error_payload}, execution, job, _opts) do
    Repo.transaction(fn ->
      with {:ok, _execution} <-
             ExecutionRecord.record_terminal_rejection(execution, %{
               terminal_rejection_reason: terminal_reason,
               last_dispatch_error_payload: error_payload,
               trace_id: execution.trace_id,
               causation_id: causation_id(job),
               actor_ref: actor_ref(job)
             }),
           {:ok, _invalidations} <-
             Leasing.invalidate_execution_leases(
               execution.id,
               "execution_rejected",
               now: DateTime.utc_now() |> DateTime.truncate(:microsecond),
               repo: Repo,
               trace_id: execution.trace_id
             ) do
        :discard
      else
        {:error, error} -> Repo.rollback(error)
      end
    end)
    |> case do
      {:ok, result} -> result
      {:error, error} -> {:error, error}
    end
  end

  defp persist_dispatch_result({:semantic_failure, payload}, execution, job, _opts) do
    ExecutionRecord.record_semantic_failure(execution, %{
      lower_receipt: Map.get(payload, "lower_receipt", execution.lower_receipt),
      last_dispatch_error_payload: Map.drop(payload, ["lower_receipt"]),
      trace_id: execution.trace_id,
      causation_id: causation_id(job),
      actor_ref: actor_ref(job)
    })
    |> case do
      {:ok, _execution} -> :discard
      {:error, error} -> {:error, error}
    end
  end

  defp persist_dispatch_result(result, execution, job) do
    persist_dispatch_result(result, execution, job, [])
  end

  defp record_accepted(%ExecutionRecord{} = execution, payload, job, opts) do
    ExecutionRecord.record_accepted(execution, %{
      submission_ref: Map.fetch!(payload, :submission_ref),
      lower_receipt: Map.fetch!(payload, :lower_receipt),
      trace_id: execution.trace_id,
      causation_id: causation_id(job),
      actor_ref: actor_ref(job)
    })
    |> case do
      {:ok, _execution} ->
        emit_dispatch_accepted(execution, payload, opts)
        :ok

      {:error, error} ->
        {:error, error}
    end
  end

  defp retry_dispatch(%ExecutionRecord{} = execution, error_kind, error_payload, job) do
    now = DateTime.utc_now()
    next_dispatch_at = DateTime.add(now, @default_retry_delay_ms, :millisecond)

    ExecutionRecord.record_retryable_failure(execution, %{
      last_dispatch_error_kind: error_kind,
      last_dispatch_error_payload: error_payload,
      next_dispatch_at: next_dispatch_at,
      trace_id: execution.trace_id,
      causation_id: causation_id(job),
      actor_ref: actor_ref(job)
    })
    |> case do
      {:ok, _execution} -> JobOutbox.snooze_response(@default_retry_delay_ms)
      {:error, error} -> {:error, error}
    end
  end

  defp record_lookup_expired(%ExecutionRecord{} = execution, last_seen_at, job) do
    ExecutionRecord.record_lookup_expired(execution, %{
      last_dispatch_error_payload: %{
        "reason" => "submission_lookup_expired",
        "last_seen_at" => DateTime.to_iso8601(last_seen_at)
      },
      trace_id: execution.trace_id,
      causation_id: causation_id(job),
      actor_ref: actor_ref(job)
    })
    |> case do
      {:ok, _execution} -> :discard
      {:error, error} -> {:error, error}
    end
  end

  defp emit_dispatch_accepted(%ExecutionRecord{} = execution, payload, opts) do
    submission_ref = Map.fetch!(payload, :submission_ref)
    lower_receipt = Map.fetch!(payload, :lower_receipt)

    metadata =
      dispatch_metadata(execution, %{
        dispatch_source: Keyword.get(opts, :dispatch_source, :dispatch),
        lower_run_id: lower_receipt_run_id(lower_receipt),
        submission_ref: submission_ref
      })

    accepted_measurements =
      %{count: 1}
      |> maybe_put_measurement(:latency_ms, Keyword.get(opts, :latency_ms))

    Telemetry.emit([:dispatch, :accepted], accepted_measurements, metadata)
    Telemetry.emit([:dispatch, :awaiting_receipt], %{count: 1}, metadata)
  end

  defp build_dispatch_claim(execution) do
    %{
      execution_id: execution.id,
      tenant_id: execution.tenant_id,
      installation_id: execution.installation_id,
      subject_id: execution.subject_id,
      trace_id: execution.trace_id,
      causation_id: execution.causation_id,
      submission_dedupe_key: execution.submission_dedupe_key,
      compiled_pack_revision: execution.compiled_pack_revision,
      binding_snapshot: execution.binding_snapshot,
      dispatch_envelope: execution.dispatch_envelope
    }
  end

  defp normalize_dispatch_result({:accepted, payload}) do
    payload = normalize_map(payload)

    {:accepted, Map.get(payload, "submission_ref", %{}), Map.get(payload, "lower_receipt", %{})}
  end

  defp normalize_dispatch_result({:rejected, payload}) do
    payload = normalize_map(payload)
    {:terminal, Map.get(payload, "reason", "terminal_rejection"), Map.delete(payload, "reason")}
  end

  defp normalize_dispatch_result({:semantic_failure, payload}) do
    {:semantic_failure, normalize_map(payload)}
  end

  defp normalize_dispatch_result({:error, {:retryable, error_kind, payload}}) do
    {:retryable, normalize_reason(error_kind), normalize_map(payload)}
  end

  defp normalize_dispatch_result({:error, {:terminal, terminal_reason, payload}}) do
    {:terminal, normalize_reason(terminal_reason), normalize_map(payload)}
  end

  defp normalize_dispatch_result({:error, {:semantic_failure, payload}}) do
    {:semantic_failure, normalize_map(payload)}
  end

  defp normalize_dispatch_result({:error, error}) do
    {:retryable, normalize_reason(error), %{"error" => normalize_value(error)}}
  end

  defp normalize_lookup_rejection(payload) do
    payload = normalize_map(payload)
    {:terminal, Map.get(payload, "reason", "terminal_rejection"), Map.delete(payload, "reason")}
  end

  defp actor_ref(%Oban.Job{} = job) do
    %{kind: :execution_dispatch_worker, job_id: job.id, queue: job.queue}
  end

  defp causation_id(%Oban.Job{} = job), do: causation_id(job.id, job.attempt)

  defp causation_id(job_id, attempt),
    do: "execution-dispatch-worker:job:#{job_id}:attempt:#{attempt}"

  defp normalize_reason(reason) when is_atom(reason), do: Atom.to_string(reason)
  defp normalize_reason(reason) when is_binary(reason), do: reason
  defp normalize_reason(reason), do: inspect(reason)

  defp normalize_map(value) when is_map(value) do
    value
    |> Enum.map(fn {key, nested} -> {to_string(key), normalize_value(nested)} end)
    |> Map.new()
  end

  defp normalize_map(value), do: %{"value" => normalize_value(value)}

  defp normalize_value(%DateTime{} = datetime), do: DateTime.to_iso8601(datetime)

  defp normalize_value(value) when is_map(value) do
    value
    |> Enum.map(fn {key, nested} -> {to_string(key), normalize_value(nested)} end)
    |> Map.new()
  end

  defp normalize_value(value) when is_list(value), do: Enum.map(value, &normalize_value/1)

  defp normalize_value(value) when is_tuple(value),
    do: value |> Tuple.to_list() |> Enum.map(&normalize_value/1)

  defp normalize_value(value) when is_atom(value), do: Atom.to_string(value)
  defp normalize_value(value), do: value

  defp dispatch_metadata(%ExecutionRecord{} = execution, extra) do
    Map.merge(
      %{
        trace_id: execution.trace_id,
        subject_id: execution.subject_id,
        execution_id: execution.id,
        submission_dedupe_key: execution.submission_dedupe_key,
        tenant_id: execution.tenant_id,
        installation_id: execution.installation_id
      },
      extra
    )
  end

  defp maybe_put_measurement(measurements, _key, nil), do: measurements
  defp maybe_put_measurement(measurements, key, value), do: Map.put(measurements, key, value)

  defp lower_receipt_run_id(lower_receipt) when is_map(lower_receipt) do
    Map.get(lower_receipt, :run_id) || Map.get(lower_receipt, "run_id")
  end

  defp lower_receipt_run_id(_other), do: nil

  defp maybe_mark_operator_cancelled(%ExecutionRecord{dispatch_state: :cancelled}), do: :terminal

  defp maybe_mark_operator_cancelled(%ExecutionRecord{} = execution) do
    case ExecutionRecord.record_operator_cancelled(execution, %{
           reason: "subject_cancelled_before_dispatch",
           trace_id: execution.trace_id,
           causation_id: "execution-dispatch-worker:operator-cancelled:#{execution.id}",
           actor_ref: %{kind: :execution_dispatch_worker}
         }) do
      {:ok, _cancelled_execution} -> :terminal
      {:error, error} -> Repo.rollback(error)
    end
  end

  defp lock_subject(subject_id) do
    case SQL.query(Repo, @subject_lock_sql, [subject_id]) do
      {:ok, _result} -> :ok
      {:error, error} -> {:error, error}
    end
  end

  defp load_subject_status(subject_id) do
    case SQL.query(Repo, @subject_status_sql, [dump_uuid!(subject_id)]) do
      {:ok, %{rows: [[status]]}} when is_binary(status) -> {:ok, status}
      {:ok, %{rows: []}} -> {:error, {:subject_not_found, subject_id}}
      {:error, error} -> {:error, error}
    end
  end

  defp dump_uuid!(uuid), do: UUID.dump!(uuid)
end
