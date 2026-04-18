defmodule Mezzanine.ExecutionReconcileWorker do
  @moduledoc """
  Reconcile accepted lower executions through the lower-gateway read seam and
  enqueue idempotent receipt work when terminal lower truth becomes visible.
  """

  use Oban.Worker,
    queue: :reconcile,
    max_attempts: 20,
    unique: [
      period: :infinity,
      states: [:available, :scheduled, :executing, :retryable],
      keys: [:execution_id]
    ]

  alias Mezzanine.Execution.ExecutionRecord
  alias Mezzanine.ExecutionReceiptWorker
  alias Mezzanine.JobOutbox
  alias Mezzanine.LowerGateway
  alias Mezzanine.LowerGatewayCircuit
  alias Mezzanine.Telemetry

  @default_retry_delay_ms 30_000

  @spec unique_declaration() :: keyword()
  def unique_declaration do
    [
      period: :infinity,
      states: [:available, :scheduled, :executing, :retryable],
      keys: [:execution_id]
    ]
  end

  @impl Oban.Worker
  def perform(%Oban.Job{args: %{"execution_id" => execution_id}}) do
    with {:ok, execution} <- Ash.get(ExecutionRecord, execution_id) do
      reconcile_execution(execution)
    end
  end

  defp reconcile_execution(%ExecutionRecord{dispatch_state: state})
       when state in [:completed, :failed, :rejected, :cancelled],
       do: :discard

  defp reconcile_execution(%ExecutionRecord{} = execution) do
    case LowerGatewayCircuit.permit(execution.tenant_id, execution.installation_id) do
      :allow ->
        lookup_started_at = System.monotonic_time()

        execution
        |> build_lookup()
        |> LowerGateway.fetch_execution_outcome(execution.tenant_id)
        |> handle_reconcile_outcome(
          execution,
          Telemetry.monotonic_duration_ms(lookup_started_at)
        )

      {:snooze, delay_ms} ->
        JobOutbox.snooze_response(delay_ms)
    end
  end

  defp handle_reconcile_outcome(:pending, %ExecutionRecord{} = execution, lookup_latency_ms) do
    emit_reconcile_lookup(execution, :pending, lookup_latency_ms)

    with {:ok, _circuit} <-
           LowerGatewayCircuit.record_success(execution.tenant_id, execution.installation_id) do
      JobOutbox.snooze_response(@default_retry_delay_ms)
    end
  end

  defp handle_reconcile_outcome({:ok, outcome}, %ExecutionRecord{} = execution, lookup_latency_ms) do
    emit_reconcile_lookup(
      execution,
      Map.get(outcome, :status) || Map.get(outcome, "status"),
      lookup_latency_ms
    )

    with {:ok, _circuit} <-
           LowerGatewayCircuit.record_success(execution.tenant_id, execution.installation_id) do
      enqueue_receipt(execution.id, outcome)
    end
  end

  defp handle_reconcile_outcome(
         {:error, error},
         %ExecutionRecord{} = execution,
         lookup_latency_ms
       ) do
    emit_reconcile_lookup(execution, :error, lookup_latency_ms, error: error)

    with {:ok, _circuit} <-
           LowerGatewayCircuit.record_failure(execution.tenant_id, execution.installation_id) do
      {:error, error}
    end
  end

  defp enqueue_receipt(execution_id, outcome) when is_map(outcome) do
    receipt_id = Map.get(outcome, :receipt_id) || Map.get(outcome, "receipt_id")

    case JobOutbox.enqueue(
           :receipt,
           ExecutionReceiptWorker,
           %{
             execution_id: execution_id,
             receipt_id: receipt_id,
             outcome: outcome
           },
           scheduled_at: DateTime.utc_now()
         ) do
      {:ok, _job_ref} -> :ok
      {:error, error} -> {:error, {:receipt_enqueue_failed, error}}
    end
  end

  defp build_lookup(%ExecutionRecord{} = execution) do
    %{}
    |> maybe_put("submission_ref", execution.submission_ref)
    |> maybe_put("submission_dedupe_key", execution.submission_dedupe_key)
    |> maybe_put("lower_receipt", execution.lower_receipt)
  end

  defp maybe_put(map, _key, value) when value in [%{}, nil], do: map
  defp maybe_put(map, key, value), do: Map.put(map, key, value)

  defp emit_reconcile_lookup(
         %ExecutionRecord{} = execution,
         outcome_status,
         lookup_latency_ms,
         extra \\ []
       ) do
    Telemetry.emit(
      [:dispatch, :reconcile, :lookup],
      %{count: 1, latency_ms: lookup_latency_ms},
      %{
        trace_id: execution.trace_id,
        subject_id: execution.subject_id,
        execution_id: execution.id,
        submission_dedupe_key: execution.submission_dedupe_key,
        tenant_id: execution.tenant_id,
        installation_id: execution.installation_id,
        outcome_status: normalize_outcome_status(outcome_status),
        error: normalize_optional_error(Keyword.get(extra, :error))
      }
    )
  end

  defp normalize_outcome_status(status) when is_atom(status), do: Atom.to_string(status)
  defp normalize_outcome_status(status) when is_binary(status), do: status
  defp normalize_outcome_status(_other), do: "unknown"

  defp normalize_optional_error(nil), do: nil
  defp normalize_optional_error(error), do: inspect(error)
end
