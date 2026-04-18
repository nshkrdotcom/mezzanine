defmodule Mezzanine.ExecutionCancelWorker do
  @moduledoc """
  Best-effort lower cancel propagation for already-accepted executions.
  """

  use Oban.Worker,
    queue: :cancel,
    max_attempts: 20,
    unique: [
      period: :infinity,
      states: [:available, :scheduled, :executing, :retryable],
      keys: [:execution_id]
    ]

  alias Ecto.Adapters.SQL
  alias Mezzanine.Execution.ExecutionRecord
  alias Mezzanine.Execution.Repo
  alias Mezzanine.LowerGateway
  alias Mezzanine.Telemetry

  @insert_audit_fact_sql """
  INSERT INTO audit_facts (
    id,
    installation_id,
    subject_id,
    execution_id,
    trace_id,
    causation_id,
    fact_kind,
    actor_ref,
    payload,
    occurred_at,
    inserted_at,
    updated_at
  )
  VALUES (
    gen_random_uuid(),
    $1,
    $2,
    $3,
    $4,
    $5,
    $6,
    $7,
    $8,
    $9,
    $9,
    $9
  )
  """

  @spec unique_declaration() :: keyword()
  def unique_declaration do
    [
      period: :infinity,
      states: [:available, :scheduled, :executing, :retryable],
      keys: [:execution_id]
    ]
  end

  @impl Oban.Worker
  def perform(%Oban.Job{args: %{"execution_id" => execution_id} = args} = job) do
    with {:ok, execution} <- Ash.get(ExecutionRecord, execution_id),
         submission_ref when submission_ref != %{} <- execution.submission_ref do
      metadata = emit_cancel_requested(execution, job, args)

      case LowerGateway.request_cancel(submission_ref, execution.tenant_id, %{
             "reason" => Map.get(args, "reason"),
             "execution_id" => execution.id
           }) do
        {:cancelled, %DateTime{} = effective_at} ->
          record_worker_audit(
            execution,
            :lower_cancelled,
            %{
              "effective_at" => DateTime.to_iso8601(effective_at)
            },
            job
          )

        {:too_late, terminal_outcome} ->
          emit_cancel_too_late(metadata, terminal_outcome)
          record_too_late_audits(execution, terminal_outcome, job)

        {:error, reason} ->
          record_worker_audit(
            execution,
            :lower_cancel_failed,
            %{
              "error" => normalize_value(reason)
            },
            job
          )
      end
    else
      {:error, error} -> {:error, error}
      %{} -> :discard
    end
  end

  defp record_too_late_audits(execution, terminal_outcome, job) do
    case record_worker_audit(
           execution,
           :post_cancel_receipt,
           %{
             "terminal_outcome" => normalize_value(terminal_outcome)
           },
           job
         ) do
      :ok ->
        record_worker_audit(
          execution,
          :reconciliation_warning,
          %{
            "reason" => "late_receipt_after_cancel",
            "terminal_outcome" => normalize_value(terminal_outcome)
          },
          job
        )

      {:error, error} ->
        {:error, error}
    end
  end

  defp emit_cancel_requested(execution, job, args) do
    metadata = telemetry_metadata(execution, job, args)
    Telemetry.emit([:cancel, :requested], %{count: 1}, metadata)
    metadata
  end

  defp emit_cancel_too_late(metadata, terminal_outcome) do
    Telemetry.emit(
      [:cancel, :too_late],
      %{count: 1},
      Map.merge(metadata, %{
        terminal_outcome_status: terminal_outcome_status(terminal_outcome),
        receipt_id: terminal_outcome_receipt_id(terminal_outcome)
      })
    )
  end

  defp record_worker_audit(execution, fact_kind, payload, job) do
    case SQL.query(Repo, @insert_audit_fact_sql, [
           execution.installation_id,
           execution.subject_id,
           execution.id,
           execution.trace_id,
           causation_id(job),
           to_string(fact_kind),
           actor_ref(job),
           payload,
           DateTime.utc_now() |> DateTime.truncate(:microsecond)
         ]) do
      {:ok, _result} -> :ok
      {:error, error} -> {:error, error}
    end
  end

  defp actor_ref(job) do
    %{
      "kind" => "execution_cancel_worker",
      "job_id" => job.id,
      "queue" => job.queue
    }
  end

  defp causation_id(job), do: "execution-cancel-worker:job:#{job.id}:attempt:#{job.attempt}"

  defp telemetry_metadata(execution, job, args) do
    %{
      trace_id: execution.trace_id,
      subject_id: execution.subject_id,
      execution_id: execution.id,
      submission_dedupe_key: Map.get(execution, :submission_dedupe_key),
      tenant_id: execution.tenant_id,
      installation_id: execution.installation_id,
      cancel_reason: Map.get(args, "reason"),
      lower_submission_ref: normalize_value(execution.submission_ref),
      job_id: job.id,
      job_attempt: job.attempt,
      queue: job.queue
    }
  end

  defp terminal_outcome_status(terminal_outcome) when is_map(terminal_outcome) do
    Map.get(terminal_outcome, "status") || Map.get(terminal_outcome, :status)
  end

  defp terminal_outcome_receipt_id(terminal_outcome) when is_map(terminal_outcome) do
    Map.get(terminal_outcome, "receipt_id") || Map.get(terminal_outcome, :receipt_id)
  end

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
end
