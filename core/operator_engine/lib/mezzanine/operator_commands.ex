defmodule Mezzanine.OperatorCommands do
  @moduledoc """
  Durable operator pause, resume, and cancel commands on the substrate path.
  """

  alias Ecto.Adapters.SQL
  alias Ecto.UUID
  alias Mezzanine.Execution.Repo
  alias Mezzanine.JobOutbox
  alias Mezzanine.Leasing

  @dispatch_worker Oban.Worker.to_string(Mezzanine.ExecutionDispatchWorker)
  @cancel_queue :cancel
  @pause_sentinel ~U[9999-12-31 00:00:00.000000Z]
  @active_dispatch_states [
    "pending_dispatch",
    "dispatching",
    "dispatching_retry",
    "awaiting_receipt",
    "running"
  ]
  @accepted_dispatch_states ["awaiting_receipt", "running"]

  @subject_lock_sql """
  SELECT pg_advisory_xact_lock(hashtext('mezzanine.subject:' || $1))
  """

  @load_subject_sql """
  SELECT id, installation_id, lifecycle_state, status, status_reason, status_updated_at, terminal_at
  FROM subject_records
  WHERE id = $1::uuid
  FOR UPDATE
  """

  @update_subject_status_sql """
  UPDATE subject_records
  SET status = $2,
      status_reason = $3,
      status_updated_at = $4,
      terminal_at = $5,
      row_version = row_version + 1,
      updated_at = $4
  WHERE id = $1::uuid
  RETURNING id, installation_id, lifecycle_state, status, status_reason, status_updated_at, terminal_at
  """

  @active_executions_sql """
  SELECT id, tenant_id, dispatch_state, submission_ref
  FROM execution_records
  WHERE subject_id = $1::uuid
    AND dispatch_state = ANY($2)
  ORDER BY inserted_at ASC
  """

  @pause_dispatch_jobs_sql """
  WITH target_execution_ids AS (
    SELECT unnest($2::text[]) AS execution_id
  )
  UPDATE oban_jobs AS job
  SET state = 'scheduled',
      scheduled_at = $1,
      meta = jsonb_set(
        COALESCE(job.meta, '{}'::jsonb),
        '{pause_scheduled_at}',
        to_jsonb(
          to_char(
            job.scheduled_at,
            'YYYY-MM-DD"T"HH24:MI:SS.US"Z"'
          )
        ),
        true
      )
  FROM target_execution_ids AS target
  WHERE target.execution_id = job.args->>'execution_id'
    AND job.queue = 'dispatch'
    AND job.worker = $3
    AND job.state IN ('available', 'scheduled', 'retryable')
    AND NOT (COALESCE(job.meta, '{}'::jsonb) ? 'pause_scheduled_at')
  RETURNING job.id
  """

  @resume_dispatch_jobs_sql """
  WITH target_execution_ids AS (
    SELECT unnest($1::text[]) AS execution_id
  )
  UPDATE oban_jobs AS job
  SET state = 'scheduled',
      scheduled_at = (job.meta->>'pause_scheduled_at')::timestamp,
      meta = COALESCE(job.meta, '{}'::jsonb) - 'pause_scheduled_at'
  FROM target_execution_ids AS target
  WHERE target.execution_id = job.args->>'execution_id'
    AND job.queue = 'dispatch'
    AND job.worker = $2
    AND job.state IN ('available', 'scheduled', 'retryable')
    AND COALESCE(job.meta, '{}'::jsonb) ? 'pause_scheduled_at'
  RETURNING job.id
  """

  @cancel_dispatch_jobs_sql """
  WITH target_execution_ids AS (
    SELECT unnest($2::text[]) AS execution_id
  )
  UPDATE oban_jobs AS job
  SET state = 'cancelled',
      cancelled_at = $1
  FROM target_execution_ids AS target
  WHERE target.execution_id = job.args->>'execution_id'
    AND job.queue = 'dispatch'
    AND job.worker = $3
    AND job.state IN ('available', 'scheduled', 'retryable')
  RETURNING job.id
  """

  @cancel_execution_rows_sql """
  UPDATE execution_records
  SET dispatch_state = 'cancelled',
      next_dispatch_at = NULL,
      last_dispatch_error_kind = 'operator_cancelled',
      last_dispatch_error_payload = $2,
      failure_kind = NULL,
      terminal_rejection_reason = NULL,
      updated_at = $3,
      trace_id = $4,
      causation_id = $5
  WHERE subject_id = $1::uuid
    AND dispatch_state = ANY($6)
  RETURNING id
  """

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
    $2::uuid,
    $3::uuid,
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

  @type result :: {:ok, map()} | {:error, term()}

  @spec pause(Ecto.UUID.t(), keyword()) :: result()
  def pause(subject_id, opts \\ []) when is_binary(subject_id) and is_list(opts) do
    now = now(opts)

    Repo.transaction(fn ->
      with :ok <- lock_subject(subject_id),
           {:ok, subject} <- fetch_subject_for_update(subject_id) do
        pause_locked_subject(subject_id, subject, opts, now)
      else
        {:error, error} -> Repo.rollback(error)
      end
    end)
    |> normalize_transaction()
  end

  @spec resume(Ecto.UUID.t(), keyword()) :: result()
  def resume(subject_id, opts \\ []) when is_binary(subject_id) and is_list(opts) do
    now = now(opts)

    Repo.transaction(fn ->
      with :ok <- lock_subject(subject_id),
           {:ok, subject} <- fetch_subject_for_update(subject_id) do
        resume_locked_subject(subject_id, subject, opts, now)
      else
        {:error, error} -> Repo.rollback(error)
      end
    end)
    |> normalize_transaction()
  end

  @spec cancel(Ecto.UUID.t(), keyword()) :: result()
  def cancel(subject_id, opts \\ []) when is_binary(subject_id) and is_list(opts) do
    now = now(opts)
    cancel_reason = keyword_reason(opts)
    trace_id = trace_id(opts, subject_id, "cancel")
    causation_id = causation_id(opts, "cancel", subject_id)
    actor_ref = actor_ref(opts)

    Repo.transaction(fn ->
      with :ok <- lock_subject(subject_id),
           {:ok, subject} <- fetch_subject_for_update(subject_id) do
        cancel_locked_subject(
          subject_id,
          subject,
          cancel_reason,
          trace_id,
          causation_id,
          actor_ref,
          now
        )
      else
        {:error, error} -> Repo.rollback(error)
      end
    end)
    |> normalize_transaction()
  end

  defp lock_subject(subject_id) do
    case SQL.query(Repo, @subject_lock_sql, [subject_id]) do
      {:ok, _result} -> :ok
      {:error, error} -> {:error, error}
    end
  end

  defp pause_locked_subject(subject_id, subject, opts, now) do
    case subject.status do
      "paused" ->
        build_result(:pause, subject, paused_job_ids: [], noop?: true)

      "cancelled" ->
        Repo.rollback({:subject_terminal, subject_id, "cancelled"})

      _active ->
        pause_active_subject(subject_id, opts, now)
    end
  end

  defp pause_active_subject(subject_id, opts, now) do
    active_executions = fetch_active_executions(subject_id)
    paused_job_ids = pause_dispatch_jobs(active_executions, now)

    {:ok, updated_subject} =
      update_subject_status(subject_id, "paused", keyword_reason(opts), now, nil)

    invalidations =
      invalidate_subject_leases!(
        subject_id,
        "subject_paused",
        trace_id(opts, updated_subject.id, "pause"),
        now
      )

    :ok = record_subject_paused(updated_subject, paused_job_ids, opts, now)

    build_result(:pause, updated_subject,
      paused_job_ids: paused_job_ids,
      invalidated_lease_ids: Enum.map(invalidations, & &1.lease_id),
      noop?: false
    )
  end

  defp resume_locked_subject(subject_id, subject, opts, now) do
    case subject.status do
      "active" ->
        build_result(:resume, subject, resumed_job_ids: [], noop?: true)

      "cancelled" ->
        Repo.rollback({:subject_terminal, subject_id, "cancelled"})

      _paused ->
        resume_paused_subject(subject_id, subject, opts, now)
    end
  end

  defp resume_paused_subject(subject_id, subject, opts, now) do
    active_executions = fetch_active_executions(subject_id)
    resumed_job_ids = resume_dispatch_jobs(active_executions)

    {:ok, updated_subject} =
      update_subject_status(subject_id, "active", nil, now, subject.terminal_at)

    :ok = record_subject_resumed(updated_subject, resumed_job_ids, opts, now)
    build_result(:resume, updated_subject, resumed_job_ids: resumed_job_ids, noop?: false)
  end

  defp cancel_locked_subject(
         subject_id,
         subject,
         cancel_reason,
         trace_id,
         causation_id,
         actor_ref,
         now
       ) do
    case subject.status do
      "cancelled" ->
        build_result(:cancel, subject,
          cancelled_execution_ids: [],
          cancel_job_refs: [],
          noop?: true
        )

      _active ->
        cancel_active_subject(
          subject_id,
          subject,
          cancel_reason,
          trace_id,
          causation_id,
          actor_ref,
          now
        )
    end
  end

  defp cancel_active_subject(
         subject_id,
         subject,
         cancel_reason,
         trace_id,
         causation_id,
         actor_ref,
         now
       ) do
    active_executions = fetch_active_executions(subject_id)
    cancelled_dispatch_job_ids = cancel_dispatch_jobs(active_executions, now)

    cancelled_execution_ids =
      cancel_execution_rows(subject_id, cancel_reason, now, trace_id, causation_id)

    cancel_job_refs = enqueue_cancel_jobs(active_executions, cancel_reason, now)

    invalidations =
      invalidate_subject_leases!(
        subject_id,
        "subject_cancelled",
        trace_id,
        now
      )

    :ok =
      record_execution_cancellations(
        subject,
        cancelled_execution_ids,
        cancel_reason,
        trace_id,
        causation_id,
        actor_ref,
        now
      )

    {:ok, updated_subject} =
      update_subject_status(subject_id, "cancelled", cancel_reason, now, now)

    :ok =
      record_subject_cancelled(
        updated_subject,
        cancelled_dispatch_job_ids,
        cancelled_execution_ids,
        cancel_job_refs,
        Enum.map(invalidations, & &1.lease_id),
        trace_id,
        causation_id,
        actor_ref,
        now
      )

    build_result(
      :cancel,
      updated_subject,
      cancelled_dispatch_job_ids: cancelled_dispatch_job_ids,
      cancelled_execution_ids: cancelled_execution_ids,
      cancel_job_refs: cancel_job_refs,
      invalidated_lease_ids: Enum.map(invalidations, & &1.lease_id),
      noop?: false
    )
  end

  defp invalidate_subject_leases!(subject_id, reason, trace_id, now) do
    case Leasing.invalidate_subject_leases(
           subject_id,
           reason,
           now: now,
           repo: Repo,
           trace_id: trace_id
         ) do
      {:ok, invalidations} -> invalidations
      {:error, error} -> Repo.rollback(error)
    end
  end

  defp record_subject_paused(updated_subject, paused_job_ids, opts, now) do
    insert_audit_fact(%{
      installation_id: updated_subject.installation_id,
      subject_id: updated_subject.id,
      execution_id: nil,
      trace_id: trace_id(opts, updated_subject.id, "pause"),
      causation_id: causation_id(opts, "pause", updated_subject.id),
      fact_kind: "subject_paused",
      actor_ref: actor_ref(opts),
      payload: %{
        "lifecycle_state" => updated_subject.lifecycle_state,
        "status" => updated_subject.status,
        "status_reason" => updated_subject.status_reason,
        "paused_dispatch_job_ids" => paused_job_ids
      },
      occurred_at: now
    })
  end

  defp record_subject_resumed(updated_subject, resumed_job_ids, opts, now) do
    insert_audit_fact(%{
      installation_id: updated_subject.installation_id,
      subject_id: updated_subject.id,
      execution_id: nil,
      trace_id: trace_id(opts, updated_subject.id, "resume"),
      causation_id: causation_id(opts, "resume", updated_subject.id),
      fact_kind: "subject_resumed",
      actor_ref: actor_ref(opts),
      payload: %{
        "lifecycle_state" => updated_subject.lifecycle_state,
        "status" => updated_subject.status,
        "resumed_dispatch_job_ids" => resumed_job_ids
      },
      occurred_at: now
    })
  end

  defp record_execution_cancellations(
         subject,
         cancelled_execution_ids,
         cancel_reason,
         trace_id,
         causation_id,
         actor_ref,
         now
       ) do
    Enum.each(cancelled_execution_ids, fn execution_id ->
      :ok =
        insert_audit_fact(%{
          installation_id: subject.installation_id,
          subject_id: subject.id,
          execution_id: execution_id,
          trace_id: trace_id,
          causation_id: causation_id,
          fact_kind: "execution_cancelled",
          actor_ref: actor_ref,
          payload: %{
            "classification" => "operator_cancelled",
            "reason" => cancel_reason
          },
          occurred_at: now
        })
    end)

    :ok
  end

  defp record_subject_cancelled(
         updated_subject,
         cancelled_dispatch_job_ids,
         cancelled_execution_ids,
         cancel_job_refs,
         invalidated_lease_ids,
         trace_id,
         causation_id,
         actor_ref,
         now
       ) do
    insert_audit_fact(%{
      installation_id: updated_subject.installation_id,
      subject_id: updated_subject.id,
      execution_id: nil,
      trace_id: trace_id,
      causation_id: causation_id,
      fact_kind: "subject_cancelled",
      actor_ref: actor_ref,
      payload: %{
        "lifecycle_state" => updated_subject.lifecycle_state,
        "status" => updated_subject.status,
        "status_reason" => updated_subject.status_reason,
        "cancelled_dispatch_job_ids" => cancelled_dispatch_job_ids,
        "cancelled_execution_ids" => cancelled_execution_ids,
        "cancel_job_ids" => Enum.map(cancel_job_refs, & &1.job_id),
        "invalidated_lease_ids" => invalidated_lease_ids
      },
      occurred_at: now
    })
  end

  defp fetch_subject_for_update(subject_id) do
    case SQL.query(Repo, @load_subject_sql, [dump_uuid!(subject_id)]) do
      {:ok,
       %{
         rows: [
           [
             id,
             installation_id,
             lifecycle_state,
             status,
             status_reason,
             status_updated_at,
             terminal_at
           ]
         ]
       }} ->
        {:ok,
         %{
           id: normalize_uuid(id),
           installation_id: installation_id,
           lifecycle_state: lifecycle_state,
           status: status,
           status_reason: status_reason,
           status_updated_at: status_updated_at,
           terminal_at: terminal_at
         }}

      {:ok, %{rows: []}} ->
        {:error, {:subject_not_found, subject_id}}

      {:error, error} ->
        {:error, error}
    end
  end

  defp update_subject_status(subject_id, status, status_reason, now, terminal_at) do
    case SQL.query(Repo, @update_subject_status_sql, [
           dump_uuid!(subject_id),
           status,
           status_reason,
           now,
           terminal_at
         ]) do
      {:ok,
       %{
         rows: [
           [
             id,
             installation_id,
             lifecycle_state,
             loaded_status,
             loaded_reason,
             status_updated_at,
             loaded_terminal_at
           ]
         ]
       }} ->
        {:ok,
         %{
           id: normalize_uuid(id),
           installation_id: installation_id,
           lifecycle_state: lifecycle_state,
           status: loaded_status,
           status_reason: loaded_reason,
           status_updated_at: status_updated_at,
           terminal_at: loaded_terminal_at
         }}

      {:error, error} ->
        {:error, error}
    end
  end

  defp fetch_active_executions(subject_id) do
    SQL.query!(Repo, @active_executions_sql, [dump_uuid!(subject_id), @active_dispatch_states]).rows
    |> Enum.map(fn [id, tenant_id, dispatch_state, submission_ref] ->
      %{
        id: normalize_uuid(id),
        tenant_id: tenant_id,
        dispatch_state: dispatch_state,
        submission_ref: normalize_map(submission_ref)
      }
    end)
  end

  defp pause_dispatch_jobs(executions, _now) do
    execution_ids = Enum.map(executions, & &1.id)

    if execution_ids == [] do
      []
    else
      SQL.query!(Repo, @pause_dispatch_jobs_sql, [
        @pause_sentinel,
        execution_ids,
        @dispatch_worker
      ]).rows
      |> Enum.map(fn [job_id] -> job_id end)
    end
  end

  defp resume_dispatch_jobs(executions) do
    execution_ids = Enum.map(executions, & &1.id)

    if execution_ids == [] do
      []
    else
      SQL.query!(Repo, @resume_dispatch_jobs_sql, [execution_ids, @dispatch_worker]).rows
      |> Enum.map(fn [job_id] -> job_id end)
    end
  end

  defp cancel_dispatch_jobs(executions, now) do
    execution_ids = Enum.map(executions, & &1.id)

    if execution_ids == [] do
      []
    else
      SQL.query!(Repo, @cancel_dispatch_jobs_sql, [now, execution_ids, @dispatch_worker]).rows
      |> Enum.map(fn [job_id] -> job_id end)
    end
  end

  defp cancel_execution_rows(subject_id, reason, now, trace_id, causation_id) do
    SQL.query!(Repo, @cancel_execution_rows_sql, [
      dump_uuid!(subject_id),
      %{"reason" => reason},
      now,
      trace_id,
      causation_id,
      @active_dispatch_states
    ]).rows
    |> Enum.map(fn [execution_id] -> normalize_uuid(execution_id) end)
  end

  defp enqueue_cancel_jobs(executions, reason, now) do
    executions
    |> Enum.filter(fn execution ->
      execution.dispatch_state in @accepted_dispatch_states and execution.submission_ref != %{}
    end)
    |> Enum.map(fn execution ->
      case JobOutbox.enqueue(
             @cancel_queue,
             Mezzanine.ExecutionCancelWorker,
             %{
               execution_id: execution.id,
               reason: reason
             },
             scheduled_at: now
           ) do
        {:ok, job_ref} ->
          job_ref

        {:error, error} ->
          Repo.rollback({:cancel_job_enqueue_failed, execution.id, error})
      end
    end)
  end

  defp insert_audit_fact(attrs) do
    case SQL.query(Repo, @insert_audit_fact_sql, [
           attrs.installation_id,
           dump_uuid!(attrs.subject_id),
           maybe_dump_uuid(attrs.execution_id),
           attrs.trace_id,
           attrs.causation_id,
           attrs.fact_kind,
           attrs.actor_ref,
           normalize_map(attrs.payload),
           attrs.occurred_at
         ]) do
      {:ok, _result} -> :ok
      {:error, error} -> Repo.rollback(error)
    end
  end

  defp build_result(action, subject, details) do
    {:ok,
     %{
       action: action,
       subject_id: subject.id,
       installation_id: subject.installation_id,
       lifecycle_state: subject.lifecycle_state,
       status: subject.status,
       status_reason: subject.status_reason,
       status_updated_at: subject.status_updated_at,
       terminal_at: subject.terminal_at,
       details: Map.new(details)
     }}
  end

  defp normalize_transaction({:ok, {:ok, result}}), do: {:ok, result}
  defp normalize_transaction({:ok, result}), do: {:ok, result}
  defp normalize_transaction({:error, reason}), do: {:error, reason}

  defp now(opts) do
    opts
    |> Keyword.get(:now, DateTime.utc_now())
    |> DateTime.truncate(:microsecond)
  end

  defp trace_id(opts, subject_id, action) do
    Keyword.get(opts, :trace_id, "operator-commands:#{action}:#{subject_id}")
  end

  defp causation_id(opts, action, subject_id) do
    Keyword.get(opts, :causation_id, "operator-commands:#{action}:#{subject_id}")
  end

  defp actor_ref(opts) do
    opts
    |> Keyword.get(:actor_ref, %{kind: :operator})
    |> normalize_map()
  end

  defp keyword_reason(opts), do: Keyword.get(opts, :reason)

  defp normalize_map(value) when is_map(value) do
    value
    |> Enum.map(fn {key, nested} -> {to_string(key), normalize_value(nested)} end)
    |> Map.new()
  end

  defp normalize_map(_other), do: %{}

  defp normalize_value(%DateTime{} = datetime), do: DateTime.to_iso8601(datetime)
  defp normalize_value(value) when is_binary(value), do: normalize_uuid(value)
  defp normalize_value(value) when is_map(value), do: normalize_map(value)
  defp normalize_value(value) when is_list(value), do: Enum.map(value, &normalize_value/1)
  defp normalize_value(value) when is_atom(value), do: Atom.to_string(value)
  defp normalize_value(value), do: value

  defp normalize_uuid(value) when is_binary(value) do
    case UUID.cast(value) do
      {:ok, uuid} -> uuid
      :error -> value
    end
  end

  defp normalize_uuid(value), do: value

  defp dump_uuid!(uuid), do: UUID.dump!(uuid)
  defp maybe_dump_uuid(nil), do: nil
  defp maybe_dump_uuid(uuid) when is_binary(uuid) and byte_size(uuid) == 16, do: uuid
  defp maybe_dump_uuid(uuid), do: dump_uuid!(uuid)
end
