defmodule Mezzanine.DecisionCommands do
  @moduledoc """
  Durable decision mutation commands coupled to delayed expiry-job ownership.
  """

  alias Ecto.Adapters.SQL
  alias Ecto.UUID
  alias Mezzanine.Decisions.DecisionRecord
  alias Mezzanine.Execution.Repo
  alias Mezzanine.JobOutbox

  @decision_queue :decision_expiry

  @insert_decision_sql """
  INSERT INTO decision_records (
    id,
    installation_id,
    subject_id,
    execution_id,
    decision_kind,
    lifecycle_state,
    decision_value,
    required_by,
    expiry_job_id,
    resolved_at,
    reason,
    trace_id,
    causation_id,
    row_version,
    inserted_at,
    updated_at
  )
  VALUES (
    $1::uuid,
    $2,
    $3::uuid,
    $4::uuid,
    $5,
    'pending',
    NULL,
    $6,
    NULL,
    NULL,
    NULL,
    $7,
    $8,
    1,
    $9,
    $9
  )
  RETURNING id
  """

  @load_decision_sql """
  SELECT id,
         installation_id,
         subject_id,
         execution_id,
         decision_kind,
         lifecycle_state,
         decision_value,
         required_by,
         expiry_job_id,
         resolved_at,
         reason,
         trace_id,
         causation_id
  FROM decision_records
  WHERE id = $1::uuid
  FOR UPDATE
  """

  @read_decision_sql """
  SELECT id,
         installation_id,
         subject_id,
         execution_id,
         decision_kind,
         lifecycle_state,
         decision_value,
         required_by,
         expiry_job_id,
         resolved_at,
         reason,
         trace_id,
         causation_id
  FROM decision_records
  WHERE id = $1::uuid
  """

  @read_decision_by_identity_sql """
  SELECT id,
         installation_id,
         subject_id,
         execution_id,
         decision_kind,
         lifecycle_state,
         decision_value,
         required_by,
         expiry_job_id,
         resolved_at,
         reason,
         trace_id,
         causation_id
  FROM decision_records
  WHERE installation_id = $1
    AND subject_id = $2::uuid
    AND execution_id IS NOT DISTINCT FROM $3::uuid
    AND decision_kind = $4
  LIMIT 1
  """

  @update_decision_resolution_sql """
  UPDATE decision_records
  SET lifecycle_state = $2,
      decision_value = $3,
      reason = $4,
      resolved_at = $5,
      expiry_job_id = NULL,
      causation_id = $6,
      row_version = row_version + 1,
      updated_at = $5
  WHERE id = $1::uuid
  RETURNING id
  """

  @set_expiry_job_sql """
  UPDATE decision_records
  SET expiry_job_id = $2,
      updated_at = $3
  WHERE id = $1::uuid
  RETURNING id
  """

  @delete_oban_job_sql """
  DELETE FROM oban_jobs
  WHERE id = $1
  """

  @insert_audit_fact_sql """
  INSERT INTO audit_facts (
    id,
    installation_id,
    subject_id,
    execution_id,
    decision_id,
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
    $4::uuid,
    $5,
    $6,
    $7,
    $8,
    $9,
    $10,
    $10,
    $10
  )
  """

  @spec create_pending(map()) :: {:ok, DecisionRecord.t()} | {:error, term()}
  def create_pending(attrs) when is_map(attrs) do
    now = now(attrs)
    trace_id = fetch_required!(attrs, :trace_id)
    causation_id = fetch_required!(attrs, :causation_id)
    actor_ref = normalize_map(fetch_required!(attrs, :actor_ref))
    decision_id = Ecto.UUID.generate()

    Repo.transaction(fn ->
      with :ok <- insert_pending_decision(decision_id, attrs, trace_id, causation_id, now),
           {:ok, expiry_job_ref} <- maybe_enqueue_expiry_job(decision_id, attrs),
           :ok <- maybe_attach_expiry_job(decision_id, expiry_job_ref, now),
           :ok <-
             insert_audit_fact(%{
               installation_id: fetch_required!(attrs, :installation_id),
               subject_id: fetch_required!(attrs, :subject_id),
               execution_id: Map.get(attrs, :execution_id) || Map.get(attrs, "execution_id"),
               decision_id: decision_id,
               trace_id: trace_id,
               causation_id: causation_id,
               fact_kind: "decision_created",
               actor_ref: actor_ref,
               payload: %{
                 "decision_kind" => fetch_required!(attrs, :decision_kind),
                 "lifecycle_state" => "pending",
                 "required_by" =>
                   encode_optional_datetime(
                     Map.get(attrs, :required_by) || Map.get(attrs, "required_by")
                   ),
                 "expiry_job_id" => expiry_job_ref && expiry_job_ref.job_id
               },
               occurred_at: now
             }) do
        {:ok, decision_id}
      else
        {:error, error} -> Repo.rollback(error)
      end
    end)
    |> case do
      {:ok, {:ok, inserted_decision_id}} -> load_decision(inserted_decision_id)
      {:ok, inserted_decision_id} -> load_decision(inserted_decision_id)
      {:error, error} -> {:error, error}
    end
  end

  @spec fetch_by_identity(map()) :: {:ok, DecisionRecord.t() | nil} | {:error, term()}
  def fetch_by_identity(attrs) when is_map(attrs) do
    params = [
      fetch_required!(attrs, :installation_id),
      dump_uuid!(fetch_required!(attrs, :subject_id)),
      maybe_dump_uuid(Map.get(attrs, :execution_id) || Map.get(attrs, "execution_id")),
      fetch_required!(attrs, :decision_kind)
    ]

    case SQL.query(Repo, @read_decision_by_identity_sql, params) do
      {:ok, %{rows: [row]}} -> {:ok, decision_from_row(row)}
      {:ok, %{rows: []}} -> {:ok, nil}
      {:error, error} -> {:error, error}
    end
  end

  @spec decide(DecisionRecord.t() | Ecto.UUID.t(), map()) ::
          {:ok, DecisionRecord.t()} | {:error, term()}
  def decide(decision_or_id, attrs) when is_map(attrs) do
    resolve(
      decision_or_id,
      "resolved",
      fetch_required!(attrs, :decision_value),
      Map.get(attrs, :reason) || Map.get(attrs, "reason"),
      attrs,
      nil
    )
  end

  @spec waive(DecisionRecord.t() | Ecto.UUID.t(), map()) ::
          {:ok, DecisionRecord.t()} | {:error, term()}
  def waive(decision_or_id, attrs) when is_map(attrs) do
    resolve(
      decision_or_id,
      "waived",
      "waive",
      Map.get(attrs, :reason) || Map.get(attrs, "reason"),
      attrs,
      nil
    )
  end

  @spec expire(DecisionRecord.t() | Ecto.UUID.t(), map(), keyword()) ::
          {:ok, DecisionRecord.t()} | {:error, term()}
  def expire(decision_or_id, attrs, opts \\ []) when is_map(attrs) and is_list(opts) do
    resolve(
      decision_or_id,
      "expired",
      "expired",
      Map.get(attrs, :reason) || Map.get(attrs, "reason"),
      attrs,
      Keyword.get(opts, :current_job_id)
    )
  end

  defp resolve(decision_or_id, lifecycle_state, decision_value, reason, attrs, current_job_id) do
    now = now(attrs)
    decision_id = decision_id(decision_or_id)
    causation_id = fetch_required!(attrs, :causation_id)
    actor_ref = normalize_map(fetch_required!(attrs, :actor_ref))

    Repo.transaction(fn ->
      with {:ok, decision} <- fetch_decision_for_update(decision_id),
           :ok <- ensure_pending(decision),
           :ok <- maybe_delete_expiry_job(decision.expiry_job_id, current_job_id),
           :ok <-
             update_decision_resolution(
               decision.id,
               lifecycle_state,
               decision_value,
               reason,
               now,
               causation_id
             ),
           :ok <-
             insert_audit_fact(%{
               installation_id: decision.installation_id,
               subject_id: decision.subject_id,
               execution_id: decision.execution_id,
               decision_id: decision.id,
               trace_id: decision.trace_id,
               causation_id: causation_id,
               fact_kind: fact_kind_for(lifecycle_state),
               actor_ref: actor_ref,
               payload: %{
                 "decision_kind" => decision.decision_kind,
                 "lifecycle_state" => lifecycle_state,
                 "decision_value" => decision_value,
                 "reason" => reason
               },
               occurred_at: now
             }) do
        {:ok, decision.id}
      else
        {:error, error} -> Repo.rollback(error)
      end
    end)
    |> case do
      {:ok, {:ok, updated_decision_id}} -> load_decision(updated_decision_id)
      {:ok, updated_decision_id} -> load_decision(updated_decision_id)
      {:error, error} -> {:error, error}
    end
  end

  defp insert_pending_decision(decision_id, attrs, trace_id, causation_id, now) do
    params = [
      dump_uuid!(decision_id),
      fetch_required!(attrs, :installation_id),
      dump_uuid!(fetch_required!(attrs, :subject_id)),
      maybe_dump_uuid(Map.get(attrs, :execution_id) || Map.get(attrs, "execution_id")),
      fetch_required!(attrs, :decision_kind),
      Map.get(attrs, :required_by) || Map.get(attrs, "required_by"),
      trace_id,
      causation_id,
      now
    ]

    case SQL.query(Repo, @insert_decision_sql, params) do
      {:ok, _result} -> :ok
      {:error, error} -> {:error, error}
    end
  end

  defp maybe_enqueue_expiry_job(decision_id, attrs) do
    case Map.get(attrs, :required_by) || Map.get(attrs, "required_by") do
      %DateTime{} = required_by ->
        JobOutbox.enqueue(
          @decision_queue,
          Mezzanine.DecisionExpiryWorker,
          %{decision_id: decision_id},
          scheduled_at: required_by
        )

      nil ->
        {:ok, nil}
    end
  end

  defp maybe_attach_expiry_job(_decision_id, nil, _now), do: :ok

  defp maybe_attach_expiry_job(decision_id, %{job_id: job_id}, now) do
    case SQL.query(Repo, @set_expiry_job_sql, [dump_uuid!(decision_id), job_id, now]) do
      {:ok, _result} -> :ok
      {:error, error} -> {:error, error}
    end
  end

  defp fetch_decision_for_update(decision_id) do
    case SQL.query(Repo, @load_decision_sql, [dump_uuid!(decision_id)]) do
      {:ok, %{rows: [row]}} ->
        {:ok, decision_from_row(row)}

      {:ok, %{rows: []}} ->
        {:error, {:decision_not_found, decision_id}}

      {:error, error} ->
        {:error, error}
    end
  end

  defp ensure_pending(%{lifecycle_state: "pending"}), do: :ok

  defp ensure_pending(%{lifecycle_state: lifecycle_state}),
    do: {:error, {:decision_not_pending, lifecycle_state}}

  defp maybe_delete_expiry_job(nil, _current_job_id), do: :ok
  defp maybe_delete_expiry_job(expiry_job_id, expiry_job_id), do: :ok

  defp maybe_delete_expiry_job(expiry_job_id, _current_job_id) do
    case SQL.query(Repo, @delete_oban_job_sql, [expiry_job_id]) do
      {:ok, _result} -> :ok
      {:error, error} -> {:error, error}
    end
  end

  defp update_decision_resolution(
         decision_id,
         lifecycle_state,
         decision_value,
         reason,
         now,
         causation_id
       ) do
    case SQL.query(Repo, @update_decision_resolution_sql, [
           dump_uuid!(decision_id),
           lifecycle_state,
           decision_value,
           reason,
           now,
           causation_id
         ]) do
      {:ok, _result} -> :ok
      {:error, error} -> {:error, error}
    end
  end

  defp load_decision(decision_id) do
    case SQL.query(Repo, @read_decision_sql, [dump_uuid!(decision_id)]) do
      {:ok, %{rows: [row]}} -> {:ok, decision_from_row(row)}
      {:ok, %{rows: []}} -> {:error, {:decision_not_found, decision_id}}
      {:error, error} -> {:error, error}
    end
  end

  defp insert_audit_fact(attrs) do
    case SQL.query(Repo, @insert_audit_fact_sql, [
           attrs.installation_id,
           dump_uuid!(attrs.subject_id),
           maybe_dump_uuid(attrs.execution_id),
           dump_uuid!(attrs.decision_id),
           attrs.trace_id,
           attrs.causation_id,
           attrs.fact_kind,
           attrs.actor_ref,
           attrs.payload,
           attrs.occurred_at
         ]) do
      {:ok, _result} -> :ok
      {:error, error} -> {:error, error}
    end
  end

  defp fact_kind_for("resolved"), do: "decision_resolved"
  defp fact_kind_for("waived"), do: "decision_waived"
  defp fact_kind_for("expired"), do: "decision_expired"

  defp decision_id(%DecisionRecord{id: id}), do: id
  defp decision_id(id) when is_binary(id), do: id

  defp fetch_required!(attrs, key) when is_map(attrs) do
    case Map.get(attrs, key) || Map.get(attrs, to_string(key)) do
      nil -> raise ArgumentError, "missing required decision attribute #{inspect(key)}"
      value -> value
    end
  end

  defp now(attrs) do
    attrs
    |> Map.get(:now, Map.get(attrs, "now", DateTime.utc_now()))
    |> DateTime.truncate(:microsecond)
  end

  defp encode_optional_datetime(%DateTime{} = value), do: DateTime.to_iso8601(value)
  defp encode_optional_datetime(nil), do: nil

  defp normalize_map(value) when is_map(value) do
    value
    |> Enum.map(fn {key, nested} -> {to_string(key), normalize_value(nested)} end)
    |> Map.new()
  end

  defp normalize_map(_other), do: %{}

  defp normalize_value(%DateTime{} = datetime), do: DateTime.to_iso8601(datetime)
  defp normalize_value(value) when is_map(value), do: normalize_map(value)
  defp normalize_value(value) when is_list(value), do: Enum.map(value, &normalize_value/1)
  defp normalize_value(value) when is_atom(value), do: Atom.to_string(value)
  defp normalize_value(value), do: value

  defp decision_from_row([
         id,
         installation_id,
         subject_id,
         execution_id,
         decision_kind,
         lifecycle_state,
         decision_value,
         required_by,
         expiry_job_id,
         resolved_at,
         reason,
         trace_id,
         causation_id
       ]) do
    struct(DecisionRecord, %{
      id: normalize_uuid(id),
      installation_id: installation_id,
      subject_id: normalize_uuid(subject_id),
      execution_id: normalize_uuid(execution_id),
      decision_kind: decision_kind,
      lifecycle_state: lifecycle_state,
      decision_value: decision_value,
      required_by: normalize_datetime(required_by),
      expiry_job_id: expiry_job_id,
      resolved_at: normalize_datetime(resolved_at),
      reason: reason,
      trace_id: trace_id,
      causation_id: causation_id
    })
  end

  defp dump_uuid!(<<_::128>> = uuid), do: uuid
  defp dump_uuid!(uuid), do: UUID.dump!(uuid)
  defp maybe_dump_uuid(nil), do: nil
  defp maybe_dump_uuid(uuid), do: dump_uuid!(uuid)

  defp normalize_uuid(nil), do: nil
  defp normalize_uuid(<<_::128>> = uuid), do: UUID.load!(uuid)
  defp normalize_uuid(uuid), do: uuid

  defp normalize_datetime(nil), do: nil
  defp normalize_datetime(%DateTime{} = datetime), do: datetime

  defp normalize_datetime(%NaiveDateTime{} = datetime),
    do: DateTime.from_naive!(datetime, "Etc/UTC")
end
