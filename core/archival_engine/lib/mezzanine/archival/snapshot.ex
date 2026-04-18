defmodule Mezzanine.Archival.Snapshot do
  @moduledoc false

  alias Ecto.Adapters.SQL
  alias Mezzanine.Archival.BundleChecksum
  alias Mezzanine.Archival.Query
  alias Mezzanine.Archival.Repo

  @spec build(String.t(), Ecto.UUID.t(), keyword()) :: {:ok, map()} | {:error, term()}
  def build(installation_id, subject_id, opts \\ [])
      when is_binary(installation_id) and is_binary(subject_id) do
    with {:ok, subject} <- fetch_subject(installation_id, subject_id),
         %DateTime{} = terminal_at <- coerce_datetime(subject["terminal_at"]) do
      dumped_subject_id = dump_uuid!(subject_id)
      executions = fetch_rows!(execution_sql(), [installation_id, dumped_subject_id])
      decisions = fetch_rows!(decision_sql(), [installation_id, dumped_subject_id])
      evidence = fetch_rows!(evidence_sql(), [installation_id, dumped_subject_id])
      audit_facts = fetch_rows!(audit_sql(), [installation_id, subject_id])
      barriers = fetch_rows!(barrier_sql(), [dumped_subject_id])
      barrier_completions = fetch_barrier_completions!(barriers)
      now = Keyword.get(opts, :now, DateTime.utc_now())

      trace_ids =
        [subject | executions ++ decisions ++ evidence ++ audit_facts]
        |> Enum.flat_map(&List.wrap(Map.get(&1, "trace_id")))
        |> Enum.reject(&is_nil/1)
        |> Enum.uniq()
        |> Enum.sort()

      bundle =
        %{
          "manifest_ref" => Query.build_manifest_ref(installation_id, subject_id, terminal_at),
          "installation_id" => installation_id,
          "subject_id" => subject_id,
          "captured_at" => now,
          "subject" => json_safe(subject),
          "audit_facts" => json_safe(audit_facts),
          "executions" => json_safe(executions),
          "decisions" => json_safe(decisions),
          "evidence" => json_safe(evidence),
          "barriers" => json_safe(barriers),
          "barrier_completions" => json_safe(barrier_completions),
          "trace_views" =>
            build_trace_views(
              json_safe(audit_facts),
              json_safe(executions),
              json_safe(decisions),
              json_safe(evidence)
            )
        }
        |> attach_checksum()

      {:ok,
       %{
         manifest_ref: bundle["manifest_ref"],
         installation_id: installation_id,
         subject_id: subject_id,
         subject_state: Map.get(subject, "lifecycle_state"),
         execution_states:
           executions
           |> Enum.map(&(Map.get(&1, "dispatch_state") || Map.get(&1, "status")))
           |> Enum.reject(&is_nil/1),
         trace_ids: trace_ids,
         execution_ids: Enum.map(executions, &Map.fetch!(&1, "id")),
         decision_ids: Enum.map(decisions, &Map.fetch!(&1, "id")),
         evidence_ids: Enum.map(evidence, &Map.fetch!(&1, "id")),
         audit_fact_ids: Enum.map(audit_facts, &Map.fetch!(&1, "id")),
         projection_names: [],
         terminal_at: terminal_at,
         bundle: bundle,
         row_counts: %{
           subject: 1,
           executions: length(executions),
           decisions: length(decisions),
           evidence: length(evidence),
           audit_facts: length(audit_facts),
           barriers: length(barriers),
           barrier_completions: length(barrier_completions)
         }
       }}
    else
      nil -> {:error, :subject_not_terminal}
      {:error, reason} -> {:error, reason}
    end
  end

  @spec delete_hot_rows!(map()) :: %{atom() => non_neg_integer()}
  def delete_hot_rows!(snapshot) when is_map(snapshot) do
    barrier_ids =
      snapshot
      |> Map.get(:bundle)
      |> Map.get("barriers", [])
      |> Enum.map(&Map.fetch!(&1, "id"))

    %{
      barrier_completions:
        delete_rows!(
          "DELETE FROM parallel_barrier_completions WHERE barrier_id = ANY($1::uuid[])",
          [dump_uuid_list(barrier_ids)]
        ),
      barriers:
        delete_rows!(
          "DELETE FROM parallel_barriers WHERE id = ANY($1::uuid[])",
          [dump_uuid_list(barrier_ids)]
        ),
      audit_facts:
        delete_rows!(
          "DELETE FROM audit_facts WHERE id = ANY($1::uuid[])",
          [dump_uuid_list(Map.get(snapshot, :audit_fact_ids, []))]
        ),
      evidence:
        delete_rows!(
          "DELETE FROM evidence_records WHERE id = ANY($1::uuid[])",
          [dump_uuid_list(Map.get(snapshot, :evidence_ids, []))]
        ),
      decisions:
        delete_rows!(
          "DELETE FROM decision_records WHERE id = ANY($1::uuid[])",
          [dump_uuid_list(Map.get(snapshot, :decision_ids, []))]
        ),
      executions:
        delete_rows!(
          "DELETE FROM execution_records WHERE id = ANY($1::uuid[])",
          [dump_uuid_list(Map.get(snapshot, :execution_ids, []))]
        ),
      subjects:
        delete_rows!(
          "DELETE FROM subject_records WHERE installation_id = $1 AND id = $2::uuid",
          [snapshot.installation_id, dump_uuid!(snapshot.subject_id)]
        )
    }
  end

  defp fetch_subject(installation_id, subject_id) do
    case fetch_rows!(subject_sql(), [installation_id, dump_uuid!(subject_id)]) do
      [subject] -> {:ok, subject}
      _ -> {:error, :subject_not_found}
    end
  end

  defp fetch_barrier_completions!([]), do: []

  defp fetch_barrier_completions!(barriers),
    do: fetch_rows!(barrier_completion_sql(), [Enum.map(barriers, &dump_uuid!(&1["id"]))])

  defp fetch_rows!(sql, params) do
    case SQL.query(Repo, sql, params) do
      {:ok, result} ->
        Enum.map(result.rows, fn row ->
          result.columns
          |> Enum.zip(row)
          |> Map.new()
        end)

      {:error, error} ->
        raise error
    end
  end

  defp delete_rows!(_sql, [[]]), do: 0

  defp delete_rows!(sql, params) do
    case SQL.query(Repo, sql, params) do
      {:ok, result} -> result.num_rows
      {:error, error} -> raise error
    end
  end

  defp build_trace_views(audit_facts, executions, decisions, evidence) do
    [audit_facts, executions, decisions, evidence]
    |> List.flatten()
    |> Enum.group_by(&Map.get(&1, "trace_id"))
    |> Enum.reject(fn {trace_id, _rows} -> is_nil(trace_id) end)
    |> Map.new(fn {trace_id, _rows} ->
      {trace_id,
       %{
         "audit_facts" => Enum.filter(audit_facts, &(Map.get(&1, "trace_id") == trace_id)),
         "executions" => Enum.filter(executions, &(Map.get(&1, "trace_id") == trace_id)),
         "decisions" => Enum.filter(decisions, &(Map.get(&1, "trace_id") == trace_id)),
         "evidence" => Enum.filter(evidence, &(Map.get(&1, "trace_id") == trace_id))
       }}
    end)
  end

  defp json_safe(%DateTime{} = value), do: DateTime.to_iso8601(value)
  defp json_safe(%NaiveDateTime{} = value), do: NaiveDateTime.to_iso8601(value)
  defp json_safe(nil), do: nil
  defp json_safe(true), do: true
  defp json_safe(false), do: false

  defp json_safe(%{} = map) do
    map
    |> Enum.map(fn {key, value} -> {to_string(key), json_safe(value)} end)
    |> Map.new()
  end

  defp json_safe(value) when is_list(value), do: Enum.map(value, &json_safe/1)
  defp json_safe(value) when is_atom(value), do: Atom.to_string(value)
  defp json_safe(value), do: value

  defp attach_checksum(bundle) do
    Map.put(bundle, "checksum", BundleChecksum.generate(bundle))
  end

  defp coerce_datetime(%DateTime{} = value), do: value
  defp coerce_datetime(%NaiveDateTime{} = value), do: DateTime.from_naive!(value, "Etc/UTC")

  defp coerce_datetime(value) when is_binary(value) do
    case DateTime.from_iso8601(value) do
      {:ok, datetime, _offset} -> datetime
      _ -> nil
    end
  end

  defp coerce_datetime(_value), do: nil

  defp dump_uuid!(value) when is_binary(value), do: Ecto.UUID.dump!(value)

  defp dump_uuid_list(values) when is_list(values), do: Enum.map(values, &dump_uuid!/1)

  defp subject_sql do
    """
    SELECT
      id::text,
      installation_id,
      source_ref,
      subject_kind,
      lifecycle_state,
      status,
      status_reason,
      status_updated_at,
      terminal_at,
      title,
      description,
      schema_ref,
      schema_version,
      payload,
      opened_at,
      blocked_at,
      block_reason,
      inserted_at,
      updated_at
    FROM subject_records
    WHERE installation_id = $1 AND id = $2::uuid
    LIMIT 1
    """
  end

  defp execution_sql do
    """
    SELECT
      id::text,
      tenant_id,
      installation_id,
      subject_id::text,
      barrier_id::text,
      recipe_ref,
      compiled_pack_revision,
      binding_snapshot,
      dispatch_envelope,
      intent_snapshot,
      dispatch_state,
      dispatch_attempt_count,
      submission_dedupe_key,
      submission_ref,
      lower_receipt,
      next_dispatch_at,
      last_dispatch_error_kind,
      last_dispatch_error_payload,
      terminal_rejection_reason,
      trace_id,
      causation_id,
      failure_kind,
      supersedes_execution_id::text,
      supersession_reason,
      supersession_depth,
      row_version,
      inserted_at,
      updated_at
    FROM execution_records
    WHERE installation_id = $1 AND subject_id = $2::uuid
    ORDER BY inserted_at ASC, id ASC
    """
  end

  defp decision_sql do
    """
    SELECT
      id::text,
      installation_id,
      subject_id::text,
      execution_id::text,
      decision_kind,
      lifecycle_state,
      decision_value,
      reason,
      required_by,
      resolved_at,
      trace_id,
      causation_id,
      expiry_job_id::text,
      row_version,
      inserted_at,
      updated_at
    FROM decision_records
    WHERE installation_id = $1 AND subject_id = $2::uuid
    ORDER BY inserted_at ASC, id ASC
    """
  end

  defp evidence_sql do
    """
    SELECT
      id::text,
      installation_id,
      subject_id::text,
      execution_id::text,
      evidence_kind,
      collector_ref,
      content_ref,
      status,
      metadata,
      collected_at,
      verified_at,
      trace_id,
      causation_id,
      row_version,
      inserted_at,
      updated_at
    FROM evidence_records
    WHERE installation_id = $1 AND subject_id = $2::uuid
    ORDER BY inserted_at ASC, id ASC
    """
  end

  defp audit_sql do
    """
    SELECT
      id::text,
      installation_id,
      subject_id::text,
      execution_id::text,
      decision_id::text,
      evidence_id::text,
      trace_id,
      causation_id,
      fact_kind,
      actor_ref,
      payload,
      occurred_at,
      inserted_at,
      updated_at
    FROM audit_facts
    WHERE installation_id = $1 AND subject_id = $2
    ORDER BY occurred_at ASC, id ASC
    """
  end

  defp barrier_sql do
    """
    SELECT
      id::text,
      subject_id::text,
      barrier_key,
      join_step_ref,
      status,
      expected_children,
      completed_children,
      trace_id,
      inserted_at,
      updated_at
    FROM parallel_barriers
    WHERE subject_id = $1::uuid
    ORDER BY inserted_at ASC, id ASC
    """
  end

  defp barrier_completion_sql do
    """
    SELECT
      id::text,
      barrier_id::text,
      child_execution_id::text,
      inserted_at,
      updated_at
    FROM parallel_barrier_completions
    WHERE barrier_id = ANY($1::uuid[])
    ORDER BY inserted_at ASC, id ASC
    """
  end
end
