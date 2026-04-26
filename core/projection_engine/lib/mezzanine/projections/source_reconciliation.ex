defmodule Mezzanine.Projections.SourceReconciliation do
  @moduledoc """
  Applies source drift reconciliation into subject state and projection rows.

  Provider identity is supplied by source admission/workflow state. This module
  never accepts static provider-object selectors as configuration.
  """

  alias Mezzanine.Audit.AuditAppend
  alias Mezzanine.Objects.SubjectRecord
  alias Mezzanine.Projections.ProjectionRow

  @queue_projection "source_reconciliation_queue"
  @revalidation_projection "source_revalidation_queue"
  @totals_projection "source_reconciliation_totals"

  @spec reconcile(map() | keyword()) :: {:ok, map()} | {:error, term()}
  def reconcile(attrs) when is_map(attrs) or is_list(attrs) do
    attrs = normalize_attrs(attrs)

    with {:ok, subject} <- fetch_subject(required!(attrs, :subject_id)),
         action <- classify(attrs),
         {:ok, subject} <- apply_subject_action(subject, attrs, action),
         {:ok, projection} <- upsert_action_projection(subject, attrs, action),
         {:ok, totals} <- maybe_upsert_totals(subject, attrs, action),
         {:ok, audit} <- append_reconciliation_audit(subject, attrs, action) do
      {:ok,
       %{subject: subject, action: action, projection: projection, totals: totals, audit: audit}}
    end
  end

  defp classify(attrs) do
    cond do
      value(attrs, :source_visible?) == false ->
        action("source_missing", "stop_lower_run", "quarantine_subject")

      value(attrs, :assigned_to_current_worker?) == false ->
        action("source_reassigned", "stop_lower_run", "block_subject")

      non_terminal_blockers?(value(attrs, :blocker_refs)) ->
        action("blocked_by_source", "skip_dispatch", "block_subject")

      truthy?(value(attrs, :stale?)) ->
        action("stale_source", "retry_source_refresh", "enqueue_revalidation")

      terminal_source?(value(attrs, :canonical_state)) ->
        action("terminal_source", "stop_lower_run", "complete_subject")

      truthy?(value(attrs, :payload_changed?)) ->
        action("source_updated", "refresh_subject_projection", "refresh_projection")

      true ->
        action("source_active", "continue_workflow", "refresh_projection")
    end
  end

  defp action(reason, safe_action, mutation) do
    %{reason: reason, safe_action: safe_action, mutation: mutation}
  end

  defp apply_subject_action(subject, attrs, %{mutation: "complete_subject"}) do
    advance_subject(subject, attrs, "completed")
  end

  defp apply_subject_action(subject, attrs, %{mutation: "quarantine_subject"}) do
    advance_subject(subject, attrs, "quarantined")
  end

  defp apply_subject_action(subject, attrs, %{mutation: "block_subject", reason: reason}) do
    if subject.block_reason == reason do
      {:ok, subject}
    else
      SubjectRecord.block(subject, %{
        block_reason: reason,
        trace_id: required!(attrs, :trace_id),
        causation_id: required!(attrs, :causation_id),
        actor_ref: required!(attrs, :actor_ref)
      })
    end
  end

  defp apply_subject_action(subject, _attrs, _action), do: {:ok, subject}

  defp advance_subject(subject, attrs, lifecycle_state) do
    if subject.lifecycle_state == lifecycle_state do
      {:ok, subject}
    else
      SubjectRecord.advance_lifecycle(subject, %{
        lifecycle_state: lifecycle_state,
        trace_id: required!(attrs, :trace_id),
        causation_id: required!(attrs, :causation_id),
        actor_ref: required!(attrs, :actor_ref)
      })
    end
  end

  defp upsert_action_projection(subject, attrs, %{safe_action: "retry_source_refresh"} = action) do
    ProjectionRow.upsert(%{
      installation_id: required!(attrs, :installation_id),
      projection_name: @revalidation_projection,
      row_key: subject.id,
      subject_id: subject.id,
      execution_id: value(attrs, :execution_id),
      projection_kind: "source_revalidation",
      sort_key: 10,
      trace_id: required!(attrs, :trace_id),
      causation_id: required!(attrs, :causation_id),
      payload: action_payload(subject, attrs, action),
      computed_at: DateTime.utc_now()
    })
  end

  defp upsert_action_projection(subject, attrs, action) do
    ProjectionRow.upsert(%{
      installation_id: required!(attrs, :installation_id),
      projection_name: @queue_projection,
      row_key: subject.id,
      subject_id: subject.id,
      execution_id: value(attrs, :execution_id),
      projection_kind: "source_reconciliation",
      sort_key: action_sort_key(action),
      trace_id: required!(attrs, :trace_id),
      causation_id: required!(attrs, :causation_id),
      payload: action_payload(subject, attrs, action),
      computed_at: DateTime.utc_now()
    })
  end

  defp maybe_upsert_totals(subject, attrs, %{mutation: "complete_subject"}) do
    current_subject_ids =
      case ProjectionRow.row_by_key(
             required!(attrs, :installation_id),
             @totals_projection,
             "completed"
           ) do
        {:ok, %ProjectionRow{} = row} -> Map.get(row.payload, "subject_ids", [])
        _other -> []
      end

    subject_ids = Enum.uniq([subject.id | current_subject_ids])

    ProjectionRow.upsert(%{
      installation_id: required!(attrs, :installation_id),
      projection_name: @totals_projection,
      row_key: "completed",
      subject_id: subject.id,
      execution_id: value(attrs, :execution_id),
      projection_kind: "source_reconciliation_total",
      sort_key: 0,
      trace_id: required!(attrs, :trace_id),
      causation_id: required!(attrs, :causation_id),
      payload: %{"completed_count" => length(subject_ids), "subject_ids" => subject_ids},
      computed_at: DateTime.utc_now()
    })
  end

  defp maybe_upsert_totals(_subject, _attrs, _action), do: {:ok, nil}

  defp action_payload(subject, attrs, action) do
    %{
      subject_id: subject.id,
      execution_id: value(attrs, :execution_id),
      reason: action.reason,
      safe_action: action.safe_action,
      mutation: action.mutation,
      source_state: value(attrs, :source_state),
      canonical_state: value(attrs, :canonical_state),
      source_revision: value(attrs, :source_revision),
      retry_at: iso8601(value(attrs, :retry_at)),
      blocker_refs: value(attrs, :blocker_refs) || []
    }
  end

  defp append_reconciliation_audit(subject, attrs, action) do
    AuditAppend.append_fact(%{
      installation_id: required!(attrs, :installation_id),
      subject_id: subject.id,
      execution_id: value(attrs, :execution_id),
      trace_id: required!(attrs, :trace_id),
      causation_id: required!(attrs, :causation_id),
      fact_kind: :source_reconciled,
      actor_ref: required!(attrs, :actor_ref),
      payload: action_payload(subject, attrs, action),
      occurred_at: DateTime.utc_now()
    })
  end

  defp terminal_source?(state) when is_binary(state),
    do: state in ["completed", "cancelled", "canceled", "closed", "done", "rejected", "expired"]

  defp terminal_source?(_state), do: false

  defp non_terminal_blockers?(blockers) when is_list(blockers) do
    Enum.any?(blockers, fn blocker ->
      not truthy?(map_value(blocker, :terminal?))
    end)
  end

  defp non_terminal_blockers?(_blockers), do: false

  defp action_sort_key(%{safe_action: "stop_lower_run"}), do: 0
  defp action_sort_key(%{safe_action: "skip_dispatch"}), do: 10
  defp action_sort_key(%{safe_action: "refresh_subject_projection"}), do: 20
  defp action_sort_key(_action), do: 50

  defp fetch_subject(subject_id) do
    case Ash.get(SubjectRecord, subject_id) do
      {:ok, %SubjectRecord{} = subject} -> {:ok, subject}
      {:ok, nil} -> {:error, :subject_not_found}
      {:error, reason} -> {:error, reason}
    end
  end

  defp iso8601(nil), do: nil
  defp iso8601(%DateTime{} = datetime), do: DateTime.to_iso8601(datetime)
  defp iso8601(value), do: value

  defp truthy?(value), do: value in [true, "true", 1, "1"]

  defp normalize_attrs(attrs) when is_list(attrs), do: Map.new(attrs)
  defp normalize_attrs(%{} = attrs), do: Map.new(attrs)

  defp required!(attrs, key) do
    case value(attrs, key) do
      nil -> raise ArgumentError, "missing required source reconciliation field #{inspect(key)}"
      value -> value
    end
  end

  defp value(attrs, key) when is_map(attrs) do
    case Map.fetch(attrs, key) do
      {:ok, value} -> value
      :error -> Map.get(attrs, Atom.to_string(key))
    end
  end

  defp map_value(nil, _key), do: nil

  defp map_value(%{} = attrs, key) do
    case Map.fetch(attrs, key) do
      {:ok, value} -> value
      :error -> Map.get(attrs, Atom.to_string(key))
    end
  end

  defp map_value(_attrs, _key), do: nil
end
