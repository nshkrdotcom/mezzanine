defmodule Mezzanine.Projections.ReceiptReducer do
  @moduledoc """
  Reduces terminal lower receipts into Mezzanine-owned ledgers and projections.

  The reducer accepts durable refs carried by workflow state/lower receipts. It
  does not discover provider objects through static selectors and it does not
  read process environment.
  """

  alias Mezzanine.Audit.AuditAppend
  alias Mezzanine.DecisionCommands
  alias Mezzanine.EvidenceLedger.EvidenceRecord
  alias Mezzanine.Execution.ExecutionRecord
  alias Mezzanine.Objects.SubjectRecord
  alias Mezzanine.Projections.ProjectionRow

  @terminal_success_states ["completed", "success", "succeeded"]
  @terminal_cancel_states ["cancelled", "canceled"]
  @terminal_blocked_states ["input_required", "blocked"]
  @terminal_failure_states ["failed", "failure", "approval_required", "semantic_failure"]
  @terminal_failed_or_blocked_states @terminal_blocked_states ++ @terminal_failure_states
  @decision_kind "operator_review_required"
  @projection_name "operator_subject_runtime"

  @type reduce_result :: %{
          execution: ExecutionRecord.t(),
          subject: SubjectRecord.t(),
          decisions: [map()],
          evidence: [EvidenceRecord.t()],
          projection: map(),
          audit: map()
        }

  @spec reduce(map() | keyword()) :: {:ok, reduce_result()} | {:error, term()}
  def reduce(attrs) when is_map(attrs) or is_list(attrs) do
    attrs = normalize_attrs(attrs)

    with {:ok, execution} <- fetch_execution(required!(attrs, :execution_id)),
         {:ok, subject} <- fetch_subject(required!(attrs, :subject_id)),
         receipt_state <- receipt_state(attrs),
         {:ok, execution} <- reduce_execution(execution, attrs, receipt_state),
         {:ok, subject} <- reduce_subject(subject, attrs, receipt_state),
         {:ok, decisions} <- reduce_decisions(subject, execution, attrs, receipt_state),
         {:ok, evidence} <- reduce_evidence(subject, execution, attrs),
         {:ok, projection} <-
           upsert_runtime_projection(
             subject,
             execution,
             decisions,
             evidence,
             attrs,
             receipt_state
           ),
         {:ok, audit} <- append_receipt_audit(subject, execution, attrs, receipt_state) do
      {:ok,
       %{
         execution: execution,
         subject: subject,
         decisions: decisions,
         evidence: evidence,
         projection: projection,
         audit: audit
       }}
    end
  end

  defp reduce_execution(execution, attrs, receipt_state)
       when receipt_state in @terminal_success_states do
    ExecutionRecord.record_completed(execution, execution_attrs(attrs))
  end

  defp reduce_execution(execution, attrs, receipt_state)
       when receipt_state in @terminal_cancel_states do
    ExecutionRecord.record_cancelled_outcome(execution, execution_attrs(attrs))
  end

  defp reduce_execution(execution, attrs, receipt_state)
       when receipt_state in @terminal_failed_or_blocked_states do
    attrs =
      attrs
      |> execution_attrs()
      |> Map.put(:failure_kind, failure_kind(receipt_state))

    ExecutionRecord.record_failed_outcome(execution, attrs)
  end

  defp reduce_execution(_execution, _attrs, receipt_state),
    do: {:error, {:unsupported_terminal_receipt_state, receipt_state}}

  defp execution_attrs(attrs) do
    %{
      receipt_id: required!(attrs, :receipt_id),
      lower_receipt: value(attrs, :lower_receipt) || %{},
      normalized_outcome: value(attrs, :normalized_outcome) || %{},
      artifact_refs: value(attrs, :artifact_refs) || artifact_refs(attrs),
      trace_id: required!(attrs, :trace_id),
      causation_id: required!(attrs, :causation_id),
      actor_ref: required!(attrs, :actor_ref)
    }
  end

  defp reduce_subject(subject, attrs, receipt_state)
       when receipt_state in @terminal_success_states do
    lifecycle_state =
      if truthy?(value(attrs, :review_required?)), do: "awaiting_review", else: "completed"

    advance_subject(subject, attrs, lifecycle_state)
  end

  defp reduce_subject(subject, attrs, receipt_state)
       when receipt_state in @terminal_cancel_states do
    with {:ok, subject} <- advance_subject(subject, attrs, "cancelled") do
      SubjectRecord.cancel(subject, %{
        reason: "lower_cancelled",
        trace_id: required!(attrs, :trace_id),
        causation_id: required!(attrs, :causation_id),
        actor_ref: required!(attrs, :actor_ref),
        operator_context: %{receipt_id: required!(attrs, :receipt_id)}
      })
    end
  end

  defp reduce_subject(subject, attrs, receipt_state)
       when receipt_state in @terminal_blocked_states do
    with {:ok, subject} <- block_subject(subject, attrs, receipt_state) do
      advance_subject(subject, attrs, "blocked")
    end
  end

  defp reduce_subject(subject, attrs, receipt_state)
       when receipt_state in @terminal_failure_states do
    advance_subject(subject, attrs, "failed")
  end

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

  defp block_subject(subject, attrs, reason) do
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

  defp reduce_decisions(subject, execution, attrs, receipt_state)
       when receipt_state in @terminal_success_states do
    if truthy?(value(attrs, :review_required?)) do
      attrs = %{
        installation_id: subject.installation_id,
        subject_id: subject.id,
        execution_id: execution.id,
        decision_kind: @decision_kind,
        required_by: value(attrs, :review_required_by),
        trace_id: required!(attrs, :trace_id),
        causation_id: required!(attrs, :causation_id),
        actor_ref: required!(attrs, :actor_ref)
      }

      case DecisionCommands.fetch_by_identity(attrs) do
        {:ok, nil} -> create_pending_decision(attrs)
        {:ok, decision} -> {:ok, [decision]}
        {:error, reason} -> {:error, reason}
      end
    else
      {:ok, []}
    end
  end

  defp reduce_decisions(_subject, _execution, _attrs, _receipt_state), do: {:ok, []}

  defp create_pending_decision(attrs) do
    case DecisionCommands.create_pending(attrs) do
      {:ok, decision} -> {:ok, [decision]}
      {:error, reason} -> {:error, reason}
    end
  end

  defp reduce_evidence(subject, execution, attrs) do
    attrs
    |> evidence_specs()
    |> Enum.reduce_while({:ok, []}, fn spec, {:ok, collected} ->
      case ensure_evidence(subject, execution, attrs, spec) do
        {:ok, evidence} -> {:cont, {:ok, [evidence | collected]}}
        {:error, reason} -> {:halt, {:error, reason}}
      end
    end)
    |> case do
      {:ok, rows} -> {:ok, Enum.reverse(rows)}
      {:error, reason} -> {:error, reason}
    end
  end

  defp ensure_evidence(subject, execution, attrs, spec) do
    case EvidenceRecord.by_subject_execution_kind(subject.id, execution.id, spec.kind) do
      {:ok, %EvidenceRecord{} = evidence} ->
        {:ok, evidence}

      {:ok, nil} ->
        collect_evidence(subject, execution, attrs, spec)

      {:error, error} ->
        if not_found?(error),
          do: collect_evidence(subject, execution, attrs, spec),
          else: {:error, error}
    end
  end

  defp collect_evidence(subject, execution, attrs, spec) do
    EvidenceRecord.collect(%{
      installation_id: subject.installation_id,
      subject_id: subject.id,
      execution_id: execution.id,
      evidence_kind: spec.kind,
      collector_ref: spec.collector_ref,
      content_ref: spec.content_ref,
      status: "verified",
      metadata: %{
        receipt_id: required!(attrs, :receipt_id),
        lower_receipt_ref: value(attrs, :lower_receipt_ref),
        evidence_source: "lower_receipt"
      },
      trace_id: required!(attrs, :trace_id),
      causation_id: required!(attrs, :causation_id),
      actor_ref: required!(attrs, :actor_ref)
    })
  end

  defp upsert_runtime_projection(subject, execution, decisions, evidence, attrs, receipt_state) do
    ProjectionRow.upsert(%{
      installation_id: subject.installation_id,
      projection_name: @projection_name,
      row_key: subject.id,
      subject_id: subject.id,
      execution_id: execution.id,
      projection_kind: "operator_runtime",
      sort_key: sort_key(subject, execution),
      trace_id: required!(attrs, :trace_id),
      causation_id: required!(attrs, :causation_id),
      payload: runtime_payload(subject, execution, decisions, evidence, attrs, receipt_state),
      computed_at: DateTime.utc_now()
    })
  end

  defp runtime_payload(subject, execution, decisions, evidence, attrs, receipt_state) do
    lower_receipt = value(attrs, :lower_receipt) || %{}

    %{
      subject: %{
        subject_id: subject.id,
        lifecycle_state: subject.lifecycle_state,
        status: subject.status,
        block_reason: subject.block_reason
      },
      execution: %{
        execution_id: execution.id,
        dispatch_state: Atom.to_string(execution.dispatch_state),
        failure_kind: maybe_atom_to_string(execution.failure_kind)
      },
      lower_receipt: %{
        receipt_id: required!(attrs, :receipt_id),
        receipt_state: receipt_state,
        lower_receipt_ref: value(attrs, :lower_receipt_ref),
        run_id: map_value(lower_receipt, :run_id),
        attempt_id: map_value(lower_receipt, :attempt_id)
      },
      runtime: %{
        token_totals: map_value(lower_receipt, :token_totals) || %{},
        rate_limit: map_value(lower_receipt, :rate_limit) || %{},
        event_counts: event_counts(map_value(lower_receipt, :runtime_events) || [])
      },
      review: %{
        pending_decision_ids: Enum.map(decisions, & &1.id)
      },
      evidence: %{
        evidence_refs: Enum.map(evidence, &evidence_projection/1)
      }
    }
  end

  defp evidence_projection(%EvidenceRecord{} = evidence) do
    %{
      evidence_id: evidence.id,
      evidence_kind: evidence.evidence_kind,
      content_ref: evidence.content_ref,
      status: evidence.status
    }
  end

  defp append_receipt_audit(subject, execution, attrs, receipt_state) do
    AuditAppend.append_fact(%{
      installation_id: subject.installation_id,
      subject_id: subject.id,
      execution_id: execution.id,
      trace_id: required!(attrs, :trace_id),
      causation_id: required!(attrs, :causation_id),
      fact_kind: :receipt_reduced,
      actor_ref: required!(attrs, :actor_ref),
      payload: %{
        receipt_id: required!(attrs, :receipt_id),
        receipt_state: receipt_state,
        lower_receipt_ref: value(attrs, :lower_receipt_ref),
        projection_name: @projection_name
      },
      occurred_at: DateTime.utc_now()
    })
  end

  defp evidence_specs(attrs) do
    refs =
      attrs
      |> value(:lower_receipt)
      |> case do
        %{} = receipt -> map_value(receipt, :artifact_refs) || []
        _other -> []
      end

    refs_by_kind =
      Map.new(refs, fn ref ->
        ref = normalize_attrs(ref)
        {required!(ref, :kind), ref}
      end)

    attrs
    |> value(:required_evidence)
    |> List.wrap()
    |> Enum.map(fn kind ->
      kind = to_string(kind)
      ref = Map.get(refs_by_kind, kind, %{})

      %{
        kind: kind,
        content_ref: value(ref, :content_ref) || "artifact://#{kind}",
        collector_ref: value(ref, :collector_ref) || "receipt_reducer"
      }
    end)
  end

  defp artifact_refs(attrs) do
    attrs
    |> value(:lower_receipt)
    |> case do
      %{} = receipt -> map_value(receipt, :artifact_refs) || []
      _other -> []
    end
    |> Enum.map(fn ref -> map_value(ref, :content_ref) end)
    |> Enum.reject(&is_nil/1)
  end

  defp event_counts(events) do
    Enum.frequencies_by(events, fn event ->
      event
      |> map_value(:event_kind)
      |> to_string()
    end)
  end

  defp sort_key(subject, execution) do
    case {subject.lifecycle_state, execution.dispatch_state} do
      {"awaiting_review", _state} -> 10
      {_state, :failed} -> 20
      {_state, :cancelled} -> 30
      {_state, :completed} -> 40
      _other -> 50
    end
  end

  defp receipt_state(attrs) do
    attrs
    |> value(:receipt_state)
    |> case do
      nil -> attrs |> value(:normalized_outcome) |> map_value(:state)
      state -> state
    end
    |> to_string()
  end

  defp failure_kind("approval_required"), do: :semantic_failure
  defp failure_kind("input_required"), do: :semantic_failure
  defp failure_kind("blocked"), do: :semantic_failure
  defp failure_kind("semantic_failure"), do: :semantic_failure
  defp failure_kind(_state), do: :fatal_error

  defp fetch_execution(execution_id) do
    case Ash.get(ExecutionRecord, execution_id) do
      {:ok, %ExecutionRecord{} = execution} -> {:ok, execution}
      {:ok, nil} -> {:error, :execution_not_found}
      {:error, reason} -> {:error, reason}
    end
  end

  defp fetch_subject(subject_id) do
    case Ash.get(SubjectRecord, subject_id) do
      {:ok, %SubjectRecord{} = subject} -> {:ok, subject}
      {:ok, nil} -> {:error, :subject_not_found}
      {:error, reason} -> {:error, reason}
    end
  end

  defp maybe_atom_to_string(nil), do: nil
  defp maybe_atom_to_string(value) when is_atom(value), do: Atom.to_string(value)
  defp maybe_atom_to_string(value), do: to_string(value)

  defp truthy?(value), do: value in [true, "true", 1, "1"]

  defp not_found?(%Ash.Error.Query.NotFound{}), do: true

  defp not_found?(%Ash.Error.Invalid{errors: errors}) when is_list(errors) do
    Enum.any?(errors, &not_found?/1)
  end

  defp not_found?(_error), do: false

  defp normalize_attrs(attrs) when is_list(attrs), do: Map.new(attrs)
  defp normalize_attrs(%{} = attrs), do: Map.new(attrs)

  defp required!(attrs, key) do
    case value(attrs, key) do
      nil -> raise ArgumentError, "missing required receipt reducer field #{inspect(key)}"
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
