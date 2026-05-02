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
    missing_evidence = missing_required_evidence(attrs)

    attrs =
      if missing_evidence == [] do
        attrs
      else
        attrs
        |> Map.put(:missing_required_evidence, missing_evidence)
        |> Map.put(:block_reason, "missing_required_evidence")
      end

    with {:ok, execution} <- fetch_execution(required!(attrs, :execution_id)),
         {:ok, subject} <- fetch_subject(required!(attrs, :subject_id)),
         receipt_state <- effective_receipt_state(attrs, receipt_state(attrs), missing_evidence),
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
    with {:ok, subject} <-
           block_subject(subject, attrs, value(attrs, :block_reason) || receipt_state) do
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
    missing_required_evidence = value(attrs, :missing_required_evidence) || []

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
        token_dedupe: token_dedupe_projection(lower_receipt),
        rate_limit: rate_limit_projection(lower_receipt),
        retry_queue: retry_queue_projection(lower_receipt),
        event_counts: event_counts(map_value(lower_receipt, :runtime_events) || [])
      },
      review: %{
        pending_decision_ids: Enum.map(decisions, & &1.id)
      },
      evidence: %{
        evidence_refs: Enum.map(evidence, &evidence_projection/1),
        aitrace: aitrace_projection(lower_receipt)
      },
      prompt: prompt_projection(lower_receipt),
      semantic: semantic_projection(lower_receipt),
      authority: authority_projection(lower_receipt),
      workpad: %{refs: string_list(map_value(lower_receipt, :workpad_refs))},
      diagnostics: %{
        missing_required_evidence: missing_required_evidence,
        review_blocking?: missing_required_evidence != []
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

  defp token_dedupe_projection(lower_receipt) do
    source = map_value(lower_receipt, :token_dedupe) || %{}

    %{}
    |> maybe_put("accepted_count", integer_or_nil(map_value(source, :accepted_count)))
    |> maybe_put("duplicate_count", integer_or_nil(map_value(source, :duplicate_count)))
    |> maybe_put("token_hash_refs", string_list_or_nil(map_value(source, :token_hash_refs)))
  end

  defp rate_limit_projection(lower_receipt) do
    source = map_value(lower_receipt, :rate_limit) || %{}

    %{}
    |> maybe_put("remaining", integer_or_nil(map_value(source, :remaining)))
    |> maybe_put("reset_at", string_or_nil(map_value(source, :reset_at)))
    |> maybe_put("retry_after_ms", integer_or_nil(map_value(source, :retry_after_ms)))
    |> maybe_put("window", string_or_nil(map_value(source, :window)))
    |> maybe_put("source_event_ref", string_or_nil(map_value(source, :source_event_ref)))
  end

  defp retry_queue_projection(lower_receipt) do
    lower_receipt
    |> map_value(:retry)
    |> List.wrap()
    |> Enum.flat_map(&retry_projection/1)
  end

  defp retry_projection(%{} = retry) do
    [
      %{}
      |> maybe_put("retry_ref", string_or_nil(map_value(retry, :retry_ref)))
      |> maybe_put("attempt_ref", string_or_nil(map_value(retry, :attempt_ref)))
      |> maybe_put("due_at", string_or_nil(map_value(retry, :due_at)))
      |> maybe_put("reason", string_or_nil(map_value(retry, :reason)))
      |> maybe_put("last_error_ref", string_or_nil(map_value(retry, :last_error_ref)))
    ]
  end

  defp retry_projection(_retry), do: []

  defp aitrace_projection(lower_receipt) do
    source = map_value(lower_receipt, :aitrace) || %{}

    %{}
    |> maybe_put("evidence_receipt_ref", string_or_nil(map_value(source, :evidence_receipt_ref)))
    |> maybe_put("trace_artifact_ref", string_or_nil(map_value(source, :trace_artifact_ref)))
    |> maybe_put("export_bounds", export_bounds_projection(map_value(source, :export_bounds)))
  end

  defp export_bounds_projection(%{} = bounds) do
    %{}
    |> maybe_put("schema_version", string_or_nil(map_value(bounds, :schema_version)))
    |> maybe_put("redaction_policy_ref", string_or_nil(map_value(bounds, :redaction_policy_ref)))
    |> maybe_put("overflow_safe_action", string_or_nil(map_value(bounds, :overflow_safe_action)))
    |> maybe_put(
      "spillover_artifact_policy",
      string_or_nil(map_value(bounds, :spillover_artifact_policy))
    )
  end

  defp export_bounds_projection(_bounds), do: nil

  defp prompt_projection(lower_receipt) do
    source = map_value(lower_receipt, :prompt_provenance) || %{}

    %{}
    |> maybe_put("semantic_ref", string_or_nil(map_value(source, :semantic_ref)))
    |> maybe_put("prompt_hash", string_or_nil(map_value(source, :prompt_hash)))
    |> maybe_put("context_hash", string_or_nil(map_value(source, :context_hash)))
    |> maybe_put(
      "input_claim_check_ref",
      string_or_nil(map_value(source, :input_claim_check_ref))
    )
    |> maybe_put(
      "output_claim_check_ref",
      string_or_nil(map_value(source, :output_claim_check_ref))
    )
    |> maybe_put("provenance_refs", string_list_or_nil(map_value(source, :provenance_refs)))
    |> maybe_put("normalizer_version", string_or_nil(map_value(source, :normalizer_version)))
    |> maybe_put("redaction_policy_ref", string_or_nil(map_value(source, :redaction_policy_ref)))
  end

  defp semantic_projection(lower_receipt) do
    %{failure: semantic_failure_projection(map_value(lower_receipt, :semantic_failure))}
  end

  defp semantic_failure_projection(%{} = failure) do
    %{}
    |> maybe_put("failure_ref", string_or_nil(map_value(failure, :semantic_failure_ref)))
    |> maybe_put("kind", string_or_nil(map_value(failure, :kind)))
    |> maybe_put("retry_class", string_or_nil(map_value(failure, :retry_class)))
    |> maybe_put("journal_entry_ref", string_or_nil(map_value(failure, :journal_entry_ref)))
    |> maybe_put("context_hash", string_or_nil(map_value(failure, :context_hash)))
  end

  defp semantic_failure_projection(_failure), do: %{}

  defp authority_projection(lower_receipt) do
    provider_account = map_value(lower_receipt, :provider_account) || %{}
    credential = map_value(lower_receipt, :credential) || %{}

    %{}
    |> maybe_put(
      "provider_account_ref",
      string_or_nil(map_value(provider_account, :provider_account_ref))
    )
    |> maybe_put(
      "provider_account_redaction",
      string_or_nil(map_value(provider_account, :redaction))
    )
    |> maybe_put("credential_ref", string_or_nil(map_value(credential, :credential_ref)))
    |> maybe_put("credential_redaction", string_or_nil(map_value(credential, :redaction)))
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
        projection_name: @projection_name,
        missing_required_evidence: value(attrs, :missing_required_evidence) || []
      },
      occurred_at: DateTime.utc_now()
    })
  end

  defp evidence_specs(attrs) do
    refs_by_kind = refs_by_kind(attrs)

    attrs
    |> value(:required_evidence)
    |> List.wrap()
    |> Enum.map(&to_string/1)
    |> Enum.flat_map(&evidence_spec(&1, Map.get(refs_by_kind, &1)))
  end

  defp evidence_spec(kind, %{} = ref) do
    content_ref = value(ref, :content_ref)

    if valid_evidence_ref?(content_ref) do
      [
        %{
          kind: kind,
          content_ref: content_ref,
          collector_ref: value(ref, :collector_ref) || "receipt_reducer"
        }
      ]
    else
      []
    end
  end

  defp evidence_spec(_kind, _missing), do: []

  defp missing_required_evidence(attrs) do
    refs_by_kind = refs_by_kind(attrs)

    attrs
    |> value(:required_evidence)
    |> List.wrap()
    |> Enum.map(&to_string/1)
    |> Enum.reject(fn kind ->
      case Map.get(refs_by_kind, kind) do
        %{} = ref -> valid_evidence_ref?(value(ref, :content_ref))
        _missing -> false
      end
    end)
  end

  defp refs_by_kind(attrs) do
    refs =
      attrs
      |> value(:lower_receipt)
      |> case do
        %{} = receipt -> map_value(receipt, :artifact_refs) || []
        _other -> []
      end

    Map.new(refs, fn ref ->
      ref = normalize_attrs(ref)
      {required!(ref, :kind), ref}
    end)
  end

  defp valid_evidence_ref?(value) when is_binary(value),
    do: String.trim(value) != "" and not String.starts_with?(value, "artifact://")

  defp valid_evidence_ref?(_value), do: false

  defp effective_receipt_state(_attrs, receipt_state, []), do: receipt_state
  defp effective_receipt_state(_attrs, _receipt_state, _missing_evidence), do: "blocked"

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

  defp maybe_put(map, _key, nil), do: map
  defp maybe_put(map, key, value), do: Map.put(map, key, value)

  defp integer_or_nil(value) when is_integer(value), do: value
  defp integer_or_nil(_value), do: nil

  defp string_or_nil(nil), do: nil

  defp string_or_nil(value) when is_binary(value) do
    if String.trim(value) == "", do: nil, else: value
  end

  defp string_or_nil(value) when is_atom(value), do: Atom.to_string(value)
  defp string_or_nil(_value), do: nil

  defp string_list_or_nil(value) do
    values = string_list(value)
    if values == [], do: nil, else: values
  end

  defp string_list(value) when is_list(value) do
    value
    |> Enum.flat_map(fn
      item when is_binary(item) ->
        if String.trim(item) == "", do: [], else: [item]

      item when is_atom(item) ->
        [Atom.to_string(item)]

      _item ->
        []
    end)
  end

  defp string_list(_value), do: []

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
