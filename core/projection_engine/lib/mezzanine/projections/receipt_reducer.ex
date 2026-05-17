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
  alias Mezzanine.Projections.LowerReceiptSummary
  alias Mezzanine.Projections.ProjectionRow
  alias Mezzanine.Projections.SubjectRuntimeProjection
  alias Mezzanine.Substrate.OperationDisposition
  alias Mezzanine.Substrate.OperationGroupReceipt
  alias Mezzanine.Substrate.OperationReceipt

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
  @type generic_reduce_result :: %{
          reducer_module: module(),
          projection: SubjectRuntimeProjection.t(),
          lower_receipt_summary: LowerReceiptSummary.t(),
          operation_dispositions: [OperationDisposition.t()]
        }

  @spec reduce(
          [OperationReceipt.t()]
          | OperationReceipt.t()
          | OperationGroupReceipt.t()
          | map()
          | keyword()
        ) ::
          {:ok, generic_reduce_result() | reduce_result()} | {:error, term()}
  def reduce([%OperationReceipt{} | _rest] = receipts) do
    with {:ok, projection} <- SubjectRuntimeProjection.from_operation_receipts(receipts) do
      {:ok, generic_reduction(projection, receipts)}
    end
  end

  def reduce(%OperationReceipt{} = receipt), do: reduce([receipt])

  def reduce(%OperationGroupReceipt{} = group), do: reduce(group, [])

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

  @spec reduce(OperationGroupReceipt.t(), keyword()) ::
          {:ok, generic_reduce_result()} | {:error, term()}
  def reduce(%OperationGroupReceipt{} = group, opts) when is_list(opts) do
    receipts = Keyword.get(opts, :operation_receipts, [])

    with {:ok, projection} <- SubjectRuntimeProjection.from_operation_group(group, receipts) do
      {:ok, generic_reduction(projection, receipts)}
    end
  end

  defp generic_reduction(projection, receipts) do
    %{
      reducer_module: __MODULE__,
      projection: projection,
      lower_receipt_summary: LowerReceiptSummary.from_projection(projection),
      operation_dispositions: Enum.map(receipts, &operation_disposition/1)
    }
  end

  defp operation_disposition(%OperationReceipt{} = receipt) do
    {:ok, disposition} =
      OperationDisposition.new(%{
        disposition_ref: "operation-disposition://#{receipt.receipt_ref}",
        receipt_ref: receipt.receipt_ref,
        disposition: disposition(receipt.status),
        metadata: %{status: receipt.status}
      })

    disposition
  end

  defp disposition(status) when status in [:accepted, :completed, :succeeded], do: :accepted
  defp disposition(status) when status in [:retryable, :retryable_failure], do: :retryable_failure
  defp disposition(status) when status in [:canceled, :cancelled], do: :cancelled
  defp disposition(status) when status in [:blocked, :input_required], do: :blocked
  defp disposition(_status), do: :terminal_failure

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
      run: run_projection(lower_receipt),
      lower_receipt: lower_receipt_projection(attrs, lower_receipt, receipt_state),
      lower_envelope: lower_envelope_projection(lower_receipt),
      runtime: %{
        token_totals: map_value(lower_receipt, :token_totals) || %{},
        token_dedupe: token_dedupe_projection(lower_receipt),
        rate_limit: rate_limit_projection(lower_receipt),
        retry_queue: retry_queue_projection(lower_receipt),
        event_counts: event_counts(map_value(lower_receipt, :runtime_events) || [])
      },
      governance: governance_projection(lower_receipt),
      review: %{
        pending_decision_ids: Enum.map(decisions, & &1.id)
      },
      evidence: %{
        evidence_refs: Enum.map(evidence, &evidence_projection/1),
        aitrace: aitrace_projection(lower_receipt)
      },
      prompt: prompt_projection(lower_receipt),
      memory_context: memory_context_projection(lower_receipt),
      semantic: semantic_projection(lower_receipt),
      authority: authority_projection(lower_receipt),
      workpad: %{refs: string_list(map_value(lower_receipt, :workpad_refs))},
      source_publication: source_publication_projection(lower_receipt),
      source_bindings: source_binding_projections(subject, lower_receipt),
      incident_bundles: incident_bundle_projections(lower_receipt),
      retry_receipts: retry_receipt_projections(lower_receipt),
      acceptance: acceptance_projection(lower_receipt),
      github_pr: github_pr_projection(lower_receipt),
      diagnostics: %{
        missing_required_evidence: missing_required_evidence,
        review_blocking?: missing_required_evidence != []
      }
    }
  end

  defp run_projection(lower_receipt) do
    runtime_profile = map_value(lower_receipt, :runtime_profile) || %{}

    %{}
    |> maybe_put(
      "run_ref",
      string_or_nil(map_value(lower_receipt, :run_ref) || map_value(lower_receipt, :run_id))
    )
    |> maybe_put(
      "attempt_ref",
      string_or_nil(
        map_value(lower_receipt, :attempt_ref) || map_value(lower_receipt, :attempt_id)
      )
    )
    |> maybe_put(
      "runtime_profile_ref",
      string_or_nil(map_value(runtime_profile, :runtime_profile_ref))
    )
    |> maybe_put(
      "runtime_profile_kind",
      string_or_nil(map_value(runtime_profile, :runtime_profile_kind))
    )
  end

  defp lower_receipt_projection(attrs, lower_receipt, receipt_state) do
    %{
      receipt_id: required!(attrs, :receipt_id),
      receipt_state: receipt_state,
      lower_receipt_ref: value(attrs, :lower_receipt_ref),
      run_id: map_value(lower_receipt, :run_id),
      attempt_id: map_value(lower_receipt, :attempt_id),
      metadata: lower_receipt_metadata(lower_receipt)
    }
  end

  defp lower_receipt_metadata(lower_receipt), do: lower_envelope_projection(lower_receipt)

  defp lower_envelope_projection(lower_receipt) do
    envelope =
      map_value(lower_receipt, :governed_lower_envelope) ||
        map_value(lower_receipt, :lower_envelope) ||
        %{}

    %{}
    |> maybe_put("lower_request_ref", string_or_nil(map_value(envelope, :lower_request_ref)))
    |> maybe_put("lower_runtime_kind", string_or_nil(map_value(envelope, :lower_runtime_kind)))
    |> maybe_put("capability_id", string_or_nil(map_value(envelope, :capability_id)))
    |> maybe_put(
      "resource_scope_refs",
      string_list_or_nil(map_value(envelope, :resource_scope_refs))
    )
    |> maybe_put(
      "policy_bundle_refs",
      string_list_or_nil(map_value(envelope, :policy_bundle_refs))
    )
    |> maybe_put("script_refs", string_list_or_nil(map_value(envelope, :script_refs)))
    |> maybe_put("package_refs", string_list_or_nil(map_value(envelope, :package_refs)))
    |> maybe_put("sandbox_profile_ref", string_or_nil(map_value(envelope, :sandbox_profile_ref)))
    |> maybe_put(
      "attestation_requirement_ref",
      string_or_nil(map_value(envelope, :attestation_requirement_ref))
    )
    |> maybe_put("denial_refs", string_list_or_nil(map_value(envelope, :denial_refs)))
  end

  defp governance_projection(lower_receipt) do
    runtime_profile = map_value(lower_receipt, :runtime_profile) || %{}
    authority = map_value(lower_receipt, :authority_decision) || %{}

    %{}
    |> maybe_put(
      "runtime_profile_ref",
      string_or_nil(map_value(runtime_profile, :runtime_profile_ref))
    )
    |> maybe_put(
      "runtime_profile_kind",
      string_or_nil(map_value(runtime_profile, :runtime_profile_kind))
    )
    |> maybe_put("authority_ref", string_or_nil(map_value(authority, :authority_ref)))
    |> maybe_put(
      "authority_decision_hash",
      string_or_nil(map_value(authority, :authority_decision_hash))
    )
    |> maybe_put(
      "connector_manifest_refs",
      lower_receipt
      |> map_value(:connector_manifests)
      |> List.wrap()
      |> Enum.flat_map(&manifest_ref/1)
    )
    |> maybe_put(
      "capability_negotiation_refs",
      lower_receipt
      |> map_value(:capability_negotiations)
      |> List.wrap()
      |> Enum.flat_map(&capability_negotiation_ref/1)
    )
    |> compact_projection()
  end

  defp manifest_ref(%{} = manifest),
    do: manifest |> map_value(:connector_manifest_ref) |> string_or_nil() |> List.wrap()

  defp manifest_ref(value), do: value |> string_or_nil() |> List.wrap()

  defp capability_negotiation_ref(%{} = negotiation),
    do: negotiation |> map_value(:capability_negotiation_ref) |> string_or_nil() |> List.wrap()

  defp capability_negotiation_ref(value), do: value |> string_or_nil() |> List.wrap()

  defp incident_bundle_projections(lower_receipt) do
    lower_receipt
    |> map_value(:incident_bundles)
    |> List.wrap()
    |> Enum.flat_map(&incident_bundle_projection/1)
  end

  defp incident_bundle_projection(%{} = bundle) do
    [
      %{}
      |> maybe_put("incident_ref", string_or_nil(map_value(bundle, :incident_ref)))
      |> maybe_put("incident_class", string_or_nil(map_value(bundle, :incident_class)))
      |> maybe_put("run_ref", string_or_nil(map_value(bundle, :run_ref)))
      |> maybe_put("subject_ref", string_or_nil(map_value(bundle, :subject_ref)))
      |> maybe_put("runtime_profile_ref", string_or_nil(map_value(bundle, :runtime_profile_ref)))
      |> maybe_put("authority_ref", string_or_nil(map_value(bundle, :authority_ref)))
      |> maybe_put(
        "connector_manifest_ref",
        string_or_nil(map_value(bundle, :connector_manifest_ref))
      )
      |> maybe_put("lower_attempt_ref", string_or_nil(map_value(bundle, :lower_attempt_ref)))
      |> maybe_put("retry_receipt_ref", string_or_nil(map_value(bundle, :retry_receipt_ref)))
      |> maybe_put(
        "terminal_receipt_ref",
        string_or_nil(map_value(bundle, :terminal_receipt_ref))
      )
      |> maybe_put(
        "redaction_manifest_ref",
        string_or_nil(map_value(bundle, :redaction_manifest_ref))
      )
      |> maybe_put(
        "operator_message_ref",
        string_or_nil(map_value(bundle, :operator_message_ref))
      )
      |> compact_projection()
    ]
  end

  defp incident_bundle_projection(_bundle), do: []

  defp retry_receipt_projections(lower_receipt) do
    lower_receipt
    |> map_value(:retry_receipts)
    |> List.wrap()
    |> Enum.flat_map(&retry_receipt_projection/1)
  end

  defp retry_receipt_projection(%{} = receipt) do
    [
      %{}
      |> maybe_put("retry_receipt_ref", string_or_nil(map_value(receipt, :retry_receipt_ref)))
      |> maybe_put("prior_attempt_ref", string_or_nil(map_value(receipt, :prior_attempt_ref)))
      |> maybe_put("failure_class", string_or_nil(map_value(receipt, :failure_class)))
      |> maybe_put("retry_safety_class", string_or_nil(map_value(receipt, :retry_safety_class)))
      |> maybe_put("policy_hash_before", string_or_nil(map_value(receipt, :policy_hash_before)))
      |> maybe_put("policy_hash_after", string_or_nil(map_value(receipt, :policy_hash_after)))
      |> maybe_put(
        "manifest_hash_before",
        string_or_nil(map_value(receipt, :manifest_hash_before))
      )
      |> maybe_put("manifest_hash_after", string_or_nil(map_value(receipt, :manifest_hash_after)))
      |> maybe_put("next_attempt_ref", string_or_nil(map_value(receipt, :next_attempt_ref)))
      |> maybe_put("terminal_denial_ref", string_or_nil(map_value(receipt, :terminal_denial_ref)))
      |> compact_projection()
    ]
  end

  defp retry_receipt_projection(_receipt), do: []

  defp acceptance_projection(lower_receipt) do
    acceptance = map_value(lower_receipt, :acceptance) || %{}

    %{}
    |> maybe_put("scenario_refs", string_list_or_nil(map_value(acceptance, :scenario_refs)))
    |> maybe_put("claim_refs", string_list_or_nil(map_value(acceptance, :claim_refs)))
  end

  defp github_pr_projection(lower_receipt) do
    source =
      map_value(lower_receipt, :github_pr_evidence) ||
        map_value(lower_receipt, :github_pr) ||
        %{}

    %{}
    |> maybe_put("provider", string_or_nil(map_value(source, :provider)))
    |> maybe_put("evidence_ref", string_or_nil(map_value(source, :evidence_ref)))
    |> maybe_put("content_ref", string_or_nil(map_value(source, :content_ref)))
    |> maybe_put("feedback", map_value(source, :feedback))
    |> compact_projection()
  end

  defp source_binding_projections(subject, lower_receipt) do
    [
      %{
        binding_ref: Map.get(subject, :source_binding_id),
        source_ref: Map.get(subject, :source_ref),
        source_kind: Map.get(subject, :subject_kind) || Map.get(subject, :provider),
        external_system: Map.get(subject, :provider),
        source_state: Map.get(subject, :source_state),
        source_url: Map.get(subject, :source_url),
        workpad_refs: string_list(map_value(lower_receipt, :workpad_refs)),
        metadata: source_publication_metadata(lower_receipt)
      }
      |> compact_projection()
    ]
  end

  defp source_publication_projection(lower_receipt) do
    source = map_value(lower_receipt, :source_publication) || %{}

    %{}
    |> maybe_put(
      "source_publication_receipt_ref",
      string_or_nil(map_value(source, :source_publication_receipt_ref))
    )
    |> maybe_put("source_publish_ref", string_or_nil(map_value(source, :source_publish_ref)))
    |> maybe_put("status", string_or_nil(map_value(source, :status)))
    |> maybe_put("capability_id", string_or_nil(map_value(source, :capability_id)))
    |> maybe_put("lower_runtime_kind", string_or_nil(map_value(source, :lower_runtime_kind)))
    |> maybe_put("lower_request_ref", string_or_nil(map_value(source, :lower_request_ref)))
    |> maybe_put("lower_receipt_ref", string_or_nil(map_value(source, :lower_receipt_ref)))
    |> maybe_put("authority_ref", string_or_nil(map_value(source, :authority_ref)))
    |> maybe_put(
      "authority_decision_hash",
      string_or_nil(map_value(source, :authority_decision_hash))
    )
    |> maybe_put(
      "connector_manifest_ref",
      string_or_nil(map_value(source, :connector_manifest_ref))
    )
    |> maybe_put(
      "capability_negotiation_ref",
      string_or_nil(map_value(source, :capability_negotiation_ref))
    )
    |> maybe_put(
      "provider_response_ref",
      string_or_nil(map_value(source, :provider_response_ref))
    )
    |> maybe_put(
      "redaction_manifest_ref",
      string_or_nil(map_value(source, :redaction_manifest_ref))
    )
    |> maybe_put("workpad_refs", string_list_or_nil(map_value(source, :workpad_refs)))
    |> maybe_put("comment_ref", string_or_nil(map_value(source, :comment_ref)))
    |> maybe_put("trace_id", string_or_nil(map_value(source, :trace_id)))
  end

  defp source_publication_metadata(lower_receipt) do
    lower_receipt
    |> source_publication_projection()
    |> Map.take([
      "source_publication_receipt_ref",
      "source_publish_ref",
      "status",
      "capability_id",
      "lower_runtime_kind",
      "lower_request_ref",
      "lower_receipt_ref",
      "authority_ref",
      "authority_decision_hash",
      "connector_manifest_ref",
      "capability_negotiation_ref",
      "provider_response_ref",
      "redaction_manifest_ref",
      "comment_ref",
      "trace_id"
    ])
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

  defp memory_context_projection(lower_receipt) do
    source = map_value(lower_receipt, :memory_context) || %{}

    %{}
    |> maybe_put("memory_profile_ref", string_or_nil(map_value(source, :memory_profile_ref)))
    |> maybe_put("context_pack_ref", string_or_nil(map_value(source, :context_pack_ref)))
    |> maybe_put("context_hash", string_or_nil(map_value(source, :context_hash)))
    |> maybe_put("fragment_refs", string_list_or_nil(map_value(source, :fragment_refs)))
    |> maybe_put("memory_query_ref", string_or_nil(map_value(source, :memory_query_ref)))
    |> maybe_put(
      "memory_evidence_refs",
      string_list_or_nil(map_value(source, :memory_evidence_refs))
    )
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

  defp compact_projection(map) do
    map
    |> Enum.reject(fn {_key, value} -> value in [nil, "", [], %{}] end)
    |> Map.new()
  end

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
