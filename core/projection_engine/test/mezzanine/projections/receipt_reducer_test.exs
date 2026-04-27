defmodule Mezzanine.Projections.ReceiptReducerTest do
  use Mezzanine.Projections.DataCase, async: false

  alias Mezzanine.Audit.AuditFact
  alias Mezzanine.DecisionCommands
  alias Mezzanine.EvidenceLedger.EvidenceRecord
  alias Mezzanine.Execution.ExecutionRecord
  alias Mezzanine.Objects.SubjectRecord
  alias Mezzanine.Projections.{ProjectionRow, ReceiptReducer}

  @required_evidence [
    "github_pr",
    "diff",
    "commit",
    "ci",
    "codex_session",
    "source_workpad",
    "run_log",
    "source_comment",
    "connector_event"
  ]

  test "reduces terminal success into execution, subject, review, evidence, projection, and audit facts" do
    %{subject: subject, execution: execution} = receipt_fixture("success")

    assert {:ok, reduced} =
             ReceiptReducer.reduce(success_attrs(subject, execution, review_required?: true))

    assert reduced.execution.dispatch_state == :completed
    assert reduced.subject.lifecycle_state == "awaiting_review"

    assert [%{decision_kind: "operator_review_required", lifecycle_state: "pending"}] =
             reduced.decisions

    assert Enum.sort(Enum.map(reduced.evidence, & &1.evidence_kind)) ==
             Enum.sort(@required_evidence)

    assert {:ok, projection} =
             ProjectionRow.row_by_key("inst-1", "operator_subject_runtime", subject.id)

    assert projection.payload["subject"]["lifecycle_state"] == "awaiting_review"
    assert projection.payload["execution"]["dispatch_state"] == "completed"
    assert projection.payload["lower_receipt"]["lower_receipt_ref"] == "lower-receipt://completed"
    assert projection.payload["runtime"]["token_totals"] == %{"input" => 120, "output" => 45}

    assert projection.payload["runtime"]["rate_limit"] == %{
             "remaining" => 80,
             "reset_at" => "later"
           }

    assert projection.payload["runtime"]["event_counts"]["tool_call"] == 2
    assert projection.payload["diagnostics"]["missing_required_evidence"] == []
    assert projection.payload["diagnostics"]["review_blocking?"] == false

    assert {:ok, audit_facts} = AuditFact.list_trace("inst-1", "trace-receipt-completed")
    audit_fact = Enum.find(audit_facts, &(&1.fact_kind == :receipt_reduced))
    assert audit_fact.payload["receipt_id"] == "receipt-completed"
  end

  test "reduces every terminal lower outcome into stable execution and subject states" do
    cases = [
      {"failed", :failed, "failed", nil},
      {"approval_required", :failed, "failed", nil},
      {"input_required", :failed, "blocked", "input_required"},
      {"cancelled", :cancelled, "cancelled", nil}
    ]

    for {receipt_state, expected_dispatch_state, expected_lifecycle, expected_block} <- cases do
      %{subject: subject, execution: execution} = receipt_fixture(receipt_state)

      assert {:ok, reduced} =
               ReceiptReducer.reduce(
                 success_attrs(subject, execution,
                   receipt_state: receipt_state,
                   review_required?: false
                 )
               )

      assert reduced.execution.dispatch_state == expected_dispatch_state
      assert reduced.subject.lifecycle_state == expected_lifecycle

      if expected_block do
        assert reduced.subject.block_reason == expected_block
      else
        assert is_nil(reduced.subject.block_reason)
      end
    end
  end

  test "duplicate lower receipt replay is idempotent for decisions evidence and projection rows" do
    %{subject: subject, execution: execution} = receipt_fixture("duplicate")
    attrs = success_attrs(subject, execution, review_required?: true)

    assert {:ok, first} = ReceiptReducer.reduce(attrs)
    assert {:ok, second} = ReceiptReducer.reduce(attrs)

    assert [first_decision] = first.decisions
    assert [second_decision] = second.decisions
    assert first_decision.id == second_decision.id

    assert {:ok, evidence_rows} = EvidenceRecord.for_subject_execution(subject.id, execution.id)
    assert Enum.sort(Enum.map(evidence_rows, & &1.evidence_kind)) == Enum.sort(@required_evidence)

    assert {:ok, decision} =
             DecisionCommands.fetch_by_identity(%{
               installation_id: "inst-1",
               subject_id: subject.id,
               execution_id: execution.id,
               decision_kind: "operator_review_required"
             })

    assert decision.id == first_decision.id

    assert {:ok, projection} =
             ProjectionRow.row_by_key("inst-1", "operator_subject_runtime", subject.id)

    assert projection.payload["lower_receipt"]["receipt_id"] == "receipt-completed"
  end

  test "missing required evidence blocks review without crashing receipt reduction" do
    %{subject: subject, execution: execution} = receipt_fixture("missing-required-evidence")

    attrs =
      success_attrs(subject, execution,
        review_required?: true,
        required_evidence: ["github_pr", "codex_session", "source_workpad"],
        evidence_refs: [
          %{kind: "github_pr", content_ref: "lower-artifact://github-pr/1", collector_ref: "github"}
        ]
      )

    assert {:ok, reduced} = ReceiptReducer.reduce(attrs)

    assert reduced.execution.dispatch_state == :failed
    assert reduced.subject.lifecycle_state == "blocked"
    assert reduced.subject.block_reason == "missing_required_evidence"
    assert Enum.map(reduced.evidence, & &1.evidence_kind) == ["github_pr"]

    assert {:ok, projection} =
             ProjectionRow.row_by_key("inst-1", "operator_subject_runtime", subject.id)

    assert projection.payload["diagnostics"]["missing_required_evidence"] == [
             "codex_session",
             "source_workpad"
           ]

    assert projection.payload["diagnostics"]["review_blocking?"] == true
    assert projection.payload["lower_receipt"]["lower_receipt_ref"] == "lower-receipt://completed"
  end

  test "placeholder artifact refs cannot satisfy required review evidence" do
    %{subject: subject, execution: execution} = receipt_fixture("placeholder-required-evidence")

    attrs =
      success_attrs(subject, execution,
        review_required?: true,
        required_evidence: ["github_pr"],
        evidence_refs: [
          %{kind: "github_pr", content_ref: "artifact://github_pr", collector_ref: "fixture"}
        ]
      )

    assert {:ok, reduced} = ReceiptReducer.reduce(attrs)

    assert reduced.evidence == []
    assert reduced.subject.lifecycle_state == "blocked"
    assert reduced.subject.block_reason == "missing_required_evidence"

    assert {:ok, projection} =
             ProjectionRow.row_by_key("inst-1", "operator_subject_runtime", subject.id)

    assert projection.payload["diagnostics"]["missing_required_evidence"] == ["github_pr"]
  end

  defp receipt_fixture(suffix) do
    source_ref = "linear:ticket:receipt-reducer-#{suffix}-#{System.unique_integer([:positive])}"

    {:ok, subject} =
      SubjectRecord.ingest(%{
        installation_id: "inst-1",
        source_ref: source_ref,
        source_event_id: "source-event-#{suffix}",
        source_binding_id: "source-binding-1",
        provider: "linear",
        provider_external_ref: "LIN-#{System.unique_integer([:positive])}",
        provider_revision: "1",
        source_state: "In Progress",
        state_mapping: %{"In Progress" => "submitted"},
        subject_kind: "linear_coding_ticket",
        lifecycle_state: "running",
        status: "active",
        title: "Receipt reducer #{suffix}",
        schema_ref: "mezzanine.subject.linear_coding_ticket.payload.v1",
        schema_version: 1,
        payload: %{},
        trace_id: "trace-subject-#{suffix}",
        causation_id: "cause-subject-#{suffix}",
        actor_ref: %{kind: :source}
      })

    {:ok, execution} =
      ExecutionRecord.dispatch(%{
        tenant_id: "tenant-1",
        installation_id: "inst-1",
        subject_id: subject.id,
        recipe_ref: "coding_ops",
        dispatch_envelope: %{"capability" => "codex.session.turn"},
        submission_dedupe_key: "inst-1:receipt-reducer:#{suffix}",
        trace_id: "trace-execution-#{suffix}",
        causation_id: "cause-execution-#{suffix}",
        actor_ref: %{kind: :scheduler}
      })

    %{subject: subject, execution: execution}
  end

  defp success_attrs(subject, execution, opts) do
    receipt_state = Keyword.get(opts, :receipt_state, "completed")
    review_required? = Keyword.get(opts, :review_required?, false)
    required_evidence = Keyword.get(opts, :required_evidence, @required_evidence)
    evidence_refs = Keyword.get(opts, :evidence_refs, evidence_refs(required_evidence))

    %{
      installation_id: "inst-1",
      subject_id: subject.id,
      execution_id: execution.id,
      receipt_id: "receipt-#{String.replace(receipt_state, "_", "-")}",
      lower_receipt_ref: "lower-receipt://#{String.replace(receipt_state, "_", "-")}",
      receipt_state: receipt_state,
      lower_receipt: %{
        "run_id" => "lower-run-#{receipt_state}",
        "attempt_id" => "lower-attempt-#{receipt_state}",
        "artifact_refs" => evidence_refs,
        "token_totals" => %{"input" => 120, "output" => 45},
        "rate_limit" => %{"remaining" => 80, "reset_at" => "later"},
        "runtime_events" => [
          %{"event_kind" => "tool_call"},
          %{"event_kind" => "tool_call"},
          %{"event_kind" => "assistant_message"}
        ]
      },
      normalized_outcome: %{"state" => receipt_state, "terminal" => true},
      artifact_refs: Enum.map(evidence_refs, & &1.content_ref),
      required_evidence: required_evidence,
      review_required?: review_required?,
      trace_id: "trace-receipt-#{receipt_state}",
      causation_id: "cause-receipt-#{receipt_state}",
      actor_ref: %{kind: :workflow, id: "execution_attempt"}
    }
  end

  defp evidence_refs(kinds) do
    Enum.map(kinds, fn kind ->
      %{kind: kind, content_ref: "lower-artifact://#{kind}/receipt", collector_ref: "receipt-reducer"}
    end)
  end
end
