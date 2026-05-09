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
             "retry_after_ms" => 120_000,
             "reset_at" => "later"
           }

    assert projection.payload["runtime"]["token_dedupe"] == %{
             "accepted_count" => 2,
             "duplicate_count" => 1,
             "token_hash_refs" => ["sha256:token-1", "sha256:token-2"]
           }

    assert projection.payload["runtime"]["retry_queue"] == [
             %{
               "due_at" => "2026-05-01T12:00:00Z",
               "reason" => "rate_limited",
               "retry_ref" => "retry://receipt/completed"
             }
           ]

    assert projection.payload["runtime"]["event_counts"]["tool_call"] == 2

    assert projection.payload["evidence"]["aitrace"]["evidence_receipt_ref"] ==
             "aitrace://receipt/1"

    assert projection.payload["evidence"]["aitrace"]["export_bounds"] == %{
             "overflow_safe_action" => "spill_to_artifact_ref",
             "redaction_policy_ref" => "aitrace.export_bounds.redact_hash.v1",
             "schema_version" => "aitrace.export_bounds.v1"
           }

    assert projection.payload["prompt"] == %{
             "context_hash" => "sha256:context",
             "input_claim_check_ref" => "claim://prompt/input",
             "output_claim_check_ref" => "claim://prompt/output",
             "prompt_hash" => "sha256:prompt",
             "provenance_refs" => ["outer-brain://provenance/1"],
             "redaction_policy_ref" => "redaction://prompt",
             "semantic_ref" => "semantic://context/1"
           }

    assert projection.payload["semantic"]["failure"] == %{
             "failure_ref" => "semantic-failure://phase12/1",
             "kind" => "semantic_insufficient_context",
             "retry_class" => "repairable"
           }

    assert projection.payload["authority"] == %{
             "credential_ref" => "credential://lease/redacted",
             "credential_redaction" => "ref_only",
             "provider_account_redaction" => "ref_only",
             "provider_account_ref" => "provider-account://codex/redacted"
           }

    assert projection.payload["workpad"]["refs"] == ["source-workpad://linear/tenant-1/subj-1"]

    assert projection.payload["run"] == %{
             "attempt_ref" => "lower-attempt-completed",
             "run_ref" => "lower-run-completed",
             "runtime_profile_kind" => "codex_session",
             "runtime_profile_ref" => "runtime-profile://codex/default"
           }

    assert projection.payload["lower_receipt"]["metadata"] == %{
             "attestation_requirement_ref" => "attestation://local/default",
             "capability_id" => "codex.session.turn",
             "denial_refs" => ["lower-denial://capability/linear"],
             "lower_request_ref" => "lower-request://codex/session-turn",
             "lower_runtime_kind" => "codex_session",
             "package_refs" => ["package://extravaganza/coding_ops"],
             "policy_bundle_refs" => ["policy-bundle://extravaganza/default"],
             "resource_scope_refs" => ["scope://subject/subj-1"],
             "sandbox_profile_ref" => "sandbox://local/read-write",
             "script_refs" => ["script://codex/session-turn"]
           }

    assert projection.payload["lower_envelope"] == %{
             "attestation_requirement_ref" => "attestation://local/default",
             "capability_id" => "codex.session.turn",
             "denial_refs" => ["lower-denial://capability/linear"],
             "lower_request_ref" => "lower-request://codex/session-turn",
             "lower_runtime_kind" => "codex_session",
             "package_refs" => ["package://extravaganza/coding_ops"],
             "policy_bundle_refs" => ["policy-bundle://extravaganza/default"],
             "resource_scope_refs" => ["scope://subject/subj-1"],
             "sandbox_profile_ref" => "sandbox://local/read-write",
             "script_refs" => ["script://codex/session-turn"]
           }

    assert projection.payload["governance"] == %{
             "authority_decision_hash" => String.duplicate("a", 64),
             "authority_ref" => "authority-decision://codex/session-turn",
             "capability_negotiation_refs" => ["cap-neg://codex/session-turn"],
             "connector_manifest_refs" => ["manifest://jido/connectors/codex_cli@local"],
             "runtime_profile_kind" => "codex_session",
             "runtime_profile_ref" => "runtime-profile://codex/default"
           }

    assert Enum.map(projection.payload["incident_bundles"], & &1["incident_class"]) ==
             [
               "policy_denied",
               "capability_denied",
               "manifest_stale",
               "lower_runtime_failed",
               "receipt_missing",
               "projection_missing",
               "source_publication_failed"
             ]

    assert Enum.map(projection.payload["retry_receipts"], & &1["retry_safety_class"]) ==
             ["safe", "approval_required", "terminal"]

    assert projection.payload["acceptance"] == %{
             "scenario_refs" => ["stacklab://scenario/local-single-node"],
             "claim_refs" => ["claim://extravaganza/local-run"]
           }

    assert projection.payload["github_pr"] == %{
             "content_ref" => "github-pr://nshkrdotcom/extravaganza/42",
             "evidence_ref" => "evidence://github-pr/nshkrdotcom/extravaganza/42",
             "feedback" => %{"rework_required?" => true},
             "provider" => "github"
           }

    assert projection.payload["source_publication"] == %{
             "authority_decision_hash" => String.duplicate("b", 64),
             "authority_ref" => "authority-decision://linear/comment",
             "capability_id" => "linear.comments.update",
             "capability_negotiation_ref" => "cap-neg://linear/comment",
             "comment_ref" => "linear-comment://comment-linear-workpad",
             "connector_manifest_ref" => "manifest://jido/connectors/linear@local",
             "lower_receipt_ref" => "lower-receipt://source/comment/succeeded",
             "lower_request_ref" => "lower-request://source/comment",
             "lower_runtime_kind" => "direct_connector",
             "provider_response_ref" => "artifact://linear/comment-update",
             "redaction_manifest_ref" => "redaction://linear/workpad",
             "source_publication_receipt_ref" => "source-publication://linear-primary/receipt",
             "source_publish_ref" => "linear_workpad_review",
             "status" => "published",
             "trace_id" => "trace-linear-publication",
             "workpad_refs" => ["source-workpad://linear/tenant-1/subj-1"]
           }

    assert [
             %{
               "binding_ref" => "source-binding-1",
               "external_system" => "linear",
               "metadata" => %{
                 "source_publication_receipt_ref" =>
                   "source-publication://linear-primary/receipt",
                 "capability_id" => "linear.comments.update",
                 "provider_response_ref" => "artifact://linear/comment-update"
               },
               "source_ref" => source_ref,
               "workpad_refs" => ["source-workpad://linear/tenant-1/subj-1"]
             }
           ] = projection.payload["source_bindings"]

    assert String.starts_with?(source_ref, "linear:")
    assert projection.payload["diagnostics"]["missing_required_evidence"] == []
    assert projection.payload["diagnostics"]["review_blocking?"] == false
    refute String.contains?(inspect(projection.payload), "should-drop")
    refute String.contains?(inspect(projection.payload), "never-project-this-token")

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
          %{
            kind: "github_pr",
            content_ref: "lower-artifact://github-pr/1",
            collector_ref: "github"
          }
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
        "token_dedupe" => %{
          "accepted_count" => 2,
          "duplicate_count" => 1,
          "token_hash_refs" => ["sha256:token-1", "sha256:token-2"],
          "token_text" => "never-project-this-token"
        },
        "rate_limit" => %{"remaining" => 80, "reset_at" => "later", "retry_after_ms" => 120_000},
        "retry" => %{
          "retry_ref" => "retry://receipt/completed",
          "due_at" => "2026-05-01T12:00:00Z",
          "reason" => "rate_limited"
        },
        "aitrace" => %{
          "evidence_receipt_ref" => "aitrace://receipt/1",
          "trace_artifact_ref" => "aitrace://artifact/1",
          "export_bounds" => %{
            "schema_version" => "aitrace.export_bounds.v1",
            "redaction_policy_ref" => "aitrace.export_bounds.redact_hash.v1",
            "overflow_safe_action" => "spill_to_artifact_ref"
          },
          "raw_provider_payload" => "should-drop"
        },
        "prompt_provenance" => %{
          "semantic_ref" => "semantic://context/1",
          "prompt_hash" => "sha256:prompt",
          "context_hash" => "sha256:context",
          "input_claim_check_ref" => "claim://prompt/input",
          "output_claim_check_ref" => "claim://prompt/output",
          "provenance_refs" => ["outer-brain://provenance/1"],
          "redaction_policy_ref" => "redaction://prompt",
          "raw_prompt" => "should-drop"
        },
        "semantic_failure" => %{
          "semantic_failure_ref" => "semantic-failure://phase12/1",
          "kind" => "semantic_insufficient_context",
          "retry_class" => "repairable",
          "provider_ref" => %{"raw_provider_payload" => "should-drop"}
        },
        "provider_account" => %{
          "provider_account_ref" => "provider-account://codex/redacted",
          "provider_account_id" => "should-drop",
          "redaction" => "ref_only"
        },
        "credential" => %{
          "credential_ref" => "credential://lease/redacted",
          "api_key" => "should-drop",
          "redaction" => "ref_only"
        },
        "runtime_profile" => %{
          "runtime_profile_ref" => "runtime-profile://codex/default",
          "runtime_profile_kind" => "codex_session"
        },
        "governed_lower_envelope" => %{
          "lower_request_ref" => "lower-request://codex/session-turn",
          "lower_runtime_kind" => "codex_session",
          "capability_id" => "codex.session.turn",
          "resource_scope_refs" => ["scope://subject/subj-1"],
          "policy_bundle_refs" => ["policy-bundle://extravaganza/default"],
          "script_refs" => ["script://codex/session-turn"],
          "package_refs" => ["package://extravaganza/coding_ops"],
          "sandbox_profile_ref" => "sandbox://local/read-write",
          "attestation_requirement_ref" => "attestation://local/default",
          "denial_refs" => ["lower-denial://capability/linear"]
        },
        "authority_decision" => %{
          "authority_ref" => "authority-decision://codex/session-turn",
          "authority_decision_hash" => String.duplicate("a", 64)
        },
        "connector_manifests" => [
          %{"connector_manifest_ref" => "manifest://jido/connectors/codex_cli@local"}
        ],
        "capability_negotiations" => [
          %{"capability_negotiation_ref" => "cap-neg://codex/session-turn"}
        ],
        "retry_receipts" => retry_receipts(),
        "incident_bundles" => incident_bundles(),
        "acceptance" => %{
          "scenario_refs" => ["stacklab://scenario/local-single-node"],
          "claim_refs" => ["claim://extravaganza/local-run"]
        },
        "github_pr_evidence" => %{
          "provider" => "github",
          "evidence_ref" => "evidence://github-pr/nshkrdotcom/extravaganza/42",
          "content_ref" => "github-pr://nshkrdotcom/extravaganza/42",
          "feedback" => %{"rework_required?" => true}
        },
        "source_publication" => %{
          "source_publication_receipt_ref" => "source-publication://linear-primary/receipt",
          "source_publish_ref" => "linear_workpad_review",
          "status" => "published",
          "capability_id" => "linear.comments.update",
          "lower_runtime_kind" => "direct_connector",
          "lower_request_ref" => "lower-request://source/comment",
          "lower_receipt_ref" => "lower-receipt://source/comment/succeeded",
          "authority_ref" => "authority-decision://linear/comment",
          "authority_decision_hash" => String.duplicate("b", 64),
          "connector_manifest_ref" => "manifest://jido/connectors/linear@local",
          "capability_negotiation_ref" => "cap-neg://linear/comment",
          "provider_response_ref" => "artifact://linear/comment-update",
          "redaction_manifest_ref" => "redaction://linear/workpad",
          "workpad_refs" => ["source-workpad://linear/tenant-1/subj-1"],
          "comment_ref" => "linear-comment://comment-linear-workpad",
          "trace_id" => "trace-linear-publication",
          "raw_provider_payload" => "should-drop"
        },
        "workpad_refs" => ["source-workpad://linear/tenant-1/subj-1"],
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

  defp retry_receipts do
    [
      %{
        "retry_receipt_ref" => "retry-receipt://safe",
        "prior_attempt_ref" => "lower-attempt-completed",
        "failure_class" => "provider_unavailable",
        "retry_safety_class" => "safe",
        "policy_hash_before" => "sha256:policy-before",
        "policy_hash_after" => "sha256:policy-before",
        "manifest_hash_before" => "sha256:manifest-before",
        "manifest_hash_after" => "sha256:manifest-before",
        "next_attempt_ref" => "lower-attempt-retry"
      },
      %{
        "retry_receipt_ref" => "retry-receipt://approval-required",
        "prior_attempt_ref" => "lower-attempt-completed",
        "failure_class" => "manifest_stale",
        "retry_safety_class" => "approval_required",
        "terminal_denial_ref" => nil
      },
      %{
        "retry_receipt_ref" => "retry-receipt://terminal",
        "prior_attempt_ref" => "lower-attempt-completed",
        "failure_class" => "policy_drift",
        "retry_safety_class" => "terminal",
        "terminal_denial_ref" => "terminal-denial://policy-drift"
      }
    ]
  end

  defp incident_bundles do
    [
      "policy_denied",
      "capability_denied",
      "manifest_stale",
      "lower_runtime_failed",
      "receipt_missing",
      "projection_missing",
      "source_publication_failed"
    ]
    |> Enum.map(fn incident_class ->
      %{
        "incident_ref" => "incident://#{incident_class}",
        "incident_class" => incident_class,
        "run_ref" => "lower-run-completed",
        "subject_ref" => "subj-1",
        "runtime_profile_ref" => "runtime-profile://codex/default",
        "authority_ref" => "authority-decision://codex/session-turn",
        "connector_manifest_ref" => "manifest://jido/connectors/codex_cli@local",
        "lower_attempt_ref" => "lower-attempt-completed",
        "retry_receipt_ref" => "retry-receipt://#{incident_class}",
        "terminal_receipt_ref" => "lower-receipt://completed",
        "redaction_manifest_ref" => "redaction://incident/#{incident_class}",
        "operator_message_ref" => "operator-message://#{incident_class}"
      }
    end)
  end

  defp evidence_refs(kinds) do
    Enum.map(kinds, fn kind ->
      %{
        kind: kind,
        content_ref: "lower-artifact://#{kind}/receipt",
        collector_ref: "receipt-reducer"
      }
    end)
  end
end
