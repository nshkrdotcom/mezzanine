defmodule Mezzanine.WorkflowRuntime.TerminalLowerReceiptShapeTest do
  use ExUnit.Case, async: true

  alias Mezzanine.WorkflowRuntime.TerminalLowerReceiptShape

  test "live workflow and deterministic completion share the normalized terminal receipt shape" do
    live_attrs = live_signal_attrs()
    deterministic = deterministic_inputs(live_attrs.routing_facts)

    live_receipt = TerminalLowerReceiptShape.from_workflow_signal(live_attrs)

    deterministic_receipt =
      TerminalLowerReceiptShape.from_deterministic_completion(
        deterministic.execution,
        deterministic.accepted,
        deterministic.facts,
        deterministic.attrs
      )

    assert Map.has_key?(live_receipt, :receipt_id)
    assert Map.has_key?(deterministic_receipt, "receipt_id")

    assert TerminalLowerReceiptShape.missing_required_fields(live_receipt) == []
    assert TerminalLowerReceiptShape.missing_required_fields(deterministic_receipt) == []

    live_shape = TerminalLowerReceiptShape.shared_shape(live_receipt)
    deterministic_shape = TerminalLowerReceiptShape.shared_shape(deterministic_receipt)

    expected_fields = MapSet.new(TerminalLowerReceiptShape.shared_fields())
    assert MapSet.new(Map.keys(live_shape)) == expected_fields
    assert MapSet.new(Map.keys(deterministic_shape)) == expected_fields
    assert live_shape == deterministic_shape
  end

  test "shared shape normalizes nested atom-key metadata without raw key-style drift" do
    receipt = %{
      receipt_id: "lower-receipt://deterministic/shared",
      receipt_state: :succeeded,
      lower_receipt_ref: "lower-receipt://deterministic/shared",
      trace_id: "trace://shared",
      causation_id: "cause://shared",
      idempotency_key: "idem-shared",
      runtime_profile: %{runtime_profile_kind: :codex_session},
      connector_manifests: [%{connector_manifest_ref: "manifest://shared"}]
    }

    shape = TerminalLowerReceiptShape.shared_shape(receipt)

    assert shape["receipt_state"] == "succeeded"
    assert shape["runtime_profile"] == %{"runtime_profile_kind" => "codex_session"}
    assert shape["connector_manifests"] == [%{"connector_manifest_ref" => "manifest://shared"}]
  end

  test "missing required fields are explicit and stable" do
    receipt = %{
      receipt_id: "lower-receipt://deterministic/shared",
      receipt_state: "succeeded",
      trace_id: "trace://shared"
    }

    assert TerminalLowerReceiptShape.missing_required_fields(receipt) == [
             "lower_receipt_ref",
             "causation_id",
             "idempotency_key"
           ]
  end

  test "ambiguous local effect receipts retain reconciliation posture" do
    receipt =
      TerminalLowerReceiptShape.from_workflow_signal(%{
        signal_id: "receipt://p04/ambiguous",
        receipt_state: "ambiguous",
        lower_receipt_ref: "receipt://p04/ambiguous",
        trace_id: "trace://p04",
        correlation_id: "cause://p04",
        idempotency_key: "p04-effect",
        routing_facts: %{
          ambiguity_state: "outcome_unknown",
          continuation_ref: "continuation://p04/reconcile",
          retry_posture: "reconciliation_only_effect_retry_prohibited"
        }
      })

    assert receipt.ambiguity_state == "outcome_unknown"
    assert receipt.continuation_ref == "continuation://p04/reconcile"
    assert receipt.retry_posture == "reconciliation_only_effect_retry_prohibited"
    assert TerminalLowerReceiptShape.missing_required_fields(receipt) == []
  end

  defp live_signal_attrs do
    %{
      signal_id: "lower-receipt://deterministic/shared",
      receipt_state: "succeeded",
      terminal_state: "succeeded",
      lower_receipt_ref: "lower-receipt://deterministic/shared",
      lower_run_ref: "lower-run-shared",
      lower_attempt_ref: "lower-attempt-shared",
      lower_event_ref: "lineage://shared/effect_receipted",
      trace_id: "trace://shared",
      correlation_id: "cause://shared",
      idempotency_key: "idem-shared",
      routing_facts: shared_routing_facts()
    }
  end

  defp deterministic_inputs(routing_facts) do
    %{
      execution: %{
        id: "execution-shared",
        trace_id: "trace://shared",
        causation_id: "cause://shared",
        submission_dedupe_key: "idem-shared"
      },
      accepted: %{
        submission_ref: %{
          "run_id" => "lower-run-shared",
          "attempt_id" => "lower-attempt-shared",
          "ji_submission_key" => "ji-submission-shared"
        }
      },
      facts:
        routing_facts
        |> Map.put(:lower_receipt_ref, "lower-receipt://deterministic/shared")
        |> Map.put(:run_id, "lower-run-shared")
        |> Map.put(:attempt_id, "lower-attempt-shared"),
      attrs: %{actor_ref: "actor://shared"}
    }
  end

  defp shared_routing_facts do
    %{
      provider_object_refs: ["provider-object://shared"],
      ambiguity_state: "outcome_unknown",
      continuation_ref: "continuation://shared/reconcile",
      retry_posture: "reconciliation_only_effect_retry_prohibited",
      evidence_artifact_refs: [
        %{kind: "provider_evidence", content_ref: "artifact://github-pr/shared"}
      ],
      artifact_refs: [
        %{kind: "provider_evidence", content_ref: "artifact://github-pr/shared"}
      ],
      token_totals: %{input: 10, output: 5},
      token_dedupe: %{accepted_count: 1},
      rate_limit: %{remaining: 99},
      retry: [%{retry_ref: "retry://shared"}],
      retry_receipts: [%{retry_receipt_ref: "retry-receipt://shared"}],
      runtime_events: [%{event_ref: "lineage://shared/effect_receipted"}],
      aitrace: %{evidence_receipt_ref: "aitrace://shared"},
      prompt_provenance: %{prompt_hash: "sha256:prompt"},
      memory_context: %{context_pack_ref: "context-pack://shared"},
      semantic_failure: %{failure_ref: "semantic-failure://shared"},
      provider_account: %{provider_account_ref: "provider-account://shared"},
      credential: %{credential_ref: "credential://shared"},
      runtime_profile: %{
        runtime_profile_ref: "runtime-profile://shared",
        runtime_profile_kind: "codex_session"
      },
      governed_lower_envelope: %{
        lower_request_ref: "lower-request://shared",
        lower_runtime_kind: "codex_session",
        capability_id: "codex.session.turn"
      },
      authority_decision: %{
        authority_ref: "authority://shared",
        authority_decision_hash: String.duplicate("a", 64)
      },
      connector_manifests: [%{connector_manifest_ref: "manifest://shared"}],
      capability_negotiations: [%{capability_negotiation_ref: "cap-neg://shared"}],
      incident_bundles: [%{incident_ref: "incident://shared"}],
      acceptance: %{scenario_refs: ["scenario://shared"]},
      provider_evidence: %{evidence_ref: "evidence://github-pr/shared"},
      source_publication: %{status: "published"},
      workpad_refs: ["workpad://shared"]
    }
  end
end
