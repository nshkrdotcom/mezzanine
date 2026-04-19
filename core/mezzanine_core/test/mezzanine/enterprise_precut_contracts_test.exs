defmodule Mezzanine.EnterprisePrecutContractsTest do
  use ExUnit.Case, async: true

  alias Mezzanine.{
    ActivityCallRef,
    ActivityLeaseBundle,
    ActivityLeaseScopeRequest,
    ActivityResult,
    CommandReceipt,
    EventFact,
    IncidentBundle,
    ProjectionSnapshot,
    ReviewTask,
    WorkflowRef,
    WorkflowSignalReceipt,
    WorkflowStartOutboxPayload
  }

  @modules [
    CommandReceipt,
    WorkflowRef,
    WorkflowStartOutboxPayload,
    WorkflowSignalReceipt,
    EventFact,
    ProjectionSnapshot,
    ReviewTask,
    ActivityCallRef,
    ActivityResult,
    IncidentBundle,
    ActivityLeaseScopeRequest,
    ActivityLeaseBundle,
    Mezzanine.WorkflowRuntime,
    Mezzanine.ActivityLeaseBroker
  ]

  test "loads M24 Mezzanine contract modules and behaviours" do
    for module <- @modules do
      assert Code.ensure_loaded?(module), "#{inspect(module)} is not compiled"
    end
  end

  test "builds command receipt and workflow start outbox with enterprise scope" do
    assert {:ok, receipt} =
             CommandReceipt.new(%{
               command_id: "cmd-105",
               command_name: "work.start",
               command_version: "v1",
               tenant_ref: "tenant-acme",
               principal_ref: "principal-operator",
               resource_ref: "resource-work-1",
               trace_id: "trace-105",
               idempotency_key: "idem-105",
               authority_packet_ref: "authpkt-105",
               permission_decision_ref: "decision-105",
               status: "accepted"
             })

    assert receipt.contract_name == "Mezzanine.CommandReceipt.v1"

    assert {:ok, outbox} =
             WorkflowStartOutboxPayload.new(%{
               outbox_id: "wso-110",
               tenant_ref: receipt.tenant_ref,
               installation_ref: "installation-main",
               workspace_ref: "workspace-main",
               project_ref: "project-main",
               environment_ref: "env-prod",
               principal_ref: receipt.principal_ref,
               resource_ref: receipt.resource_ref,
               command_envelope_ref: "command-envelope-105",
               command_receipt_ref: "command-receipt-105",
               command_id: receipt.command_id,
               workflow_type: "agentic_workflow",
               workflow_id: "wf-110",
               workflow_version: "agent-run.v1",
               workflow_input_version: "v1",
               workflow_input_ref: "claim-workflow-input-110",
               authority_packet_ref: receipt.authority_packet_ref,
               permission_decision_ref: receipt.permission_decision_ref,
               idempotency_key: receipt.idempotency_key,
               dedupe_scope: "tenant-acme:resource-work-1:agentic_workflow:cmd-105",
               trace_id: receipt.trace_id,
               correlation_id: receipt.correlation_id || "corr-105",
               release_manifest_ref: "phase4-v6-milestone26",
               payload_hash: String.duplicate("a", 64),
               payload_ref: "claim-workflow-110",
               dispatch_state: "queued",
               retry_count: 0
             })

    assert outbox.workflow_id == "wf-110"

    assert {:ok, receipt} =
             Mezzanine.WorkflowStartReceipt.new(%{
               workflow_ref: "workflow-ref://wf-110",
               workflow_id: outbox.workflow_id,
               workflow_run_id: "run-110",
               workflow_type: outbox.workflow_type,
               workflow_version: outbox.workflow_version,
               tenant_ref: outbox.tenant_ref,
               resource_ref: outbox.resource_ref,
               command_id: outbox.command_id,
               idempotency_key: outbox.idempotency_key,
               trace_id: outbox.trace_id,
               correlation_id: outbox.correlation_id,
               release_manifest_ref: outbox.release_manifest_ref,
               start_state: "started",
               duplicate?: false,
               retry_class: "none",
               failure_class: "none"
             })

    assert receipt.workflow_run_id == "run-110"

    assert {:error, :workflow_runtime_unconfigured} =
             Mezzanine.WorkflowRuntime.start_workflow(outbox)
  end

  test "workflow signal receipts expose lifecycle states without claiming completion" do
    assert {:ok, receipt} =
             WorkflowSignalReceipt.new(%{
               tenant_ref: "tenant-acme",
               signal_id: "sig-111",
               workflow_id: "wf-110",
               signal_name: "operator.cancel",
               signal_version: "v1",
               command_id: "cmd-111",
               authority_packet_ref: "authpkt-111",
               permission_decision_ref: "decision-111",
               idempotency_key: "idem-111",
               trace_id: "trace-111",
               authority_state: "authorized",
               local_state: "accepted",
               dispatch_state: "queued",
               workflow_effect_state: "pending",
               projection_state: "stale"
             })

    assert receipt.dispatch_state == "queued"
    assert receipt.workflow_effect_state == "pending"
  end

  test "activity and incident contracts carry lower, semantic, projection, and release refs" do
    assert {:ok, workflow_ref} =
             WorkflowRef.new(%{
               tenant_ref: "tenant-acme",
               resource_ref: "resource-work-1",
               subject_ref: "subject-1",
               workflow_type: "agentic_workflow",
               workflow_id: "wf-110",
               workflow_version: "v1",
               starter_command_id: "cmd-105",
               trace_id: "trace-116",
               search_attributes: %{"phase4.workflow_type" => "agentic_workflow"},
               release_manifest_version: "phase4-v6-milestone24"
             })

    assert {:ok, activity_call} =
             ActivityCallRef.new(%{
               activity_call_id: "act-112",
               activity_name: "lower.execute",
               activity_version: "v1",
               workflow_ref: workflow_ref.workflow_id,
               tenant_ref: "tenant-acme",
               resource_ref: "resource-work-1",
               idempotency_key: "idem-act-112",
               trace_id: "trace-112",
               owner_repo: "jido_integration",
               timeout_policy: "bounded",
               retry_policy: "safe_idempotent"
             })

    assert {:ok, _lease_request} =
             ActivityLeaseScopeRequest.new(%{
               tenant_ref: "tenant-acme",
               principal_ref: "principal-operator",
               resource_ref: "resource-work-1",
               authority_packet_ref: "authpkt-112",
               permission_decision_ref: "decision-112",
               policy_revision: "policy-rev-1",
               lease_epoch: 1,
               revocation_epoch: 1,
               activity_type: "lower.execute",
               activity_id: activity_call.activity_call_id,
               workflow_ref: workflow_ref.workflow_id,
               lower_scope_ref: "lower-scope-112",
               requested_capabilities: ["lower.execute"],
               idempotency_key: activity_call.idempotency_key,
               trace_id: activity_call.trace_id,
               deadline: "2026-04-18T00:00:00Z"
             })

    assert {:ok, _bundle} =
             ActivityLeaseBundle.new(%{
               lease_ref: "lease-112",
               capability_scope_hash: String.duplicate("b", 64),
               authority_packet_ref: "authpkt-112",
               permission_decision_ref: "decision-112",
               policy_revision: "policy-rev-1",
               lease_epoch: 1,
               revocation_epoch: 1,
               expires_at: "2026-04-18T00:05:00Z",
               max_uses: 10,
               remaining_uses: 9,
               cache_status: "hit",
               evidence_ref: "evidence-112",
               failure_class: "none"
             })

    assert {:ok, _activity_result} =
             ActivityResult.new(%{
               activity_call_id: activity_call.activity_call_id,
               tenant_ref: "tenant-acme",
               resource_ref: "resource-work-1",
               workflow_ref: workflow_ref.workflow_id,
               lower_ref: "lower-112",
               semantic_ref: "semantic-115",
               routing_facts: %{"review_required" => false},
               failure_class: "none",
               retry_class: "none",
               trace_id: "trace-116"
             })

    assert {:ok, _bundle} =
             IncidentBundle.new(%{
               incident_bundle_id: "incident-116",
               tenant_ref: "tenant-acme",
               trace_id: "trace-116",
               command_ref: "cmd-105",
               authority_packet_ref: "authpkt-105",
               permission_decision_ref: "decision-105",
               workflow_ref: workflow_ref.workflow_id,
               activity_call_refs: [activity_call.activity_call_id],
               lower_refs: ["lower-112"],
               semantic_refs: ["semantic-115"],
               projection_refs: ["projection-116"],
               release_manifest_version: "phase4-v6-milestone24",
               proof_artifact_path:
                 "stack_lab/proofs/scenario_116_incident_reconstruction_join.md"
             })

    assert {:ok, _event_fact} =
             EventFact.new(%{
               event_fact_id: "fact-116",
               tenant_ref: "tenant-acme",
               fact_kind: "workflow.projected",
               producer_repo: "mezzanine",
               resource_ref: "resource-work-1",
               trace_id: "trace-116",
               causation_id: "cmd-105",
               payload_hash: String.duplicate("c", 64)
             })

    assert {:ok, _snapshot} =
             ProjectionSnapshot.new(%{
               projection_id: "projection-116",
               tenant_ref: "tenant-acme",
               projection_kind: "workflow_summary",
               owner_repo: "mezzanine",
               source_position: "fact-116",
               staleness_class: "fresh",
               trace_id: "trace-116"
             })

    assert {:ok, _review_task} =
             ReviewTask.new(%{
               review_task_id: "review-116",
               tenant_ref: "tenant-acme",
               resource_ref: "resource-work-1",
               workflow_ref: workflow_ref.workflow_id,
               requested_by_ref: "principal-operator",
               required_action: "approve",
               authority_context_ref: "authctx-116",
               status: "pending",
               trace_id: "trace-116"
             })
  end
end
