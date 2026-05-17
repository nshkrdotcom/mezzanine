defmodule Mezzanine.Substrate.ConstructorTest do
  use ExUnit.Case, async: true

  alias Mezzanine.Substrate.Artifact
  alias Mezzanine.Substrate.Command
  alias Mezzanine.Substrate.ContextConsistency
  alias Mezzanine.Substrate.EvidenceRecord
  alias Mezzanine.Substrate.ExecutionInstruction
  alias Mezzanine.Substrate.GovernedInvocationEnvelope
  alias Mezzanine.Substrate.OperationContext
  alias Mezzanine.Substrate.OperationLineageEvent
  alias Mezzanine.Substrate.OperationPlanValidator
  alias Mezzanine.Substrate.OperationReceipt
  alias Mezzanine.Substrate.OperationRequest
  alias Mezzanine.Substrate.OperationGroupReceipt
  alias Mezzanine.Substrate.OperationDisposition
  alias Mezzanine.Substrate.PayloadEnvelope
  alias Mezzanine.Substrate.ProjectionRef
  alias Mezzanine.Substrate.ResolvedOperationPlan
  alias Mezzanine.Substrate.ResultEnvelope
  alias Mezzanine.Substrate.ReviewCase
  alias Mezzanine.Substrate.SourceItem
  alias Mezzanine.Substrate.WorkItem
  alias Mezzanine.Substrate.WorkTarget
  alias Mezzanine.Substrate.WorkflowRun

  test "constructs required generic substrate structs" do
    assert {:ok, context} = OperationContext.new(context_attrs())
    assert context.operation_context_ref == "operation-context://tenant-a/request-a"

    assert {:ok, _source_item} =
             SourceItem.new(%{
               source_item_ref: "source-item://tenant-a/item-a",
               operation_context_ref: context.operation_context_ref,
               source_role_ref: "role://issue-tracker",
               subject_ref: "subject://tenant-a/item-a",
               payload: inline_payload()
             })

    assert {:ok, _work_target} =
             WorkTarget.new(%{
               work_target_ref: "work-target://tenant-a/target-a",
               operation_context_ref: context.operation_context_ref,
               target_kind: :artifact,
               subject_ref: "subject://tenant-a/item-a"
             })

    assert {:ok, _work_item} =
             WorkItem.new(%{
               work_item_ref: "work-item://tenant-a/work-a",
               operation_context_ref: context.operation_context_ref,
               workflow_run_ref: "workflow-run://tenant-a/run-a",
               state: :queued
             })

    assert {:ok, _artifact} =
             Artifact.new(%{
               artifact_ref: "artifact://tenant-a/repo-a/file-a",
               operation_context_ref: context.operation_context_ref,
               artifact_kind: :text,
               payload: inline_payload()
             })

    assert {:ok, _command} =
             Command.new(%{
               command_ref: "command://tenant-a/command-a",
               operation_context_ref: context.operation_context_ref,
               command_kind: :submit_work,
               payload: inline_payload()
             })

    assert {:ok, _request} =
             OperationRequest.new(%{
               operation_request_ref: "operation-request://tenant-a/request-a",
               operation_context_ref: context.operation_context_ref,
               operation_role_ref: "operation-role://runtime/draft",
               operation_class: :runtime_operation,
               payload: inline_payload()
             })

    assert {:ok, plan} =
             ResolvedOperationPlan.new(%{
               operation_plan_ref: "operation-plan://tenant-a/run-a/runtime",
               operation_context_ref: context.operation_context_ref,
               binding_ref: "binding://tenant-a/install-a/runtime/primary",
               manifest_ref: "manifest://local/doc-runtime",
               operation_ref: "operation://runtime/draft",
               operation_class: :runtime_operation,
               adapter_ref: "adapter://local/doc-runtime",
               credential_scope_ref: "credential-scope://tenant-a/doc-runtime",
               side_effect_class: :read_write,
               input_schema_ref: "schema://input",
               output_schema_ref: "schema://output"
             })

    assert {:ok, envelope} =
             GovernedInvocationEnvelope.new(%{
               invocation_ref: "invocation://tenant-a/invocation-a",
               operation_context_ref: context.operation_context_ref,
               tenant_ref: context.tenant_ref,
               installation_ref: context.installation_ref,
               trace_ref: context.trace_ref,
               idempotency_key: context.idempotency_key,
               operation_plan: plan,
               payload: inline_payload()
             })

    refute :operation_role_ref in GovernedInvocationEnvelope.fields()
    refute :source_role_ref in GovernedInvocationEnvelope.fields()
    refute :runtime_role_ref in GovernedInvocationEnvelope.fields()
    assert envelope.operation_plan.operation_plan_ref == plan.operation_plan_ref

    assert {:ok, _instruction} =
             ExecutionInstruction.new(%{
               instruction_ref: "instruction://tenant-a/instruction-a",
               invocation_ref: envelope.invocation_ref,
               operation_context_ref: context.operation_context_ref,
               execution_target_ref: "execution-target://local/document",
               operation_ref: plan.operation_ref,
               payload: inline_payload()
             })

    assert {:ok, receipt} =
             OperationReceipt.new(%{
               receipt_ref: "receipt://tenant-a/receipt-a",
               operation_context_ref: context.operation_context_ref,
               operation_plan_ref: plan.operation_plan_ref,
               trace_ref: context.trace_ref,
               status: :accepted,
               started_at: ~U[2026-05-16 00:00:00Z],
               completed_at: ~U[2026-05-16 00:00:01Z],
               result: inline_result()
             })

    assert {:ok, _group} =
             OperationGroupReceipt.new(%{
               group_receipt_ref: "receipt-group://tenant-a/group-a",
               operation_context_ref: context.operation_context_ref,
               receipt_refs: [receipt.receipt_ref],
               status: :accepted
             })

    assert {:ok, _lineage} =
             OperationLineageEvent.new(%{
               event_ref: "lineage://tenant-a/event-a",
               operation_context_ref: context.operation_context_ref,
               trace_ref: context.trace_ref,
               event_kind: :operation_completed,
               occurred_at: ~U[2026-05-16 00:00:01Z],
               predecessor_refs: []
             })

    assert {:ok, _disposition} =
             OperationDisposition.new(%{
               disposition_ref: "disposition://tenant-a/receipt-a",
               receipt_ref: receipt.receipt_ref,
               disposition: :accepted
             })

    assert {:ok, _review} =
             ReviewCase.new(%{
               review_ref: "review://tenant-a/review-a",
               operation_context_ref: context.operation_context_ref,
               subject_ref: "subject://tenant-a/item-a",
               state: :pending
             })

    assert {:ok, _evidence} =
             EvidenceRecord.new(%{
               evidence_ref: "evidence://tenant-a/evidence-a",
               operation_context_ref: context.operation_context_ref,
               subject_ref: "subject://tenant-a/item-a",
               receipt_ref: receipt.receipt_ref,
               payload: inline_payload()
             })

    assert {:ok, _projection_ref} =
             ProjectionRef.new(%{
               projection_ref: "projection://tenant-a/work-a",
               operation_context_ref: context.operation_context_ref,
               subject_ref: "subject://tenant-a/item-a",
               projection_kind: :work_item
             })

    assert {:ok, _workflow} =
             WorkflowRun.new(%{
               workflow_run_ref: "workflow-run://tenant-a/run-a",
               operation_context_ref: context.operation_context_ref,
               work_item_ref: "work-item://tenant-a/work-a",
               state: :planned
             })
  end

  test "rejects forbidden concrete vendor-shaped fields in generic attrs" do
    assert {:error, {:forbidden_generic_field, :github_pr_number}} =
             OperationRequest.new(
               Map.put(
                 %{
                   operation_request_ref: "operation-request://tenant-a/request-a",
                   operation_context_ref: "operation-context://tenant-a/request-a",
                   operation_role_ref: "operation-role://runtime/draft",
                   operation_class: :runtime_operation,
                   payload: inline_payload()
                 },
                 :github_pr_number,
                 12
               )
             )
  end

  test "validates resolved operation plan same-request dispatch completeness" do
    assert {:ok, plan} =
             ResolvedOperationPlan.new(%{
               operation_plan_ref: "operation-plan://tenant-a/run-a/runtime",
               operation_context_ref: "operation-context://tenant-a/request-a",
               binding_ref: "binding://tenant-a/install-a/runtime/primary",
               manifest_ref: "manifest://local/doc-runtime",
               operation_ref: "operation://runtime/draft",
               operation_class: :runtime_operation,
               adapter_ref: "adapter://local/doc-runtime",
               credential_scope_ref: "credential-scope://tenant-a/doc-runtime",
               side_effect_class: :read_write,
               input_schema_ref: "schema://input"
             })

    assert :ok = OperationPlanValidator.validate_complete(plan)

    incomplete = %{plan | adapter_ref: ""}

    assert {:error, {:incomplete_resolved_operation_plan, :adapter_ref}} =
             OperationPlanValidator.validate_complete(incomplete)
  end

  test "validates denormalized boundary fields against operation context" do
    assert {:ok, context} = OperationContext.new(context_attrs())

    assert :ok =
             ContextConsistency.validate_boundary(context, %{
               tenant_ref: context.tenant_ref,
               installation_ref: context.installation_ref,
               trace_ref: context.trace_ref,
               idempotency_key: context.idempotency_key
             })

    assert {:error, {:context_mismatch, :tenant_ref}} =
             ContextConsistency.validate_boundary(context, %{
               tenant_ref: "tenant://other",
               trace_ref: context.trace_ref
             })
  end

  defp context_attrs do
    %{
      operation_context_ref: "operation-context://tenant-a/request-a",
      actor_ref: "actor://tenant-a/user-a",
      tenant_ref: "tenant://tenant-a",
      installation_ref: "installation://tenant-a/product-a/install-a",
      trace_ref: "trace://tenant-a/trace-a",
      request_ref: "request://tenant-a/request-a",
      idempotency_key: "idempotency://tenant-a/request-a"
    }
  end

  defp inline_payload do
    {:ok, payload} =
      PayloadEnvelope.new(%{
        payload_ref: "payload://tenant-a/payload-a",
        storage_mode: :inline,
        schema_ref: "schema://payload",
        redaction_ref: "redaction://standard",
        data: %{body: "hello"}
      })

    payload
  end

  defp inline_result do
    {:ok, result} =
      ResultEnvelope.new(%{
        result_ref: "result://tenant-a/result-a",
        storage_mode: :inline,
        schema_ref: "schema://result",
        redaction_ref: "redaction://standard",
        data: %{body: "ok"}
      })

    result
  end
end
