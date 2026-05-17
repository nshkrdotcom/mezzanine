defmodule Mezzanine.Substrate.TransitionsTest do
  use ExUnit.Case, async: true

  alias Mezzanine.Substrate.OperationRequest
  alias Mezzanine.Substrate.PayloadEnvelope
  alias Mezzanine.Substrate.ReviewCase
  alias Mezzanine.Substrate.SourceItem
  alias Mezzanine.Substrate.Transitions
  alias Mezzanine.Substrate.WorkflowRun

  test "transitions source admission without provider branching" do
    {:ok, source_item} =
      SourceItem.new(%{
        source_item_ref: "source-item://tenant-a/item-a",
        operation_context_ref: "operation-context://tenant-a/request-a",
        source_role_ref: "role://tracker",
        subject_ref: "subject://tenant-a/item-a",
        payload: inline_payload()
      })

    assert {:ok, deduped} = Transitions.transition(source_item, :deduplicated)
    assert {:ok, publication} = Transitions.transition(deduped, :publication_requested)
    assert {:ok, published} = Transitions.transition(publication, :published)
    assert published.state == :published

    assert {:error, {:invalid_transition, :published, :publication_requested}} =
             Transitions.transition(published, :publication_requested)
  end

  test "transitions operation requests through resolution and dispatch" do
    {:ok, request} =
      OperationRequest.new(%{
        operation_request_ref: "operation-request://tenant-a/request-a",
        operation_context_ref: "operation-context://tenant-a/request-a",
        operation_role_ref: "operation-role://runtime/draft",
        operation_class: :runtime_operation,
        payload: inline_payload(),
        metadata: %{state: :ignored}
      })

    request = %{request | state: :requested}

    assert {:ok, resolved} = Transitions.transition(request, :resolved)
    assert {:ok, authorized} = Transitions.transition(resolved, :authorized)
    assert {:ok, dispatched} = Transitions.transition(authorized, :dispatched)
    assert {:ok, completed} = Transitions.transition(dispatched, :completed)
    assert completed.state == :completed
  end

  test "transitions review cases by required decision count" do
    {:ok, review} =
      ReviewCase.new(%{
        review_ref: "review://tenant-a/review-a",
        operation_context_ref: "operation-context://tenant-a/request-a",
        subject_ref: "subject://tenant-a/item-a",
        state: :pending,
        required_decisions: 2
      })

    assert {:ok, partial} = Transitions.transition(review, {:record_decision, "decision://one"})
    assert partial.state == :pending
    assert {:ok, decided} = Transitions.transition(partial, {:record_decision, "decision://two"})
    assert decided.state == :decided
  end

  test "transitions workflow runs through retry, rework, expiry, cancellation, and archive" do
    {:ok, workflow} =
      WorkflowRun.new(%{
        workflow_run_ref: "workflow-run://tenant-a/run-a",
        operation_context_ref: "operation-context://tenant-a/request-a",
        work_item_ref: "work-item://tenant-a/work-a",
        state: :queued
      })

    assert {:ok, running} = Transitions.transition(workflow, :running)
    assert {:ok, retrying} = Transitions.transition(running, :retry_scheduled)
    assert {:ok, resumed} = Transitions.transition(retrying, :running)
    assert {:ok, review} = Transitions.transition(resumed, :awaiting_review)
    assert {:ok, rework} = Transitions.transition(review, :rework_requested)
    assert {:ok, queued} = Transitions.transition(rework, :queued)
    assert {:ok, cancelled} = Transitions.transition(queued, :cancelled)
    assert {:ok, archived} = Transitions.transition(cancelled, :archived)
    assert archived.state == :archived

    {:ok, expiring} = %{workflow | state: :queued} |> Transitions.transition(:expired)
    assert {:ok, _archived_expiry} = Transitions.transition(expiring, :archived)
  end

  defp inline_payload do
    {:ok, payload} =
      PayloadEnvelope.new(%{
        payload_ref: "payload://tenant-a/payload-a",
        storage_mode: :inline,
        schema_ref: "schema://payload",
        redaction_ref: "redaction://standard",
        data: %{title: "Document"}
      })

    payload
  end
end
