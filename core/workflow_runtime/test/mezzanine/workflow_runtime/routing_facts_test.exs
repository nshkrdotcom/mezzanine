defmodule Mezzanine.WorkflowRuntime.RoutingFactsTest do
  use ExUnit.Case, async: true

  alias Mezzanine.WorkflowExecutionLifecycleInput
  alias Mezzanine.WorkflowRuntime.RoutingFacts

  test "normalizes string-key routing facts through an explicit contract" do
    assert {:ok, facts} =
             RoutingFacts.decode(
               lifecycle_input(%{
                 "installation_revision" => 7,
                 "capability" => "codex.session.turn",
                 "runtime_class" => "workflow"
               })
             )

    assert facts.installation_revision == 7
    assert facts.capability == "codex.session.turn"
    assert facts.subject_id == "subject-14"
    assert RoutingFacts.atom(facts, :runtime_class, :session) == :workflow
  end

  test "rejects unknown routing facts instead of extending a broad key table" do
    assert {:error, {:unknown_routing_fact_keys, ["surprise_provider_switch"]}} =
             RoutingFacts.decode(
               lifecycle_input(%{
                 installation_revision: 7,
                 capability: "codex.session.turn",
                 surprise_provider_switch: true
               })
             )
  end

  test "reports required routing facts per workflow operation" do
    input =
      lifecycle_input(%{
        installation_revision: 7,
        allowed_operations: ["codex.session.turn"]
      })

    assert {:error, {:missing_routing_facts, :compile_citadel_authority, [:capability]}} =
             RoutingFacts.for_operation(input, :compile_citadel_authority)

    assert {:ok, facts} =
             input
             |> put_in([Access.key!(:routing_facts), :capability], "codex.session.turn")
             |> RoutingFacts.for_operation(:compile_citadel_authority)

    assert facts.subject_id == "subject-14"
  end

  defp lifecycle_input(routing_facts) do
    {:ok, input} =
      WorkflowExecutionLifecycleInput.new(%{
        tenant_ref: "tenant-14",
        installation_ref: "installation://installation-14@7",
        principal_ref: "principal-14",
        resource_ref: "resource-14",
        subject_ref: "subject-14",
        workflow_id: "workflow-14",
        workflow_type: "execution_attempt",
        workflow_version: "execution-attempt.v1",
        command_id: "cmd-14",
        command_receipt_ref: "command-receipt-14",
        workflow_input_ref: "claim://workflow-input/14",
        lower_submission_ref: "lower-submission-14",
        lower_idempotency_key: "lower-idem-14",
        activity_call_ref: "activity-call-14",
        authority_packet_ref: "authpkt-14",
        permission_decision_ref: "decision-14",
        idempotency_key: "idem-14",
        trace_id: "trace-14",
        correlation_id: "corr-14",
        release_manifest_ref: "phase4-v6-milestone27-execution-lifecycle-workflow",
        retry_policy: %{max_attempts: 3},
        terminal_policy: "quarantine_late_receipts",
        routing_facts: routing_facts
      })

    input
  end
end
