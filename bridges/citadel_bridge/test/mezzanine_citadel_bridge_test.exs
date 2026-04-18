defmodule Mezzanine.CitadelBridgeTest do
  use ExUnit.Case, async: true

  alias Citadel.HostIngress.{RequestContext, RunRequest}
  alias Mezzanine.CitadelBridge
  alias Mezzanine.Intent.RunIntent

  test "compile_run_request emits a valid Citadel host-ingress request" do
    intent = run_intent()

    assert {:ok, %RunRequest{} = request} =
             CitadelBridge.compile_run_request(intent, %{
               target_id: "linear-default",
               service_id: "linear",
               allowed_operations: ["linear.issue.execute"],
               objective: "Execute Linear work"
             })

    assert request.run_request_id == intent.intent_id
    assert request.capability_id == intent.capability
    assert request.target.target_id == "linear-default"
    assert request.execution.allowed_operations == ["linear.issue.execute"]
  end

  test "compile_run_request accepts an explicit lower execution intent override" do
    intent = run_intent()

    assert {:ok, %RunRequest{} = request} =
             CitadelBridge.compile_run_request(intent, %{
               target_id: "linear-default",
               service_id: "linear",
               execution_intent_family: "process",
               execution_intent: %{
                 "command" => "echo",
                 "args" => ["hello"],
                 "environment" => %{},
                 "extensions" => %{"source" => "test"}
               },
               allowed_operations: ["linear.issue.execute"],
               objective: "Execute Linear work"
             })

    assert request.execution.execution_intent_family == "process"
    assert request.execution.execution_intent["command"] == "echo"
    assert request.execution.execution_intent["extensions"]["source"] == "test"
  end

  test "compile_run_request propagates durable submission identity into request extensions" do
    intent = run_intent()

    assert {:ok, %RunRequest{} = request} =
             CitadelBridge.compile_run_request(intent, %{
               target_id: "linear-default",
               service_id: "linear",
               allowed_operations: ["linear.issue.execute"],
               submission_dedupe_key: "tenant-cb:work-1:expense_capture:1",
               objective: "Execute Linear work"
             })

    assert request.extensions["submission_dedupe_key"] == "tenant-cb:work-1:expense_capture:1"
  end

  test "build_request_context emits a valid host-ingress request context" do
    intent = run_intent()

    assert {:ok, %RequestContext{} = context} =
             CitadelBridge.build_request_context(intent, %{
               tenant_id: "tenant-cb",
               actor_id: "ops_lead",
               request_id: "req-1",
               trace_id: "trace-1"
             })

    assert context.tenant_id == "tenant-cb"
    assert context.actor_id == "ops_lead"
    assert context.request_id == "req-1"
  end

  test "event mapping produces Mezzanine audit attrs" do
    mapped =
      CitadelBridge.to_audit_attrs(
        %{
          status: :completed,
          run_id: "run-1",
          actor_ref: "citadel-runtime",
          payload: %{"step" => 2}
        },
        %{program_id: "program-1", work_object_id: "work-1"}
      )

    assert mapped.program_id == "program-1"
    assert mapped.work_object_id == "work-1"
    assert mapped.run_id == "run-1"
    assert mapped.event_kind == :run_completed
  end

  defp run_intent do
    RunIntent.new!(%{
      intent_id: "intent-1",
      program_id: "program-1",
      work_id: "work-1",
      capability: "linear.issue.execute",
      runtime_class: :session,
      placement: %{
        target_id: "linear-default",
        service_id: "linear",
        boundary_class: "session",
        routing_tags: ["linear", "session"]
      },
      grant_profile: %{"allowed_tools" => ["linear.issue.update"]},
      input: %{"issue_id" => "ENG-42"},
      metadata: %{"tenant_id" => "tenant-cb", "objective" => "Resolve Linear issue"}
    })
  end
end
