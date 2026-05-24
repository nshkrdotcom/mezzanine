defmodule Mezzanine.AIExecutionTest do
  use ExUnit.Case, async: true

  alias Mezzanine.AIExecution
  alias Mezzanine.AIExecution.{RenderResult, RuntimeDeps}
  alias OuterBrain.ContextABI
  alias OuterBrain.ContextABI.Failure

  test "fixture router returns deterministic route decisions through the adapter behaviour" do
    assert {:ok, packet, _compile_receipt} = ContextABI.compile(compile_request())
    request = route_request(packet)

    assert {:ok, decision} = AIExecution.route(request)
    assert {:ok, decision_again} = AIExecution.route(request)

    assert decision == decision_again
    assert decision.selected_route_kind == :fixture
    assert decision.selected_model_profile_ref == "class://coding-small"
    assert decision.route_decision_ref =~ ~r/^route-decision:\/\/[0-9a-f]{64}$/
  end

  test "render context converts OuterBrain render refs into Mezzanine render result" do
    assert {:ok, packet, _compile_receipt} = ContextABI.compile(compile_request())
    assert {:ok, decision} = AIExecution.route(route_request(packet))

    deps = %RuntimeDeps{renderer: OuterBrain.Prompting.ContextRenderer.Fixture}

    assert {:ok, %RenderResult{} = result} =
             AIExecution.render_context(packet, decision, deps,
               workflow_ref: "workflow://tenant-a/run-a"
             )

    assert result.tenant_ref == packet.tenant_ref
    assert result.context_packet_ref == packet.context_packet_ref
    assert result.route_decision_ref == decision.route_decision_ref
    assert result.prompt_artifact_ref =~ ~r/^prompt-artifact:\/\/[0-9a-f]{64}$/
    assert result.provider_payload_ref =~ ~r/^provider-payload:\/\/[0-9a-f]{64}$/
    assert result.payload_hash =~ ~r/^sha256:[0-9a-f]{64}$/
    refute Map.has_key?(Map.from_struct(result), :raw_prompt)
  end

  test "invocation request copies prompt/provider payload refs from RenderResult" do
    assert {:ok, packet, _compile_receipt} = ContextABI.compile(compile_request())
    assert {:ok, decision} = AIExecution.route(route_request(packet))

    assert {:ok, render_result} =
             AIExecution.render_context(packet, decision, %RuntimeDeps{},
               workflow_ref: "workflow://tenant-a/run-a"
             )

    assert {:ok, invocation_request} =
             AIExecution.invocation_request(render_result, decision,
               idempotency_key: "idem://tenant-a/model/a",
               credential_lease_ref: "credential-lease://tenant-a/model/a"
             )

    assert invocation_request.prompt_artifact_ref == render_result.prompt_artifact_ref
    assert invocation_request.provider_payload_ref == render_result.provider_payload_ref
    assert invocation_request.model_profile_ref == decision.selected_model_profile_ref
  end

  test "fixture optimizer emits promotion-required candidate receipts" do
    assert {:ok, [candidate]} =
             AIExecution.propose(%{
               tenant_ref: "tenant://tenant-a",
               objective_ref: "objective://tenant-a/quality",
               candidate_source_refs: ["trace-dataset://tenant-a/a"],
               promotion_policy_ref: "promotion-policy://tenant-a/default",
               trace_ref: "trace://tenant-a/optimization"
             })

    assert candidate.candidate_ref =~ ~r/^optimization-candidate:\/\/[0-9a-f]{64}$/
    assert candidate.promotion_required? == true
    assert candidate.lineage_refs == ["trace-dataset://tenant-a/a"]
  end

  test "render result rejects raw prompt or provider payload fields" do
    assert {:error, %Failure{} = failure} =
             RenderResult.new(%{
               tenant_ref: "tenant://tenant-a",
               workflow_ref: "workflow://tenant-a/run-a",
               context_packet_ref: "context-packet://a",
               route_decision_ref: "route-decision://a",
               prompt_artifact_ref: "prompt-artifact://a",
               provider_payload_ref: "provider-payload://a",
               payload_hash: "sha256:" <> String.duplicate("a", 64),
               provider_family: "fixture",
               trace_ref: "trace://tenant-a/run-a",
               raw_prompt: "no"
             })

    assert failure.reason_code == "mezzanine.ai_execution.raw_payload_rejected.v1"
  end

  defp compile_request do
    %{
      tenant_ref: "tenant://tenant-a",
      user_request_ref: "artifact://tenant-a/request/a",
      system_instruction_ref: "artifact://tenant-a/system/a",
      memory_refs: ["memory://tenant-a/promoted/a"],
      budget_ref: "budget://tenant-a/run-a",
      model_class_allowlist: ["class://coding-small"],
      route_policy_ref: "route-policy://tenant-a/default",
      trace_ref: "trace://tenant-a/run-a"
    }
  end

  defp route_request(packet) do
    %{
      tenant_ref: packet.tenant_ref,
      workflow_ref: "workflow://tenant-a/run-a",
      context_packet_ref: packet.context_packet_ref,
      packet_hash: packet.packet_hash,
      authority_ref: "authority://tenant-a/context/a",
      route_policy_ref: packet.route_policy_ref,
      model_class_allowlist: packet.model_class_allowlist,
      trace_ref: packet.trace_ref
    }
  end
end
