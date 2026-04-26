defmodule Mezzanine.Citadel.SubstrateIngressTest do
  use ExUnit.Case, async: true

  alias Citadel.ActionOutboxEntry
  alias Citadel.InvocationRequest.V2, as: InvocationRequestV2
  alias Mezzanine.Citadel.SubstrateIngress
  alias Mezzanine.CitadelBridge
  alias Mezzanine.Intent.RunIntent

  test "compiles run intents through pure Citadel substrate governance" do
    assert {:ok, compiled} =
             SubstrateIngress.compile_run_intent(run_intent(), compile_attrs(), [policy_pack()])

    assert compiled.audit_attrs.fact_kind == :substrate_governance_accepted
    assert compiled.audit_attrs.execution_id == "execution-1"
    assert compiled.decision_hash == compiled.authority_packet.decision_hash
    assert compiled.rejection_classification == nil

    assert %InvocationRequestV2{} = compiled.lower_intent.invocation_request
    assert %ActionOutboxEntry{} = compiled.lower_intent.outbox_entry

    request = compiled.lower_intent.invocation_request

    assert request.request_id == "execution-1"
    assert request.session_id == "substrate/execution-1"
    assert request.trace_id == "0123456789abcdef0123456789abcdef"

    assert request.extensions["citadel"]["ingress_provenance"]["ingress_kind"] ==
             "substrate_origin"

    assert request.extensions["citadel"]["execution_envelope"]["submission_dedupe_key"] ==
             "tenant-cb:work-1:expense_capture:3"

    assert compiled.lower_intent.outbox_entry.action.action_kind ==
             "citadel.substrate_invocation_request.v2"
  end

  test "facade keeps the old bridge name but exposes only substrate governance" do
    assert {:ok, compiled} =
             CitadelBridge.compile_submission(run_intent(), compile_attrs(), [policy_pack()])

    assert compiled.lower_intent.invocation_request.session_id == "substrate/execution-1"
  end

  test "carries run intent grant-profile tools into Citadel execution governance" do
    assert {:ok, compiled} =
             SubstrateIngress.compile_run_intent(run_intent(), compile_attrs(), [policy_pack()])

    assert compiled.lower_intent.invocation_request.execution_governance.sandbox[
             "allowed_tools"
           ] == ["linear.issue.update"]
  end

  test "bridge lib contains no host-ingress or session-continuity dependencies" do
    bridge_lib =
      Path.expand("../lib", __DIR__)
      |> Path.join("**/*.ex")
      |> Path.wildcard()

    assert bridge_lib != []

    forbidden = [
      "Citadel.HostIngress",
      "SessionServer",
      "SessionDirectory",
      "SessionContinuityCommit",
      "PersistedSessionBlob",
      "PersistedSessionEnvelope"
    ]

    Enum.each(bridge_lib, fn path ->
      contents = File.read!(path)

      Enum.each(forbidden, fn fragment ->
        refute String.contains?(contents, fragment), "#{path} still references #{fragment}"
      end)
    end)
  end

  defp compile_attrs do
    %{
      tenant_id: "tenant-cb",
      installation_id: "installation-1",
      installation_revision: 3,
      actor_ref: "scheduler",
      subject_id: "work-1",
      execution_id: "execution-1",
      request_trace_id: "request-trace-1",
      substrate_trace_id: "0123456789abcdef0123456789abcdef",
      idempotency_key: "tenant-cb:work-1:expense_capture:3",
      submission_dedupe_key: "tenant-cb:work-1:expense_capture:3",
      target_id: "workspace_runtime",
      service_id: "workspace_runtime",
      boundary_class: "workspace_session",
      scope_kind: "work_object",
      target_kind: "runtime_target",
      execution_intent_family: "process",
      execution_intent: %{
        "command" => "linear.issue.execute",
        "args" => ["work-1"],
        "environment" => %{"TRACE_ID" => "0123456789abcdef0123456789abcdef"},
        "extensions" => %{}
      },
      allowed_operations: ["linear.issue.execute"],
      downstream_scope: "work:work-1",
      workspace_mutability: "read_write",
      risk_hints: ["writes_workspace"],
      policy_refs: ["policy-v1"]
    }
  end

  defp run_intent do
    RunIntent.new!(%{
      intent_id: "intent-1",
      program_id: "program-1",
      work_id: "work-1",
      capability: "linear.issue.execute",
      runtime_class: :session,
      placement: %{
        target_id: "workspace_runtime",
        service_id: "workspace_runtime",
        boundary_class: "workspace_session",
        routing_tags: ["linear", "session"]
      },
      grant_profile: %{"allowed_tools" => ["linear.issue.update"]},
      input: %{"issue_id" => "ENG-42"},
      metadata: %{"tenant_id" => "tenant-cb", "objective" => "Resolve Linear issue"}
    })
  end

  defp policy_pack do
    %{
      pack_id: "default",
      policy_version: "policy-v1",
      policy_epoch: 3,
      priority: 0,
      selector: %{
        tenant_ids: [],
        scope_kinds: [],
        environments: [],
        default?: true,
        extensions: %{}
      },
      profiles: %{
        trust_profile: "baseline",
        approval_profile: "standard",
        egress_profile: "restricted",
        workspace_profile: "workspace",
        resource_profile: "standard",
        boundary_class: "workspace_session",
        extensions: %{}
      },
      rejection_policy: %{
        denial_audit_reason_codes: ["policy_denied", "approval_missing"],
        derived_state_reason_codes: ["planning_failed"],
        runtime_change_reason_codes: ["scope_unavailable", "service_hidden", "boundary_stale"],
        governance_change_reason_codes: ["approval_missing"],
        extensions: %{}
      },
      extensions: %{}
    }
  end
end
