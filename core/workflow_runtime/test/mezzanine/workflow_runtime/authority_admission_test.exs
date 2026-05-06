defmodule Mezzanine.WorkflowRuntime.AuthorityAdmissionTest do
  use ExUnit.Case, async: true

  alias Mezzanine.WorkflowRuntime.AgentLoop
  alias Mezzanine.WorkflowRuntime.AuthorityAdmission

  test "provider dispatch rejects before lower handoff when authority refs are missing" do
    assert {:error, {:missing_required_authority_refs, missing}} =
             AuthorityAdmission.authorize_provider_dispatch(%{
               provider_family: "claude",
               provider_account_ref: "provider-account://tenant-1/claude/main",
               connector_instance_ref: "connector-instance://tenant-1/claude/default",
               credential_handle_ref: "credential-handle://tenant-1/claude/handle-1",
               credential_lease_ref: "credential-lease://tenant-1/claude/lease-1",
               target_ref: "target://tenant-1/local-process/1",
               operation_policy_ref: "operation-policy://tenant-1/claude/chat",
               policy_revision_ref: "policy-revision://tenant-1/rev-1",
               idempotency_key: "idem-authority-admission-1"
             })

    assert :system_authorization_ref in missing
    assert :authority_packet_ref in missing
    assert :attach_grant_ref in missing
    assert :target_auth_posture_ref in missing
    assert :workspace_ref in missing
  end

  test "provider dispatch rejects raw material before provider effects" do
    assert {:error, {:forbidden_authority_material, forbidden}} =
             AuthorityAdmission.authorize_provider_dispatch(
               valid_attrs()
               |> Map.put(:raw_token, "secret")
               |> Map.put(:provider_payload, %{"token" => "secret"})
             )

    assert :raw_token in forbidden
    assert :provider_payload in forbidden
  end

  test "provider dispatch rejects raw target material before lower handoff" do
    assert {:error, {:forbidden_authority_material, forbidden}} =
             AuthorityAdmission.authorize_provider_dispatch(
               valid_attrs()
               |> Map.put(:target_credential, "secret-target-token")
               |> Map.put(:target_path, "/tmp/tenant-1/token")
               |> Map.put(:workspace_secret, "workspace-secret")
               |> Map.put(:token_file, "/home/operator/.token")
             )

    assert :target_credential in forbidden
    assert :target_path in forbidden
    assert :workspace_secret in forbidden
    assert :token_file in forbidden
  end

  test "provider dispatch emits redacted handoff with idempotency lineage" do
    assert {:ok, handoff} = AuthorityAdmission.authorize_provider_dispatch(valid_attrs())

    assert handoff.provider_family == "claude"
    assert handoff.authority_packet_ref == "authority-packet://tenant-1/packet-1"
    assert handoff.connector_binding_ref == "connector-binding://tenant-1/claude/default"
    assert handoff.target_auth_posture_ref == "target-posture://tenant-1/local-process/1"
    assert handoff.operation_scope_ref == "operation-scope://tenant-1/claude/chat"
    assert handoff.workspace_ref == "workspace://tenant-1/runtime"
    assert handoff.idempotency_key == "idem-authority-admission-1"
    assert handoff.handoff_ref == "workflow-authority-handoff://idem-authority-admission-1"
    assert handoff.raw_material_present? == false

    refute String.contains?(inspect(handoff), "secret")
    refute Map.has_key?(handoff, :raw_token)
    refute Map.has_key?(handoff, :provider_payload)
  end

  test "provider dispatch requires connector binding and operation scope refs" do
    assert {:error, {:missing_required_authority_refs, missing}} =
             valid_attrs()
             |> Map.delete(:connector_binding_ref)
             |> Map.delete(:operation_scope_ref)
             |> AuthorityAdmission.authorize_provider_dispatch()

    assert :connector_binding_ref in missing
    assert :operation_scope_ref in missing
  end

  test "workflow admission keeps ReqLlmNext and another provider authority refs distinct" do
    reqllm_attrs =
      valid_attrs()
      |> Map.merge(%{
        provider_family: "reqllm_next",
        provider_account_ref: "provider-account://tenant-1/reqllm/openai",
        connector_instance_ref: "connector-instance://tenant-1/reqllm/openai",
        credential_handle_ref: "credential-handle://tenant-1/reqllm/openai-key",
        credential_lease_ref: "credential-lease://tenant-1/reqllm/openai-key",
        target_ref: "target://tenant-1/llm-http/1",
        attach_grant_ref: "attach-grant://tenant-1/llm-http/1",
        target_auth_posture_ref: "target-posture://tenant-1/llm-http/1",
        operation_policy_ref: "operation-policy://tenant-1/reqllm/responses",
        idempotency_key: "idem-authority-admission-reqllm",
        trace_id: "trace-authority-admission-reqllm"
      })

    claude_attrs = valid_attrs()

    assert {:ok, reqllm_handoff} =
             AuthorityAdmission.authorize_provider_dispatch(reqllm_attrs)

    assert {:ok, claude_handoff} =
             AuthorityAdmission.authorize_provider_dispatch(claude_attrs)

    assert reqllm_handoff.provider_family == "reqllm_next"
    assert claude_handoff.provider_family == "claude"
    refute reqllm_handoff.provider_account_ref == claude_handoff.provider_account_ref
    refute reqllm_handoff.credential_lease_ref == claude_handoff.credential_lease_ref
    refute reqllm_handoff.target_ref == claude_handoff.target_ref
    refute reqllm_handoff.operation_policy_ref == claude_handoff.operation_policy_ref
    refute String.contains?(inspect(reqllm_handoff), "secret")
    refute String.contains?(inspect(claude_handoff), "secret")
  end

  test "agent loop refuses provider lower submission without provider authority refs" do
    state = %{
      authority_decision: %{decision: :approved},
      turn_ref: "turn://agent-loop/provider/1",
      runtime_events: [],
      run_ref: "run://agent-loop/provider",
      provider_family: "claude",
      idempotency_key: "idem-provider-missing",
      trace_id: "trace-provider-missing"
    }

    assert {:error, {:missing_required_authority_refs, missing}} =
             AgentLoop.submit_lower_run_activity(state)

    assert :authority_packet_ref in missing
    assert :credential_lease_ref in missing
  end

  defp valid_attrs do
    %{
      system_authorization_ref: "system-authority://tenant-1/decision-1",
      authority_packet_ref: "authority-packet://tenant-1/packet-1",
      provider_family: "claude",
      provider_account_ref: "provider-account://tenant-1/claude/main",
      connector_instance_ref: "connector-instance://tenant-1/claude/default",
      connector_binding_ref: "connector-binding://tenant-1/claude/default",
      credential_handle_ref: "credential-handle://tenant-1/claude/handle-1",
      credential_lease_ref: "credential-lease://tenant-1/claude/lease-1",
      target_ref: "target://tenant-1/local-process/1",
      attach_grant_ref: "attach-grant://tenant-1/local-process/1",
      target_auth_posture_ref: "target-posture://tenant-1/local-process/1",
      boundary_session_id: "boundary-session-1",
      workspace_ref: "workspace://tenant-1/runtime",
      no_egress_posture_ref: "no-egress-posture://tenant-1/deny-external",
      process_target_identity_ref: "process-target-identity://tenant-1/local-process/1",
      stream_target_identity_ref: "stream-target-identity://tenant-1/stdout/1",
      operation_scope_ref: "operation-scope://tenant-1/claude/chat",
      operation_policy_ref: "operation-policy://tenant-1/claude/chat",
      policy_revision_ref: "policy-revision://tenant-1/rev-1",
      idempotency_key: "idem-authority-admission-1",
      trace_id: "trace-authority-admission-1"
    }
  end
end
