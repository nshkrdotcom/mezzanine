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

  test "provider dispatch emits redacted handoff with idempotency lineage" do
    assert {:ok, handoff} = AuthorityAdmission.authorize_provider_dispatch(valid_attrs())

    assert handoff.provider_family == "claude"
    assert handoff.authority_packet_ref == "authority-packet://tenant-1/packet-1"
    assert handoff.idempotency_key == "idem-authority-admission-1"
    assert handoff.handoff_ref == "workflow-authority-handoff://idem-authority-admission-1"
    assert handoff.raw_material_present? == false

    refute inspect(handoff) =~ "secret"
    refute Map.has_key?(handoff, :raw_token)
    refute Map.has_key?(handoff, :provider_payload)
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
      credential_handle_ref: "credential-handle://tenant-1/claude/handle-1",
      credential_lease_ref: "credential-lease://tenant-1/claude/lease-1",
      target_ref: "target://tenant-1/local-process/1",
      attach_grant_ref: "attach-grant://tenant-1/local-process/1",
      operation_policy_ref: "operation-policy://tenant-1/claude/chat",
      policy_revision_ref: "policy-revision://tenant-1/rev-1",
      idempotency_key: "idem-authority-admission-1",
      trace_id: "trace-authority-admission-1"
    }
  end
end
