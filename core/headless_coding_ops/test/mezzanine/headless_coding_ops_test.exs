defmodule Mezzanine.HeadlessCodingOpsTest do
  use ExUnit.Case, async: true

  alias Mezzanine.HeadlessCodingOps

  test "accepts headless intake with provider, target, session, and authority refs" do
    assert {:ok, work_item} = HeadlessCodingOps.intake(valid_intake())

    assert work_item.status == :accepted
    assert work_item.provider_selection_ref == "provider-selection://tenant-1/claude/main"
    assert work_item.target_selection_ref == "target-selection://tenant-1/local-process"
    assert work_item.session_ref == "session://tenant-1/headless/1"
    assert work_item.receipt_ref == "headless-coding-ops-receipt://tenant-1/idempotency-1"
    refute inspect(work_item) =~ "secret"
  end

  test "rejects raw credential material in headless intake" do
    assert {:error, {:forbidden_headless_material, forbidden}} =
             valid_intake()
             |> Map.put(:raw_token, "secret")
             |> Map.put(:target_credentials, %{"token" => "secret"})
             |> HeadlessCodingOps.intake()

    assert forbidden == [:raw_token, :target_credentials]
  end

  test "bounds provider-neutral readback states" do
    assert {:ok, state} =
             HeadlessCodingOps.readback_state(%{
               work_item_ref: "work-item://tenant-1/headless/1",
               state: "auth_required",
               authority_refs: ["credential-lease://tenant-1/claude/lease-1"],
               receipt_ref: "receipt://tenant-1/headless/1"
             })

    assert state.state == :auth_required

    assert {:error, {:invalid_headless_state, :provider_secret_dumped}} =
             HeadlessCodingOps.readback_state(%{
               work_item_ref: "work-item://tenant-1/headless/1",
               state: :provider_secret_dumped,
               receipt_ref: "receipt://tenant-1/headless/1"
             })
  end

  test "operator controls are bounded and authority-ref only" do
    required_authority_actions = [
      :revoke_authority,
      :rotate_authority,
      :renew_authority,
      :rebind_authority,
      :detach_authority,
      :transfer_authority,
      :inspect_authority,
      :invalidate_authority
    ]

    assert Enum.all?(required_authority_actions, &(&1 in HeadlessCodingOps.actions()))

    Enum.each(required_authority_actions, fn action ->
      assert {:ok, %HeadlessCodingOps.OperatorCommand{action: ^action}} =
               HeadlessCodingOps.operator_action(%{
                 action: action,
                 actor_ref: "actor://tenant-1/operator/1",
                 work_item_ref: "work-item://tenant-1/headless/1",
                 authority_refs: ["authority://tenant-1/#{action}/1"],
                 idempotency_key: "operator-#{action}"
               })
    end)

    assert {:ok, command} =
             HeadlessCodingOps.operator_action(%{
               action: :rotate_lease,
               actor_ref: "actor://tenant-1/operator/1",
               work_item_ref: "work-item://tenant-1/headless/1",
               authority_refs: ["credential-lease://tenant-1/claude/lease-1"],
               idempotency_key: "operator-1"
             })

    assert command.action == :rotate_lease

    assert {:error, {:invalid_operator_action, :dump_token}} =
             HeadlessCodingOps.operator_action(%{
               action: :dump_token,
               actor_ref: "actor://tenant-1/operator/1",
               work_item_ref: "work-item://tenant-1/headless/1",
               idempotency_key: "operator-2"
             })
  end

  test "headless handoff resume preserves refs and revalidates single active execution" do
    assert {:ok, resume} = HeadlessCodingOps.resume_handoff(valid_resume())

    assert resume.handoff_ref == "asm-handoff://tenant-1/headless/1"
    assert resume.session_ref == "session://tenant-1/headless/1"
    assert resume.provider_account_ref == "provider-account://tenant-1/claude/main"
    assert resume.connector_binding_ref == "connector-binding://tenant-1/claude/default"
    assert resume.credential_handle_ref == "credential-handle://tenant-1/claude/main"
    assert resume.credential_lease_ref == "credential-lease://tenant-1/claude/lease-1"
    assert resume.native_auth_assertion_ref == "native-auth://tenant-1/claude/main"
    assert resume.target_ref == "target://tenant-1/local-process"
    assert resume.attach_grant_ref == "attach-grant://tenant-1/local-process"
    assert resume.operation_policy_ref == "operation-policy://tenant-1/claude/coding"
    assert resume.trace_ref == "trace://tenant-1/headless/1"
    assert resume.idempotency_key == "resume-idempotency-1"
    assert resume.restart_event == :workflow_resume
    assert resume.redacted?
    refute inspect(resume) =~ "secret"

    assert {:error, {:duplicate_active_execution_after_restart, details}} =
             valid_resume()
             |> Map.put(:current_execution_ref, "execution://tenant-1/headless/other")
             |> HeadlessCodingOps.resume_handoff()

    assert details.redacted?
  end

  test "headless handoff resume rejects raw material and unsupported restart events" do
    assert {:error, {:forbidden_headless_material, [:raw_token]}} =
             valid_resume()
             |> Map.put(:raw_token, "secret")
             |> HeadlessCodingOps.resume_handoff()

    assert {:error, {:unsupported_restart_event, :dump_previous_token}} =
             valid_resume()
             |> Map.put(:restart_event, :dump_previous_token)
             |> HeadlessCodingOps.resume_handoff()
  end

  defp valid_intake do
    %{
      tenant_ref: "tenant://tenant-1",
      request_ref: "agent-intake://tenant-1/request-1",
      session_ref: "session://tenant-1/headless/1",
      provider_selection_ref: "provider-selection://tenant-1/claude/main",
      target_selection_ref: "target-selection://tenant-1/local-process",
      provider_account_ref: "provider-account://tenant-1/claude/main",
      connector_binding_ref: "connector-binding://tenant-1/claude/default",
      credential_lease_ref: "credential-lease://tenant-1/claude/lease-1",
      target_ref: "target://tenant-1/local-process",
      operation_policy_ref: "operation-policy://tenant-1/claude/coding",
      idempotency_key: "idempotency-1",
      trace_ref: "trace://tenant-1/headless/1"
    }
  end

  defp valid_resume do
    %{
      handoff_ref: "asm-handoff://tenant-1/headless/1",
      tenant_ref: "tenant://tenant-1",
      session_ref: "session://tenant-1/headless/1",
      work_item_ref: "work-item://tenant-1/headless/1",
      provider_account_ref: "provider-account://tenant-1/claude/main",
      connector_binding_ref: "connector-binding://tenant-1/claude/default",
      credential_handle_ref: "credential-handle://tenant-1/claude/main",
      credential_lease_ref: "credential-lease://tenant-1/claude/lease-1",
      native_auth_assertion_ref: "native-auth://tenant-1/claude/main",
      target_ref: "target://tenant-1/local-process",
      attach_grant_ref: "attach-grant://tenant-1/local-process",
      operation_policy_ref: "operation-policy://tenant-1/claude/coding",
      trace_ref: "trace://tenant-1/headless/1",
      idempotency_key: "resume-idempotency-1",
      active_execution_ref: "execution://tenant-1/headless/active",
      current_execution_ref: "execution://tenant-1/headless/active",
      restart_event: "workflow_resume"
    }
  end
end
