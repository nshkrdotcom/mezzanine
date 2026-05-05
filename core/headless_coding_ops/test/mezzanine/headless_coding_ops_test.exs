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
end
