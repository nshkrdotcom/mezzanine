defmodule Mezzanine.ControlRoom.EvidenceAuditContractsTest do
  use ExUnit.Case, async: true

  alias Mezzanine.ControlRoom.SuppressionVisibility

  test "accepts operator-visible suppression evidence" do
    assert {:ok, visibility} = SuppressionVisibility.new(base_visibility())

    assert visibility.contract_name == "Platform.SuppressionVisibility.v1"
    assert visibility.operator_visibility == "visible"
    assert visibility.recovery_action_refs == ["recovery-action:m13:072"]
  end

  test "fails closed on hidden suppression or missing recovery posture" do
    assert {:error, :invalid_suppression_visibility} =
             SuppressionVisibility.new(%{base_visibility() | operator_visibility: "hidden"})

    assert {:error, {:missing_required_fields, [:recovery_action_refs]}} =
             SuppressionVisibility.new(%{base_visibility() | recovery_action_refs: []})
  end

  test "requires tenant, actor, authority, trace, and release refs" do
    assert {:error, {:missing_required_fields, fields}} =
             SuppressionVisibility.new(%{
               base_visibility()
               | tenant_ref: nil,
                 principal_ref: nil,
                 system_actor_ref: nil,
                 authority_packet_ref: nil,
                 trace_id: nil,
                 release_manifest_ref: nil
             })

    assert :tenant_ref in fields
    assert :principal_ref_or_system_actor_ref in fields
    assert :authority_packet_ref in fields
    assert :trace_id in fields
    assert :release_manifest_ref in fields
  end

  defp base_visibility do
    %{
      tenant_ref: "tenant:acme",
      installation_ref: "installation:acme",
      workspace_ref: "workspace:core",
      project_ref: "project:ops",
      environment_ref: "prod",
      principal_ref: "principal:operator-1",
      system_actor_ref: nil,
      resource_ref: "suppression://semantic/072",
      authority_packet_ref: "authority-packet:m13:072",
      permission_decision_ref: "permission-decision:m13:072",
      idempotency_key: "suppression-visibility:m13:072",
      trace_id: "trace:m13:072",
      correlation_id: "correlation:m13:072",
      release_manifest_ref: "phase4-v6-milestone13",
      suppression_ref: "suppression://semantic/072",
      suppression_kind: "duplicate",
      reason_code: "semantic_duplicate",
      target_ref: "semantic://candidate/072",
      operator_visibility: "visible",
      recovery_action_refs: ["recovery-action:m13:072"],
      diagnostics_ref: "diagnostics://suppression/072"
    }
  end
end
