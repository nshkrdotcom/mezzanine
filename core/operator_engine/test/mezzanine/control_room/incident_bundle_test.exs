defmodule Mezzanine.ControlRoom.IncidentBundleTest do
  use ExUnit.Case, async: true

  alias Mezzanine.ControlRoom.IncidentBundle

  test "builds release-linked incident bundles with control-room evidence refs" do
    assert {:ok, bundle} =
             IncidentBundle.new(%{
               tenant_ref: "tenant-1",
               installation_ref: "inst-1",
               workspace_ref: "workspace-main",
               project_ref: "project-core",
               environment_ref: "prod",
               principal_ref: "operator:ops-lead",
               resource_ref: "workflow/expense-1",
               authority_packet_ref: "authz-packet-1",
               permission_decision_ref: "decision-1",
               idempotency_key: "incident:expense-1",
               trace_id: "0123456789abcdef0123456789abcdef",
               correlation_id: "corr-incident-1",
               release_manifest_ref: "phase4-v6-milestone3",
               incident_ref: "incident-1",
               command_ref: "command-1",
               workflow_ref: "workflow-1",
               signal_ref: "signal-1",
               activity_ref: "activity-1",
               lower_fact_refs: ["lower-run-1", "lower-attempt-1"],
               semantic_ref: "semantic-1",
               projection_ref: "operator_signal_projection:7",
               staleness_class: :pending_workflow_ack
             })

    assert bundle.contract_name == "Mezzanine.IncidentBundle.v1"
    assert bundle.incident_ref == "incident-1"
    assert bundle.lower_fact_refs == ["lower-run-1", "lower-attempt-1"]
    assert bundle.staleness_class == :pending_workflow_ack
  end

  test "rejects incident bundles without tenant, authority, trace, or staleness evidence" do
    assert {:error, {:missing_required_fields, fields}} =
             IncidentBundle.new(%{
               tenant_ref: "tenant-1",
               installation_ref: "inst-1",
               principal_ref: "operator:ops-lead",
               resource_ref: "workflow/expense-1",
               authority_packet_ref: "authz-packet-1",
               permission_decision_ref: "decision-1",
               idempotency_key: "incident:expense-1",
               trace_id: "0123456789abcdef0123456789abcdef",
               correlation_id: "corr-incident-1",
               release_manifest_ref: "phase4-v6-milestone3",
               incident_ref: "incident-1",
               command_ref: "command-1",
               workflow_ref: "workflow-1",
               signal_ref: "signal-1",
               activity_ref: "activity-1",
               lower_fact_refs: [],
               semantic_ref: "semantic-1",
               projection_ref: "operator_signal_projection:7"
             })

    assert :staleness_class in fields
    assert :lower_fact_refs in fields

    assert {:error, {:invalid_staleness_class, :complete}} =
             IncidentBundle.new(valid_attrs(%{staleness_class: :complete}))
  end

  defp valid_attrs(overrides) do
    Map.merge(
      %{
        tenant_ref: "tenant-1",
        installation_ref: "inst-1",
        principal_ref: "operator:ops-lead",
        resource_ref: "workflow/expense-1",
        authority_packet_ref: "authz-packet-1",
        permission_decision_ref: "decision-1",
        idempotency_key: "incident:expense-1",
        trace_id: "0123456789abcdef0123456789abcdef",
        correlation_id: "corr-incident-1",
        release_manifest_ref: "phase4-v6-milestone3",
        incident_ref: "incident-1",
        command_ref: "command-1",
        workflow_ref: "workflow-1",
        signal_ref: "signal-1",
        activity_ref: "activity-1",
        lower_fact_refs: ["lower-run-1"],
        semantic_ref: "semantic-1",
        projection_ref: "operator_signal_projection:7",
        staleness_class: :pending_workflow_ack
      },
      overrides
    )
  end
end
