defmodule Mezzanine.ControlRoom.IncidentExportBundleTest do
  use ExUnit.Case, async: true

  alias Mezzanine.ControlRoom.IncidentExportBundle

  test "builds redacted release-linked incident export bundles" do
    assert {:ok, bundle} =
             IncidentExportBundle.new(%{
               tenant_ref: "tenant-1",
               installation_ref: "inst-1",
               workspace_ref: "workspace-main",
               project_ref: "project-core",
               environment_ref: "prod",
               principal_ref: "operator:ops-lead",
               resource_ref: "incident/incident-1",
               authority_packet_ref: "authz-packet-1",
               permission_decision_ref: "decision-1",
               idempotency_key: "incident-export:incident-1",
               trace_id: "0123456789abcdef0123456789abcdef",
               correlation_id: "corr-export-1",
               release_manifest_ref: "phase4-v6-milestone8",
               export_ref: "incident-export-1",
               incident_ref: "incident-1",
               included_ref_set: [
                 "command:command-1",
                 "workflow:workflow-1",
                 "projection:operator_signal_projection:7"
               ],
               redaction_manifest_ref: "redaction-manifest-1",
               checksum: "sha256:#{String.duplicate("a", 64)}",
               created_by_operator_ref: "operator:ops-lead",
               artifact_refs: ["artifact:incident-export-1"],
               omitted_field_refs: ["raw_prompt", "raw_provider_body"],
               export_format: :json,
               redaction_status: :redacted
             })

    assert bundle.contract_name == "Mezzanine.IncidentExportBundle.v1"
    assert bundle.redaction_status == :redacted

    assert bundle.included_ref_set == [
             "command:command-1",
             "workflow:workflow-1",
             "projection:operator_signal_projection:7"
           ]
  end

  test "rejects export bundles without evidence, redaction, authority, or trace" do
    assert {:error, {:missing_required_fields, fields}} =
             IncidentExportBundle.new(%{
               tenant_ref: "tenant-1",
               installation_ref: "inst-1",
               principal_ref: "operator:ops-lead",
               resource_ref: "incident/incident-1",
               authority_packet_ref: "authz-packet-1",
               permission_decision_ref: "decision-1",
               idempotency_key: "incident-export:incident-1",
               trace_id: "0123456789abcdef0123456789abcdef",
               correlation_id: "corr-export-1",
               release_manifest_ref: "phase4-v6-milestone8",
               export_ref: "incident-export-1",
               incident_ref: "incident-1",
               included_ref_set: [],
               checksum: "sha256:#{String.duplicate("a", 64)}",
               created_by_operator_ref: "operator:ops-lead",
               redaction_status: :redacted
             })

    assert :included_ref_set in fields
    assert :redaction_manifest_ref in fields

    assert {:error, {:forbidden_raw_fields, [:raw_provider_body]}} =
             IncidentExportBundle.new(
               valid_attrs(%{raw_provider_body: %{provider: "unredacted"}})
             )

    assert {:error, {:invalid_redaction_status, :unreviewed}} =
             IncidentExportBundle.new(valid_attrs(%{redaction_status: :unreviewed}))
  end

  defp valid_attrs(overrides) do
    Map.merge(
      %{
        tenant_ref: "tenant-1",
        installation_ref: "inst-1",
        principal_ref: "operator:ops-lead",
        resource_ref: "incident/incident-1",
        authority_packet_ref: "authz-packet-1",
        permission_decision_ref: "decision-1",
        idempotency_key: "incident-export:incident-1",
        trace_id: "0123456789abcdef0123456789abcdef",
        correlation_id: "corr-export-1",
        release_manifest_ref: "phase4-v6-milestone8",
        export_ref: "incident-export-1",
        incident_ref: "incident-1",
        included_ref_set: ["command:command-1"],
        redaction_manifest_ref: "redaction-manifest-1",
        checksum: "sha256:#{String.duplicate("a", 64)}",
        created_by_operator_ref: "operator:ops-lead",
        artifact_refs: ["artifact:incident-export-1"],
        omitted_field_refs: ["raw_prompt"],
        export_format: :json,
        redaction_status: :redacted
      },
      overrides
    )
  end
end
