defmodule Mezzanine.ControlRoom.ForensicReplayTest do
  use ExUnit.Case, async: true

  alias Mezzanine.ControlRoom.ForensicReplay

  test "builds compact forensic replay timelines from incident evidence refs" do
    assert {:ok, replay} =
             ForensicReplay.new(%{
               tenant_ref: "tenant-1",
               installation_ref: "inst-1",
               workspace_ref: "workspace-main",
               project_ref: "project-core",
               environment_ref: "prod",
               principal_ref: "operator:ops-lead",
               resource_ref: "incident/incident-1",
               authority_packet_ref: "authz-packet-1",
               permission_decision_ref: "decision-1",
               idempotency_key: "forensic-replay:incident-1",
               trace_id: "0123456789abcdef0123456789abcdef",
               correlation_id: "corr-replay-1",
               release_manifest_ref: "phase4-v6-milestone17",
               incident_ref: "incident-1",
               timeline_ref: "timeline-1",
               ordered_event_refs: [
                 "command:command-1",
                 "authority:decision-1",
                 "workflow:workflow-1",
                 "activity:activity-1",
                 "lower:lower-run-1",
                 "semantic:semantic-1",
                 "projection:operator-surface-1"
               ],
               integrity_hash: "sha256:#{String.duplicate("b", 64)}",
               missing_ref_set: [],
               replay_result_ref: "replay-result-1",
               evidence_refs: ["incident-bundle:incident-1", "export:incident-export-1"]
             })

    assert replay.contract_name == "Mezzanine.ForensicReplay.v1"
    assert replay.timeline_ref == "timeline-1"
    assert replay.missing_ref_set == []
    assert Enum.at(replay.ordered_event_refs, 0) == "command:command-1"
  end

  test "rejects replay requests without ordered evidence or with raw history leaks" do
    assert {:error, {:missing_required_fields, fields}} =
             ForensicReplay.new(%{
               tenant_ref: "tenant-1",
               installation_ref: "inst-1",
               workspace_ref: "workspace-main",
               project_ref: "project-core",
               environment_ref: "prod",
               principal_ref: "operator:ops-lead",
               resource_ref: "incident/incident-1",
               authority_packet_ref: "authz-packet-1",
               permission_decision_ref: "decision-1",
               idempotency_key: "forensic-replay:incident-1",
               trace_id: "0123456789abcdef0123456789abcdef",
               correlation_id: "corr-replay-1",
               release_manifest_ref: "phase4-v6-milestone17",
               incident_ref: "incident-1",
               timeline_ref: "timeline-1",
               ordered_event_refs: [],
               integrity_hash: "sha256:#{String.duplicate("b", 64)}",
               replay_result_ref: "replay-result-1"
             })

    assert :ordered_event_refs in fields
    assert :missing_ref_set in fields

    assert {:error, {:forbidden_raw_fields, [:raw_workflow_history]}} =
             ForensicReplay.new(valid_attrs(%{raw_workflow_history: [%{event: "payload"}]}))
  end

  defp valid_attrs(overrides) do
    Map.merge(
      %{
        tenant_ref: "tenant-1",
        installation_ref: "inst-1",
        workspace_ref: "workspace-main",
        project_ref: "project-core",
        environment_ref: "prod",
        principal_ref: "operator:ops-lead",
        resource_ref: "incident/incident-1",
        authority_packet_ref: "authz-packet-1",
        permission_decision_ref: "decision-1",
        idempotency_key: "forensic-replay:incident-1",
        trace_id: "0123456789abcdef0123456789abcdef",
        correlation_id: "corr-replay-1",
        release_manifest_ref: "phase4-v6-milestone17",
        incident_ref: "incident-1",
        timeline_ref: "timeline-1",
        ordered_event_refs: ["command:command-1", "workflow:workflow-1"],
        integrity_hash: "sha256:#{String.duplicate("b", 64)}",
        missing_ref_set: [],
        replay_result_ref: "replay-result-1"
      },
      overrides
    )
  end
end
