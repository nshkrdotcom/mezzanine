defmodule Mezzanine.Archival.ReleaseContractsTest do
  use ExUnit.Case, async: true

  alias Mezzanine.Archival.ArchivalConflict
  alias Mezzanine.Archival.ArchivalSweep
  alias Mezzanine.Archival.ColdRestoreArtifactQuery
  alias Mezzanine.Archival.ColdRestoreTraceQuery

  test "builds cold restore trace query contracts" do
    assert {:ok, query} =
             ColdRestoreTraceQuery.new(
               base_attrs()
               |> Map.merge(%{
                 restore_request_ref: "restore-request:trace:1",
                 archive_partition_ref: "archive-partition:tenant-1:2026-04",
                 hot_index_ref: "hot-index:trace-archive-1",
                 cold_object_ref: "cold-object:archive/inst-1/subject-1/1",
                 restore_consistency_hash:
                   "sha256:8f434346648f6b96df89dda901c5176b10a6d83961dd3c1ac88b59b2dc327aa4"
               })
             )

    assert query.contract_name == "Mezzanine.ColdRestoreTraceQuery.v1"
    assert query.trace_id == "trace:m12:060"
    assert query.restore_consistency_hash =~ "sha256:"
  end

  test "builds cold restore artifact query contracts" do
    assert {:ok, query} =
             ColdRestoreArtifactQuery.new(
               base_attrs()
               |> Map.merge(%{
                 artifact_id: "artifact-123",
                 artifact_kind: "run_log",
                 artifact_hash:
                   "sha256:ed7002b439e9ac845f2233ce2e61e5b32c02fb0722d3cc9045a91b46f41c1590",
                 lineage_ref: "lineage:artifact-123",
                 archive_object_ref: "cold-object:archive/inst-1/artifact-123",
                 restore_validation_ref: "restore-validation:artifact-123"
               })
             )

    assert query.contract_name == "Mezzanine.ColdRestoreArtifactQuery.v1"
    assert query.artifact_id == "artifact-123"
    assert query.artifact_hash =~ "sha256:"
  end

  test "builds deterministic hot cold conflict contracts" do
    assert {:ok, conflict} =
             ArchivalConflict.new(
               base_attrs()
               |> Map.merge(%{
                 conflict_ref: "archival-conflict:trace:m12:062",
                 hot_hash:
                   "sha256:ab0b934789acee88a3a39b141f9a0602f075cb5403b7cce210c3acdac0d5686d",
                 cold_hash:
                   "sha256:ed7002b439e9ac845f2233ce2e61e5b32c02fb0722d3cc9045a91b46f41c1590",
                 precedence_rule: :quarantine_until_operator_resolution,
                 quarantine_ref: "quarantine:archive-conflict:1",
                 resolution_action_ref: "operator-action:resolve-archive-conflict:1"
               })
             )

    assert conflict.contract_name == "Mezzanine.ArchivalConflict.v1"
    assert conflict.precedence_rule == :quarantine_until_operator_resolution
  end

  test "builds archival sweep retry quarantine contracts" do
    assert {:ok, sweep} =
             ArchivalSweep.new(
               base_attrs()
               |> Map.merge(%{
                 sweep_ref: "archival-sweep:tenant-1:2026-04-19T12:00:00Z",
                 artifact_ref: "artifact-123",
                 retry_count: 3,
                 retry_policy_ref: "retry-policy:archive-sweep:v1",
                 quarantine_ref: "quarantine:archive-sweep:artifact-123",
                 next_retry_at: ~U[2026-04-19 12:30:00Z]
               })
             )

    assert sweep.contract_name == "Mezzanine.ArchivalSweep.v1"
    assert sweep.retry_count == 3
    assert sweep.next_retry_at == ~U[2026-04-19 12:30:00Z]
  end

  test "rejects missing actor, same hash conflicts, and invalid retry counts" do
    assert {:error, {:missing_required_fields, fields}} =
             ColdRestoreTraceQuery.new(base_attrs() |> Map.delete(:system_actor_ref))

    assert :principal_ref_or_system_actor_ref in fields

    same_hash = "sha256:ab0b934789acee88a3a39b141f9a0602f075cb5403b7cce210c3acdac0d5686d"

    assert {:error, :invalid_archival_conflict} =
             ArchivalConflict.new(
               base_attrs()
               |> Map.merge(%{
                 conflict_ref: "archival-conflict:trace:m12:062",
                 hot_hash: same_hash,
                 cold_hash: same_hash,
                 precedence_rule: :hot_authoritative,
                 quarantine_ref: "quarantine:archive-conflict:1",
                 resolution_action_ref: "operator-action:resolve-archive-conflict:1"
               })
             )

    assert {:error, :invalid_archival_sweep} =
             ArchivalSweep.new(
               base_attrs()
               |> Map.merge(%{
                 sweep_ref: "archival-sweep:tenant-1:2026-04-19T12:00:00Z",
                 artifact_ref: "artifact-123",
                 retry_count: -1,
                 retry_policy_ref: "retry-policy:archive-sweep:v1",
                 quarantine_ref: "quarantine:archive-sweep:artifact-123",
                 next_retry_at: ~U[2026-04-19 12:30:00Z]
               })
             )
  end

  defp base_attrs do
    %{
      tenant_ref: "tenant-1",
      installation_ref: "inst-1",
      workspace_ref: "workspace-main",
      project_ref: "project-core",
      environment_ref: "prod",
      system_actor_ref: "system:archival-restore",
      resource_ref: "archive:inst-1:subject-1",
      authority_packet_ref: "authz-packet-archive-restore",
      permission_decision_ref: "decision-archive-restore",
      idempotency_key: "archive-restore:trace:m12:060",
      trace_id: "trace:m12:060",
      correlation_id: "corr-archive-restore",
      release_manifest_ref: "phase4-v6-milestone12"
    }
  end
end
