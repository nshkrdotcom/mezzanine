defmodule Mezzanine.Archival.PersistenceTest do
  use Mezzanine.Archival.DataCase, async: false

  alias Mezzanine.Archival.ArchivalManifest

  test "stage persists a staging archival manifest with graph membership" do
    subject_id = Ecto.UUID.generate()

    assert {:ok, manifest} =
             ArchivalManifest.stage(%{
               manifest_ref: "archive/inst-1/#{subject_id}/1",
               installation_id: "inst-1",
               subject_id: subject_id,
               trace_ids: ["trace-1"],
               subject_state: "completed",
               execution_states: ["completed"],
               execution_ids: [Ecto.UUID.generate()],
               decision_ids: [Ecto.UUID.generate()],
               evidence_ids: [Ecto.UUID.generate()],
               audit_fact_ids: [Ecto.UUID.generate()],
               projection_names: ["review_queue"],
               terminal_at: ~U[2026-04-16 11:00:00Z],
               due_at: ~U[2026-04-16 13:00:00Z],
               retention_seconds: 7_200,
               storage_kind: "filesystem",
               metadata: %{"source" => "scheduler"}
             })

    assert manifest.status == "staging"
    assert manifest.due_at == ~U[2026-04-16 13:00:00.000000Z]
    assert manifest.projection_names == ["review_queue"]

    assert {:ok, fetched} = ArchivalManifest.by_manifest_ref(manifest.manifest_ref)
    assert fetched.metadata == %{"source" => "scheduler"}
  end

  test "verified, archived, and failed transitions preserve manifest identity" do
    subject_id = Ecto.UUID.generate()

    assert {:ok, manifest} =
             ArchivalManifest.stage(%{
               manifest_ref: "archive/inst-1/#{subject_id}/1",
               installation_id: "inst-1",
               subject_id: subject_id,
               trace_ids: ["trace-1"],
               subject_state: "completed",
               execution_states: ["completed"],
               execution_ids: [],
               decision_ids: [],
               evidence_ids: [],
               audit_fact_ids: [],
               projection_names: [],
               terminal_at: ~U[2026-04-16 11:00:00Z],
               due_at: ~U[2026-04-16 13:00:00Z],
               retention_seconds: 7_200,
               storage_kind: "filesystem",
               metadata: %{}
             })

    assert {:ok, verified} =
             ArchivalManifest.mark_verified(manifest, %{
               storage_uri: "/tmp/archive.json",
               checksum: "sha256:abc123",
               verified_at: ~U[2026-04-16 11:10:00Z],
               metadata: %{"bucket" => "cold-store"}
             })

    assert verified.status == "verified"
    assert verified.verified_at == ~U[2026-04-16 11:10:00.000000Z]

    assert {:ok, archived} =
             ArchivalManifest.mark_archived(verified, %{archived_at: ~U[2026-04-16 11:20:00Z]})

    assert archived.status == "archived"
    assert archived.archived_at == ~U[2026-04-16 11:20:00.000000Z]

    assert {:ok, failed} =
             ArchivalManifest.mark_failed(archived, %{
               reason: "verification retry needed",
               metadata: %{"reason" => "verification retry needed"}
             })

    assert failed.status == "failed"
    assert failed.failure_reason == "verification retry needed"
  end
end
