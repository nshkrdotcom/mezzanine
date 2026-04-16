defmodule Mezzanine.Archival.PersistenceTest do
  use Mezzanine.Archival.DataCase, async: false

  alias Mezzanine.Archival.{ArchivalManifest, CountdownPolicy, Graph}

  test "plan_from_graph persists a pending archival manifest with graph members" do
    subject_id = Ecto.UUID.generate()
    execution_id = Ecto.UUID.generate()
    decision_id = Ecto.UUID.generate()
    evidence_id = Ecto.UUID.generate()
    audit_fact_id = Ecto.UUID.generate()

    graph =
      Graph.new!(%{
        installation_id: "inst-1",
        subject_id: subject_id,
        trace_ids: ["trace-1"],
        subject_state: :completed,
        execution_states: [:completed],
        terminal_at: ~U[2026-04-16 11:00:00Z],
        execution_ids: [execution_id],
        decision_ids: [decision_id],
        evidence_ids: [evidence_id],
        audit_fact_ids: [audit_fact_id]
      })

    policy = CountdownPolicy.new!(%{hot_retention_seconds: 7_200})

    assert {:ok, manifest} =
             ArchivalManifest.plan_from_graph(graph, policy, %{
               projection_names: ["review_queue"],
               metadata: %{"source" => "scheduler"}
             })

    assert manifest.status == "pending"
    assert manifest.due_at == ~U[2026-04-16 13:00:00.000000Z]
    assert manifest.execution_ids == [execution_id]
    assert manifest.projection_names == ["review_queue"]

    assert {:ok, fetched} = ArchivalManifest.by_manifest_ref(manifest.manifest_ref)
    assert fetched.metadata == %{"source" => "scheduler"}
  end

  test "completed manifests carry cold-storage metadata and unlock hot deletion only after due time" do
    graph =
      Graph.new!(%{
        installation_id: "inst-1",
        subject_id: Ecto.UUID.generate(),
        trace_ids: ["trace-1"],
        subject_state: :completed,
        execution_states: [:completed],
        terminal_at: ~U[2026-04-16 11:00:00Z]
      })

    policy = CountdownPolicy.new!(%{hot_retention_seconds: 60})

    assert {:ok, manifest} = ArchivalManifest.plan_from_graph(graph, policy)

    refute ArchivalManifest.hot_deletion_allowed?(manifest, ~U[2026-04-16 11:00:30Z])

    assert {:ok, completed} =
             ArchivalManifest.complete(manifest, %{
               storage_uri: "s3://cold-store/archive.json",
               checksum: "sha256:abc123",
               completed_at: ~U[2026-04-16 11:00:40Z],
               metadata: %{"bucket" => "cold-store"}
             })

    refute ArchivalManifest.hot_deletion_allowed?(completed, ~U[2026-04-16 11:00:59Z])
    assert ArchivalManifest.hot_deletion_allowed?(completed, ~U[2026-04-16 11:01:01Z])
  end

  test "due_for_installation returns only pending manifests whose due_at has elapsed" do
    due_graph =
      Graph.new!(%{
        installation_id: "inst-1",
        subject_id: Ecto.UUID.generate(),
        trace_ids: ["trace-due"],
        subject_state: :completed,
        execution_states: [:completed],
        terminal_at: ~U[2026-04-16 09:00:00Z]
      })

    future_graph =
      Graph.new!(%{
        installation_id: "inst-1",
        subject_id: Ecto.UUID.generate(),
        trace_ids: ["trace-future"],
        subject_state: :completed,
        execution_states: [:completed],
        terminal_at: ~U[2026-04-16 11:59:30Z]
      })

    policy = CountdownPolicy.new!(%{hot_retention_seconds: 60})

    assert {:ok, due_manifest} = ArchivalManifest.plan_from_graph(due_graph, policy)
    assert {:ok, _future_manifest} = ArchivalManifest.plan_from_graph(future_graph, policy)

    assert {:ok, due_manifests} =
             ArchivalManifest.due_for_installation("inst-1", ~U[2026-04-16 12:00:00Z])

    assert Enum.map(due_manifests, & &1.manifest_ref) == [due_manifest.manifest_ref]
  end
end
