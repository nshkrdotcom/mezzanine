defmodule Mezzanine.Archival.OffloadPlanTest do
  use ExUnit.Case, async: true

  alias Mezzanine.Archival.{
    CountdownPolicy,
    Graph,
    Manifest,
    OffloadPlan
  }

  test "terminal graphs produce a countdown plan with a trace-carrying manifest" do
    policy = CountdownPolicy.new!(%{hot_retention_seconds: 7_200})

    graph =
      Graph.new!(%{
        installation_id: "inst-1",
        subject_id: "subject-1",
        trace_ids: ["trace-1"],
        subject_state: :completed,
        execution_states: [:completed],
        terminal_at: ~U[2026-04-15 10:00:00Z],
        execution_ids: ["exec-1"],
        decision_ids: ["decision-1"],
        evidence_ids: ["evidence-1"],
        audit_fact_ids: ["audit-1"]
      })

    assert {:ok, %OffloadPlan{} = plan} = OffloadPlan.build(graph, policy)
    assert plan.due_at == ~U[2026-04-15 12:00:00Z]
    assert %Manifest{trace_ids: ["trace-1"], status: :pending} = plan.manifest

    refute OffloadPlan.hot_deletion_allowed?(plan, ~U[2026-04-15 12:00:01Z])

    completed_plan =
      OffloadPlan.complete_manifest(plan, %{
        storage_uri: "s3://cold-store/subject-1.json",
        checksum: "sha256:abc123",
        completed_at: ~U[2026-04-15 11:00:00Z]
      })

    assert OffloadPlan.hot_deletion_allowed?(completed_plan, ~U[2026-04-15 12:00:01Z])
  end

  test "non-terminal graphs are rejected before archival countdown begins" do
    policy = CountdownPolicy.new!(%{hot_retention_seconds: 60})

    graph =
      Graph.new!(%{
        installation_id: "inst-1",
        subject_id: "subject-1",
        trace_ids: ["trace-1"],
        subject_state: :executing,
        execution_states: [:running],
        terminal_at: ~U[2026-04-15 10:00:00Z]
      })

    assert {:error, {:graph_not_terminal, :subject_state}} = OffloadPlan.build(graph, policy)
  end
end
