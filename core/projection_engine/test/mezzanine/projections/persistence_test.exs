defmodule Mezzanine.Projections.PersistenceTest do
  use Mezzanine.Projections.DataCase, async: false

  alias Mezzanine.Projections.{MaterializedProjection, ProjectionRow}

  test "materialized projections upsert by installation and projection name" do
    assert {:ok, first} =
             MaterializedProjection.upsert(%{
               installation_id: "inst-1",
               projection_name: "ops_dashboard",
               payload: %{"open_count" => 1},
               computed_at: ~U[2026-04-16 10:00:00Z]
             })

    assert {:ok, second} =
             MaterializedProjection.upsert(%{
               installation_id: "inst-1",
               projection_name: "ops_dashboard",
               payload: %{"open_count" => 2},
               computed_at: ~U[2026-04-16 10:05:00Z]
             })

    assert first.id == second.id

    assert {:ok, fetched} =
             MaterializedProjection.by_installation_and_name("inst-1", "ops_dashboard")

    assert fetched.payload == %{"open_count" => 2}
    assert fetched.computed_at == ~U[2026-04-16 10:05:00.000000Z]
  end

  test "projection rows upsert and list in durable sort order" do
    subject_id = Ecto.UUID.generate()

    assert {:ok, first_row} =
             ProjectionRow.upsert(%{
               installation_id: "inst-1",
               projection_name: "review_queue",
               row_key: subject_id,
               subject_id: subject_id,
               projection_kind: "queue",
               sort_key: 20,
               trace_id: "trace-2",
               causation_id: "cause-2",
               payload: %{"state" => "awaiting_decision"},
               computed_at: ~U[2026-04-16 10:01:00Z]
             })

    assert {:ok, second_row} =
             ProjectionRow.upsert(%{
               installation_id: "inst-1",
               projection_name: "review_queue",
               row_key: subject_id,
               subject_id: subject_id,
               projection_kind: "queue",
               sort_key: 5,
               trace_id: "trace-1",
               causation_id: "cause-1",
               payload: %{"state" => "escalated"},
               computed_at: ~U[2026-04-16 10:02:00Z]
             })

    other_subject_id = Ecto.UUID.generate()

    assert {:ok, _third_row} =
             ProjectionRow.upsert(%{
               installation_id: "inst-1",
               projection_name: "review_queue",
               row_key: other_subject_id,
               subject_id: other_subject_id,
               projection_kind: "queue",
               sort_key: 10,
               trace_id: "trace-2",
               causation_id: "cause-2",
               payload: %{"state" => "pending"},
               computed_at: ~U[2026-04-16 10:03:00Z]
             })

    assert first_row.id == second_row.id

    assert {:ok, fetched} = ProjectionRow.row_by_key("inst-1", "review_queue", subject_id)
    assert fetched.sort_key == 5
    assert fetched.trace_id == "trace-1"
    assert fetched.payload == %{"state" => "escalated"}

    assert {:ok, rows} = ProjectionRow.rows_for_projection("inst-1", "review_queue")
    assert Enum.map(rows, & &1.sort_key) == [5, 10]

    assert {:ok, trace_rows} = ProjectionRow.rows_for_trace("inst-1", "trace-2")
    assert Enum.map(trace_rows, & &1.row_key) == [other_subject_id]
  end
end
