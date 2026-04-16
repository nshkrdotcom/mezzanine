defmodule Mezzanine.Audit.PersistenceTest do
  use Mezzanine.Audit.DataCase, async: false

  alias Mezzanine.Audit.{AuditFact, ExecutionLineage, ExecutionLineageStore}

  test "record persists audit facts with operator trace joins" do
    occurred_at = ~U[2026-04-16 01:00:00.000000Z]

    assert {:ok, fact} =
             AuditFact.record(%{
               installation_id: "inst-1",
               subject_id: "subject-1",
               execution_id: "exec-1",
               fact_kind: :execution_completed,
               actor_ref: %{kind: :system},
               payload: %{result_summary: %{status: "ok"}},
               trace_id: "trace-1",
               causation_id: "cause-1",
               occurred_at: occurred_at
             })

    assert fact.trace_id == "trace-1"
    assert fact.causation_id == "cause-1"
    assert fact.fact_kind == :execution_completed

    assert {:ok, [reloaded]} = AuditFact.list_trace("inst-1", "trace-1")
    assert reloaded.id == fact.id
    assert reloaded.occurred_at == occurred_at

    assert has_index?("audit_facts", ["installation_id", "trace_id", "occurred_at"])
    assert has_index?("audit_facts", ["causation_id"])
  end

  test "execution lineage store upserts stable bridge linkage by execution id" do
    initial_lineage =
      ExecutionLineage.new!(%{
        trace_id: "trace-1",
        causation_id: "cause-1",
        installation_id: "inst-1",
        subject_id: "subject-1",
        execution_id: "exec-1",
        citadel_submission_id: "citadel-sub-1",
        ji_submission_key: "ji-sub-1"
      })

    assert {:ok, stored_lineage} = ExecutionLineageStore.store(initial_lineage)
    assert stored_lineage.execution_id == "exec-1"

    updated_lineage =
      ExecutionLineage.new!(%{
        trace_id: "trace-1",
        causation_id: "cause-1",
        installation_id: "inst-1",
        subject_id: "subject-1",
        execution_id: "exec-1",
        citadel_submission_id: "citadel-sub-1",
        ji_submission_key: "ji-sub-1",
        lower_run_id: "run-1",
        lower_attempt_id: "attempt-1",
        artifact_refs: ["artifact-1"]
      })

    assert {:ok, persisted_lineage} = ExecutionLineageStore.store(updated_lineage)
    assert persisted_lineage.lower_run_id == "run-1"

    assert {:ok, fetched_lineage} = ExecutionLineageStore.fetch("exec-1")

    assert %{
             installation_id: "inst-1",
             subject_id: "subject-1",
             execution_id: "exec-1",
             trace_id: "trace-1"
           } == ExecutionLineage.public_lookup(fetched_lineage)

    assert %{
             citadel_submission_id: "citadel-sub-1",
             ji_submission_key: "ji-sub-1",
             lower_run_id: "run-1",
             lower_attempt_id: "attempt-1",
             artifact_refs: ["artifact-1"]
           } == ExecutionLineage.lower_identifiers(fetched_lineage)

    assert {:ok, [trace_lineage]} = ExecutionLineageStore.list_trace("inst-1", "trace-1")
    assert trace_lineage.execution_id == "exec-1"

    assert has_index?("execution_lineage_records", ["execution_id"])
    assert has_index?("execution_lineage_records", ["installation_id", "trace_id"])
  end

  defp has_index?(table_name, columns) when is_binary(table_name) and is_list(columns) do
    columns_sql = Enum.join(columns, ", ")

    Repo.query!(
      """
      SELECT indexdef
      FROM pg_indexes
      WHERE schemaname = current_schema()
        AND tablename = $1
      """,
      [table_name]
    ).rows
    |> Enum.any?(fn [indexdef] ->
      String.contains?(indexdef, "(#{columns_sql})")
    end)
  end
end
