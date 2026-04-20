defmodule Mezzanine.Decisions.DecisionCommandsBoundaryTest do
  use ExUnit.Case, async: true

  @source_path Path.expand("../../../lib/mezzanine/decision_commands.ex", __DIR__)
  @forbidden_sql_tokens [
    "@insert_decision_sql",
    "@load_decision_sql",
    "@read_decision_sql",
    "@read_decision_by_identity_sql",
    "@update_decision_resolution_sql",
    "@insert_audit_fact_sql",
    "SQL.query"
  ]

  test "decision commands do not own raw SQL for decision or audit rows" do
    source = File.read!(@source_path)

    refute source =~ "Ecto.Adapters.SQL"
    refute source =~ "Mezzanine.Execution.Repo"

    for token <- @forbidden_sql_tokens do
      refute source =~ token
    end
  end
end
