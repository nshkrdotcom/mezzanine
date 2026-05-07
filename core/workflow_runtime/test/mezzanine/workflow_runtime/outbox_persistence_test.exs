defmodule Mezzanine.WorkflowRuntime.OutboxPersistenceTest do
  use ExUnit.Case, async: false

  alias Mezzanine.WorkflowRuntime.OutboxPersistence
  alias Mezzanine.WorkflowRuntime.OutboxPersistence.Memory

  test "default outbox persistence is memory only" do
    Memory.reset!()

    assert :ok =
             OutboxPersistence.record_start_outcome(
               %{outbox_id: "start-1"},
               %{dispatch_state: "started"}
             )

    assert {:ok, row} = Memory.fetch_start_outcome("start-1")

    assert row.dispatch_state == "started"
  end
end
