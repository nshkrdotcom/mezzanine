defmodule Mezzanine.Execution.StoreTest do
  use ExUnit.Case, async: false

  alias Mezzanine.Execution.Store
  alias Mezzanine.Execution.Store.Memory

  test "memory adapter is the default and does not expose restart durability" do
    assert Store.adapter([]) == Mezzanine.Execution.Store.Memory
    assert :ok = Store.preflight([])
    assert Store.capabilities().tier == :memory_ephemeral
    refute Store.capabilities().restart_safe?
  end

  test "postgres adapter fails early without a migration proof" do
    assert Store.adapter(profile: :integration_postgres) == Mezzanine.Execution.Store.AshPostgres

    assert {:error, {:missing_migration_proof, :execution}} =
             Store.preflight(profile: :integration_postgres)
  end

  test "memory adapter stores execution rows and events" do
    Memory.reset!()

    assert {:ok, row} = Store.put_record(%{tenant_id: "tenant-1"}, [])
    assert {:ok, event} = Store.append_event(row.id, %{state: :queued}, [])
    assert event.sequence == 1
    assert {:ok, stored} = Store.fetch_record(row.id, [])
    assert stored.events == [event]
  end
end
