defmodule Mezzanine.Execution.StoreTest do
  use Mezzanine.Execution.DataCase, async: false

  alias Mezzanine.Execution.Store
  alias Mezzanine.Execution.Store.Memory

  test "omitted options select the live durable execution store" do
    assert Store.adapter([]) == Mezzanine.Execution.Store.AshPostgres
    assert :ok = Store.preflight([])
    assert Store.capabilities().tier == :postgres_shared
    assert Store.capabilities().restart_safe?
    assert Store.resource_modules() != []

    assert {:ok, health} = Store.health([])
    assert health.adapter == :ash_postgres
    assert health.tier == :postgres_shared
    assert health.restart_safe?
  end

  test "production facade rejects memory profile selection" do
    for profile <- [:mickey_mouse, :memory_debug, "mickey_mouse", "memory_debug"] do
      assert_raise ArgumentError, ~r/production execution store cannot select/, fn ->
        Store.adapter(profile: profile)
      end
    end
  end

  test "explicit test adapter stores execution rows and events" do
    start_supervised!(Mezzanine.Persistence.MemoryStore)
    Memory.reset!()

    assert {:ok, row} = Memory.put_record(%{tenant_id: "tenant-1"}, [])
    assert {:ok, event} = Memory.append_event(row.id, %{state: :queued}, [])
    assert event.sequence == 1
    assert {:ok, stored} = Memory.fetch_record(row.id, [])
    assert stored.events == [event]
  end
end
