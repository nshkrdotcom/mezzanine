defmodule Mezzanine.PersistenceTest do
  use ExUnit.Case, async: false

  alias Mezzanine.Persistence
  alias Mezzanine.Persistence.MemoryStore

  test "default profile resolves to memory without a restart claim" do
    assert {:ok, profile} = Persistence.resolve([])
    assert profile.id == :mickey_mouse
    assert profile.default_tier == :memory_ephemeral
    refute profile.durable?
    refute Persistence.restart_safe?(profile)
  end

  test "durable preflight fails before mutation when capability is unavailable" do
    assert {:error, {:missing_store_capability, :postgres_shared}} =
             Persistence.preflight([profile: :integration_postgres], [])
  end

  test "memory store creates updates and appends refs without durable deps" do
    MemoryStore.reset!(:phase_5_test)

    assert {:ok, row} = MemoryStore.put(:phase_5_test, %{kind: :execution})
    assert {:ok, fetched} = MemoryStore.fetch(:phase_5_test, row.id)
    assert fetched.kind == :execution

    assert {:ok, updated} = MemoryStore.update(:phase_5_test, row.id, %{state: :done})
    assert updated.state == :done

    assert {:ok, event} = MemoryStore.append_event(:phase_5_test, row.id, %{type: :completed})
    assert event.sequence == 1
    assert {:ok, stored} = MemoryStore.fetch(:phase_5_test, row.id)
    assert stored.events == [event]
  end

  test "memory store is supervised and restart-scoped" do
    MemoryStore.reset!(:phase_15_restart_test)

    assert {:ok, row} = MemoryStore.put(:phase_15_restart_test, %{kind: :ephemeral})
    assert {:ok, _stored} = MemoryStore.fetch(:phase_15_restart_test, row.id)

    old_pid = Process.whereis(MemoryStore)
    Process.exit(old_pid, :kill)
    refute Process.alive?(old_pid)

    assert restarted_pid = eventually_registered(MemoryStore)
    assert restarted_pid != old_pid
    assert {:error, :not_found} = MemoryStore.fetch(:phase_15_restart_test, row.id)
  end

  defp eventually_registered(name, attempts \\ 20)
  defp eventually_registered(name, 0), do: Process.whereis(name)

  defp eventually_registered(name, attempts) do
    case Process.whereis(name) do
      nil ->
        Process.sleep(25)
        eventually_registered(name, attempts - 1)

      pid ->
        pid
    end
  end
end
