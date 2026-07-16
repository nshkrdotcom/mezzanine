defmodule Mezzanine.Archival.StoreTest do
  use Mezzanine.Archival.DataCase, async: false

  alias Mezzanine.Archival.Store

  test "omitted options select the live durable archival store" do
    assert Store.adapter([]) == Mezzanine.Archival.Store.AshPostgres
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
      assert_raise ArgumentError, ~r/production archival store cannot select/, fn ->
        Store.adapter(profile: profile)
      end
    end
  end
end
