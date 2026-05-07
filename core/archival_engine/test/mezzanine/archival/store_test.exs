defmodule Mezzanine.Archival.StoreTest do
  use ExUnit.Case, async: false

  alias Mezzanine.Archival.Store

  test "memory adapter is the default archival store" do
    assert Store.adapter([]) == Mezzanine.Archival.Store.Memory
    assert :ok = Store.preflight([])
    assert Store.capabilities().tier == :memory_ephemeral
  end

  test "postgres archival store fails early without a migration proof" do
    assert Store.adapter(profile: :integration_postgres) == Mezzanine.Archival.Store.AshPostgres

    assert {:error, {:missing_migration_proof, :archival}} =
             Store.preflight(profile: :integration_postgres)
  end
end
