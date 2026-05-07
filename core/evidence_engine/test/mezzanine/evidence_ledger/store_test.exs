defmodule Mezzanine.EvidenceLedger.StoreTest do
  use ExUnit.Case, async: false

  alias Mezzanine.EvidenceLedger.Store

  test "memory adapter is the default evidence store" do
    assert Store.adapter([]) == Mezzanine.EvidenceLedger.Store.Memory
    assert :ok = Store.preflight([])
    assert Store.capabilities().tier == :memory_ephemeral
  end

  test "postgres evidence store fails early without a migration proof" do
    assert Store.adapter(profile: :integration_postgres) ==
             Mezzanine.EvidenceLedger.Store.AshPostgres

    assert {:error, {:missing_migration_proof, :evidence}} =
             Store.preflight(profile: :integration_postgres)
  end
end
