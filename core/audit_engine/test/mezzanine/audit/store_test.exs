defmodule Mezzanine.Audit.StoreTest do
  use ExUnit.Case, async: false

  alias Mezzanine.Audit.Store

  test "memory adapter is the default audit store" do
    assert Store.adapter([]) == Mezzanine.Audit.Store.Memory
    assert :ok = Store.preflight([])
    assert Store.capabilities().tier == :memory_ephemeral
  end

  test "postgres audit store fails early without a migration proof" do
    assert Store.adapter(profile: :integration_postgres) == Mezzanine.Audit.Store.AshPostgres

    assert {:error, {:missing_migration_proof, :audit}} =
             Store.preflight(profile: :integration_postgres)
  end
end
