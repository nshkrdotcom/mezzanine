defmodule Mezzanine.Objects.StoreTest do
  use ExUnit.Case, async: false

  alias Mezzanine.Objects.Store

  test "memory adapter is the default object store" do
    assert Store.adapter([]) == Mezzanine.Objects.Store.Memory
    assert :ok = Store.preflight([])
    assert Store.capabilities().tier == :memory_ephemeral
  end

  test "postgres object store fails early without a migration proof" do
    assert Store.adapter(profile: :integration_postgres) == Mezzanine.Objects.Store.AshPostgres

    assert {:error, {:missing_migration_proof, :objects}} =
             Store.preflight(profile: :integration_postgres)
  end
end
