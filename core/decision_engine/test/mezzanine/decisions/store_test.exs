defmodule Mezzanine.Decisions.StoreTest do
  use ExUnit.Case, async: false

  alias Mezzanine.Decisions.Store

  test "memory adapter is the default decision store" do
    assert Store.adapter([]) == Mezzanine.Decisions.Store.Memory
    assert :ok = Store.preflight([])
    assert Store.capabilities().tier == :memory_ephemeral
  end

  test "postgres decision store fails early without a migration proof" do
    assert Store.adapter(profile: :integration_postgres) == Mezzanine.Decisions.Store.AshPostgres

    assert {:error, {:missing_migration_proof, :decisions}} =
             Store.preflight(profile: :integration_postgres)
  end
end
