defmodule Mezzanine.Projections.StoreTest do
  use ExUnit.Case, async: false

  alias Mezzanine.Projections.Store

  test "memory adapter is the default projection store" do
    assert Store.adapter([]) == Mezzanine.Projections.Store.Memory
    assert :ok = Store.preflight([])
    assert Store.capabilities().tier == :memory_ephemeral
  end

  test "postgres projection store fails early without a migration proof" do
    assert Store.adapter(profile: :integration_postgres) ==
             Mezzanine.Projections.Store.AshPostgres

    assert {:error, {:missing_migration_proof, :projections}} =
             Store.preflight(profile: :integration_postgres)
  end
end
