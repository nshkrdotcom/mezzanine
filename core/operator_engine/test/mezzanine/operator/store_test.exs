defmodule Mezzanine.Operator.StoreTest do
  use ExUnit.Case, async: false

  alias Mezzanine.Operator.Store

  test "memory adapter is the default operator store" do
    assert Store.adapter([]) == Mezzanine.Operator.Store.Memory
    assert :ok = Store.preflight([])
    assert Store.capabilities().tier == :memory_ephemeral
  end

  test "postgres operator store fails early without a migration proof" do
    assert Store.adapter(profile: :integration_postgres) == Mezzanine.Operator.Store.AshPostgres

    assert {:error, {:missing_migration_proof, :operator}} =
             Store.preflight(profile: :integration_postgres)
  end
end
