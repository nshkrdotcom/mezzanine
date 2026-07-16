defmodule Mezzanine.PersistenceTest do
  use ExUnit.Case, async: false

  alias Mezzanine.Persistence

  test "durable preflight fails before mutation when capability is unavailable" do
    assert {:error, {:missing_store_capability, :postgres_shared}} =
             Persistence.preflight([profile: :integration_postgres], [])
  end

  test "core application does not supervise a production memory store" do
    assert Process.whereis(Mezzanine.Persistence.MemoryStore) == nil
  end
end
