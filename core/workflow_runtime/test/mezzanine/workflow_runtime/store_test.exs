defmodule Mezzanine.WorkflowRuntime.StoreTest do
  use ExUnit.Case, async: false

  alias Mezzanine.WorkflowRuntime.Store

  test "only the configured Postgres owner store is selectable" do
    assert Store.adapter() == Mezzanine.WorkflowRuntime.Store.Postgres
    assert Store.capabilities().tier == :postgres_shared
    assert Store.capabilities().restart_safe?
  end

  test "production memory selection fails closed" do
    previous = Application.fetch_env!(:mezzanine_core, :run_store)
    Application.put_env(:mezzanine_core, :run_store, Mezzanine.Persistence.MemoryStore)

    on_exit(fn -> Application.put_env(:mezzanine_core, :run_store, previous) end)

    assert_raise RuntimeError, ~r/non-production Mezzanine run store configured/, fn ->
      Store.adapter()
    end
  end
end
