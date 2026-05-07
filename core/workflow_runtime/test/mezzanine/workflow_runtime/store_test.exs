defmodule Mezzanine.WorkflowRuntime.StoreTest do
  use ExUnit.Case, async: false

  alias Mezzanine.WorkflowRuntime.Store

  test "memory adapter is the default workflow runtime store" do
    assert Store.adapter([]) == Mezzanine.WorkflowRuntime.Store.Memory
    assert :ok = Store.preflight([])
    assert Store.capabilities().tier == :memory_ephemeral
  end

  test "postgres workflow runtime store fails early without a migration proof" do
    assert Store.adapter(profile: :integration_postgres) ==
             Mezzanine.WorkflowRuntime.Store.Postgres

    assert {:error, {:missing_migration_proof, :workflow_runtime}} =
             Store.preflight(profile: :integration_postgres)
  end
end
