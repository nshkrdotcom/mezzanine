defmodule Mezzanine.IntegrationBridgeTest.LazyLowerFactsStub do
  @operations [:fetch_run]

  def operation_supported?(operation), do: operation in @operations

  def fetch_run(%Jido.Integration.V2.TenantScope{} = scope, run_id) do
    send(
      Process.get(:integration_bridge_test_pid),
      {:lazy_lower_facts_fetch_run, [scope, run_id]}
    )

    {:ok, %{run_id: run_id, status: :completed}}
  end
end
