defmodule Mezzanine.LowerGatewayCircuitTest do
  use Mezzanine.Execution.DataCase, async: false

  alias Mezzanine.Execution.Repo
  alias Mezzanine.LowerGatewayCircuit

  test "persisted circuit timestamps round-trip as DateTime values" do
    telemetry_ids = attach_telemetry([[:mezzanine, :spine, :circuit, :open]])
    tenant_id = "tenant-circuit"
    installation_id = "inst-circuit"
    first_failure_at = DateTime.utc_now() |> DateTime.truncate(:microsecond)

    assert {:ok, first_circuit} =
             LowerGatewayCircuit.record_failure(tenant_id, installation_id,
               now: first_failure_at,
               repo: Repo
             )

    assert %DateTime{} = first_circuit.window_started_at

    fetched_circuit = LowerGatewayCircuit.fetch(tenant_id, installation_id, repo: Repo)

    assert %DateTime{} = fetched_circuit.window_started_at
    assert fetched_circuit.window_started_at == first_circuit.window_started_at

    assert {:ok, next_circuit} =
             LowerGatewayCircuit.record_failure(tenant_id, installation_id,
               now: DateTime.add(first_failure_at, 1, :second),
               repo: Repo
             )

    assert %DateTime{} = next_circuit.window_started_at
    assert next_circuit.error_count == 2

    Enum.reduce(3..5, next_circuit, fn offset, _acc ->
      {:ok, circuit} =
        LowerGatewayCircuit.record_failure(tenant_id, installation_id,
          now: DateTime.add(first_failure_at, offset, :second),
          repo: Repo
        )

      circuit
    end)

    assert_receive {:telemetry_event, [:mezzanine, :spine, :circuit, :open], %{count: 1},
                    metadata}

    assert metadata.event_name == "spine.circuit.open"
    assert metadata.tenant_id == tenant_id
    assert metadata.installation_id == installation_id
    detach_telemetry(telemetry_ids)
  end

  test "half-open followers use the short probe backoff instead of the open-circuit minimum" do
    telemetry_ids = attach_telemetry([[:mezzanine, :spine, :circuit, :snooze]])
    tenant_id = "tenant-half-open"
    installation_id = "inst-half-open"
    base_time = DateTime.utc_now() |> DateTime.truncate(:microsecond)

    open_circuit =
      Enum.reduce(1..5, nil, fn offset, _acc ->
        {:ok, circuit} =
          LowerGatewayCircuit.record_failure(tenant_id, installation_id,
            now: DateTime.add(base_time, offset, :second),
            repo: Repo
          )

        circuit
      end)

    probe_time = DateTime.add(open_circuit.opened_at, 31, :second)

    Repo.query!(
      """
      UPDATE lower_gateway_circuits
      SET state = 'half_open',
          last_probe_at = $3,
          updated_at = $3
      WHERE tenant_id = $1
        AND installation_id = $2
      """,
      [tenant_id, installation_id, probe_time]
    )

    assert {:snooze, 250} =
             LowerGatewayCircuit.permit(tenant_id, installation_id,
               now: probe_time,
               probe_owner: "scheduler-node-b",
               allow_probe_without_runtime_lease?: false,
               jitter_ms: 0,
               repo: Repo
             )

    assert_receive {:telemetry_event, [:mezzanine, :spine, :circuit, :snooze],
                    %{count: 1, snooze_ms: 250}, metadata}

    assert metadata.event_name == "spine.circuit.snooze"
    assert metadata.circuit_state == :half_open
    assert metadata.installation_id == installation_id
    detach_telemetry(telemetry_ids)
  end

  defp attach_telemetry(events) do
    Enum.map(events, fn event ->
      handler_id = {__MODULE__, make_ref(), event}
      :ok = :telemetry.attach(handler_id, event, &__MODULE__.handle_telemetry_event/4, self())
      handler_id
    end)
  end

  defp detach_telemetry(handler_ids) do
    Enum.each(handler_ids, &:telemetry.detach/1)
  end

  def handle_telemetry_event(event, measurements, metadata, pid) do
    send(pid, {:telemetry_event, event, measurements, metadata})
  end
end
