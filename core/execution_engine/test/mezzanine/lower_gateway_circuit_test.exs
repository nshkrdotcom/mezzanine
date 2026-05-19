defmodule Mezzanine.LowerGatewayCircuitTest do
  use Mezzanine.Execution.DataCase, async: false

  alias Mezzanine.Execution.Repo
  alias Mezzanine.LowerGatewayCircuit
  alias Mezzanine.LowerGatewayCircuit.CacheInvalidator

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

  test "circuit cache is owned by the supervised invalidator and cleared after owner restart" do
    tenant_id = "tenant-cache-owner"
    installation_id = "inst-cache-owner"
    now = DateTime.utc_now() |> DateTime.truncate(:microsecond)

    assert {:ok, circuit} =
             LowerGatewayCircuit.record_failure(tenant_id, installation_id,
               now: now,
               repo: Repo
             )

    assert circuit.error_count == 1

    assert %{error_count: 1} = LowerGatewayCircuit.fetch(tenant_id, installation_id, repo: Repo)

    cache_key = {:mezzanine_lower_gateway_circuit, tenant_id, installation_id}
    now_ms = System.monotonic_time(:millisecond)
    assert {:ok, %{error_count: 1}} = CacheInvalidator.read(cache_key, now_ms, 1_000)

    old_pid = Process.whereis(CacheInvalidator)
    Process.exit(old_pid, :kill)
    refute Process.alive?(old_pid)

    assert restarted_pid = eventually_registered(CacheInvalidator)
    assert restarted_pid != old_pid
    assert :stale = CacheInvalidator.read(cache_key, now_ms, 1_000)
  end

  test "cache invalidation broadcasts to local pg members and emits success telemetry" do
    ensure_pg_started()
    restart_cache_invalidator()

    telemetry_ids = attach_telemetry([[:mezzanine, :spine, :circuit, :invalidate]])
    cache_key = {:mezzanine_lower_gateway_circuit, "tenant-pg", "inst-pg"}

    member =
      start_supervised!(
        {__MODULE__.ForwardingInvalidationMember, parent: self(), group: CacheInvalidator.group()}
      )

    assert :ok = CacheInvalidator.broadcast(cache_key)

    assert_receive {:forwarded_invalidation, ^member, ^cache_key}

    assert_receive {:telemetry_event, [:mezzanine, :spine, :circuit, :invalidate], %{count: 1},
                    metadata}

    assert metadata.event_name == "spine.circuit.invalidate"
    assert metadata.status == :broadcast
    assert metadata.member_count >= 1
    detach_telemetry(telemetry_ids)
  end

  test "cache invalidation emits no-group telemetry when no members exist" do
    telemetry_ids = attach_telemetry([[:mezzanine, :spine, :circuit, :invalidate]])
    cache_key = {:mezzanine_lower_gateway_circuit, "tenant-no-group", "inst-no-group"}

    assert :ok = CacheInvalidator.broadcast(cache_key, pg: __MODULE__.EmptyPg)

    assert_receive {:telemetry_event, [:mezzanine, :spine, :circuit, :invalidate], %{count: 1},
                    metadata}

    assert metadata.status == :no_group
    assert metadata.member_count == 0
    detach_telemetry(telemetry_ids)
  end

  test "cache invalidation emits failure telemetry when pg broadcast fails" do
    telemetry_ids = attach_telemetry([[:mezzanine, :spine, :circuit, :invalidate]])
    cache_key = {:mezzanine_lower_gateway_circuit, "tenant-failed-pg", "inst-failed-pg"}

    assert :ok = CacheInvalidator.broadcast(cache_key, pg: __MODULE__.FailingPg)

    assert_receive {:telemetry_event, [:mezzanine, :spine, :circuit, :invalidate], %{count: 1},
                    metadata}

    assert metadata.status == :failed
    assert metadata.member_count == 0
    assert metadata.failure_kind == :error
    detach_telemetry(telemetry_ids)
  end

  test "cache epochs reject stale entries after invalidation" do
    cache_key = {:mezzanine_lower_gateway_circuit, "tenant-epoch", "inst-epoch"}
    now_ms = System.monotonic_time(:millisecond)
    circuit = circuit_fixture("tenant-epoch", "inst-epoch")

    assert :ok = CacheInvalidator.write(cache_key, now_ms, circuit)
    assert {:ok, ^circuit} = CacheInvalidator.read(cache_key, now_ms, 1_000)

    assert :ok = CacheInvalidator.invalidate(cache_key)

    :sys.replace_state(CacheInvalidator, fn state ->
      %{state | entries: Map.put(state.entries, cache_key, {now_ms, 0, circuit})}
    end)

    assert :stale = CacheInvalidator.read(cache_key, now_ms, 1_000)
    assert :stale = CacheInvalidator.read(cache_key, now_ms, 1_000)
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

  defp circuit_fixture(tenant_id, installation_id) do
    %{
      tenant_id: tenant_id,
      installation_id: installation_id,
      state: :closed,
      error_count: 0,
      window_started_at: nil,
      opened_at: nil,
      last_probe_at: nil,
      generation: 1
    }
  end

  defp ensure_pg_started do
    case Process.whereis(:pg) do
      nil ->
        {:ok, _pid} = :pg.start_link()
        :ok

      _pid ->
        :ok
    end
  end

  defp restart_cache_invalidator do
    old_pid = Process.whereis(CacheInvalidator)
    Process.exit(old_pid, :kill)
    refute Process.alive?(old_pid)
    assert eventually_registered(CacheInvalidator) != old_pid
    :ok
  end

  defp eventually_registered(name, attempts \\ 20)
  defp eventually_registered(name, 0), do: Process.whereis(name)

  defp eventually_registered(name, attempts) do
    case Process.whereis(name) do
      nil ->
        Process.sleep(25)
        eventually_registered(name, attempts - 1)

      pid ->
        pid
    end
  end

  defmodule ForwardingInvalidationMember do
    use GenServer

    def start_link(opts) do
      GenServer.start_link(__MODULE__, opts)
    end

    @impl true
    def init(opts) do
      parent = Keyword.fetch!(opts, :parent)
      group = Keyword.fetch!(opts, :group)
      :pg.join(group, self())
      {:ok, %{parent: parent}}
    end

    @impl true
    def handle_info({:invalidate_circuit_cache, cache_key}, state) do
      send(state.parent, {:forwarded_invalidation, self(), cache_key})
      {:noreply, state}
    end
  end

  defmodule EmptyPg do
    def get_members(_group), do: []
  end

  defmodule FailingPg do
    def get_members(_group), do: raise("pg unavailable")
  end
end
