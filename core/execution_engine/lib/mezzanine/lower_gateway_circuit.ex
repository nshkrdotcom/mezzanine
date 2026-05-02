defmodule Mezzanine.LowerGatewayCircuit do
  @moduledoc """
  Cluster-observable lower-gateway circuit state for dispatch and reconcile work.
  """

  alias Ecto.Adapters.SQL
  alias Mezzanine.Execution.Repo
  alias Mezzanine.LowerGatewayCircuit.CacheInvalidator
  alias Mezzanine.Telemetry

  @states [:closed, :open, :half_open]
  @state_lookup Map.new(@states, &{Atom.to_string(&1), &1})
  @cache_ttl_ms 1_000
  @failure_threshold 5
  @failure_window_seconds 30
  @open_cooldown_ms 30_000
  @max_snooze_ms 30_000
  @min_snooze_ms 1_000
  @half_open_follower_snooze_ms 250

  @type state :: :closed | :open | :half_open

  @type t :: %{
          tenant_id: String.t(),
          installation_id: String.t(),
          state: state(),
          error_count: non_neg_integer(),
          window_started_at: DateTime.t() | nil,
          opened_at: DateTime.t() | nil,
          last_probe_at: DateTime.t() | nil,
          generation: pos_integer()
        }

  @fetch_circuit_sql """
  SELECT tenant_id,
         installation_id,
         state,
         error_count,
         window_started_at,
         opened_at,
         last_probe_at,
         generation
  FROM lower_gateway_circuits
  WHERE tenant_id = $1
    AND installation_id = $2
  """

  @insert_circuit_sql """
  INSERT INTO lower_gateway_circuits (
    tenant_id,
    installation_id,
    state,
    error_count,
    window_started_at,
    opened_at,
    last_probe_at,
    generation,
    inserted_at,
    updated_at
  )
  VALUES ($1, $2, $3, $4, $5, $6, $7, 1, $8, $8)
  """

  @update_circuit_sql """
  UPDATE lower_gateway_circuits
  SET state = $4,
      error_count = $5,
      window_started_at = $6,
      opened_at = $7,
      last_probe_at = $8,
      generation = generation + 1,
      updated_at = $9
  WHERE tenant_id = $1
    AND installation_id = $2
    AND generation = $3
  RETURNING generation
  """

  @lease_holder_sql """
  SELECT holder
  FROM installation_runtime_leases
  WHERE installation_id = $1
  """

  @spec permit(String.t(), String.t(), keyword()) :: :allow | {:snooze, pos_integer()}
  def permit(tenant_id, installation_id, opts \\ [])
      when is_binary(tenant_id) and is_binary(installation_id) do
    now = normalize_datetime(Keyword.get(opts, :now, DateTime.utc_now()))

    case fetch(tenant_id, installation_id, opts) do
      %{state: state} = circuit ->
        permit_for_state(state, circuit, tenant_id, installation_id, now, opts)
    end
  end

  @spec record_success(String.t(), String.t(), keyword()) :: {:ok, t()} | {:error, term()}
  def record_success(tenant_id, installation_id, opts \\ [])
      when is_binary(tenant_id) and is_binary(installation_id) do
    now = normalize_datetime(Keyword.get(opts, :now, DateTime.utc_now()))

    mutate(tenant_id, installation_id, now, opts, fn circuit ->
      {:ok,
       %{
         circuit
         | state: :closed,
           error_count: 0,
           window_started_at: nil,
           opened_at: nil,
           last_probe_at: nil
       }}
    end)
  end

  @spec record_failure(String.t(), String.t(), keyword()) :: {:ok, t()} | {:error, term()}
  def record_failure(tenant_id, installation_id, opts \\ [])
      when is_binary(tenant_id) and is_binary(installation_id) do
    now = normalize_datetime(Keyword.get(opts, :now, DateTime.utc_now()))

    mutate(tenant_id, installation_id, now, opts, fn circuit ->
      {:ok, failure_transition(circuit, now)}
    end)
  end

  @spec fetch(String.t(), String.t(), keyword()) :: t()
  def fetch(tenant_id, installation_id, opts \\ [])
      when is_binary(tenant_id) and is_binary(installation_id) do
    now_ms = System.monotonic_time(:millisecond)
    cache_key = cache_key(tenant_id, installation_id)

    case read_cache(cache_key, now_ms) do
      {:ok, circuit} ->
        circuit

      :stale ->
        circuit = fetch_db(tenant_id, installation_id, opts)
        write_cache(cache_key, now_ms, circuit)
        circuit
    end
  end

  defp mutate(tenant_id, installation_id, now, opts, fun) do
    repo = repo(opts)

    repo.transaction(fn ->
      circuit = fetch_db(tenant_id, installation_id, opts)

      with {:ok, updated} <- fun.(circuit),
           {:ok, persisted} <- persist_transition(repo, circuit, updated, now) do
        {circuit, persisted}
      else
        {:error, reason} -> repo.rollback(reason)
      end
    end)
    |> case do
      {:ok, {original, circuit}} ->
        invalidate_cache(tenant_id, installation_id)
        emit_transition(original, circuit)
        {:ok, circuit}

      {:error, reason} ->
        {:error, reason}
    end
  end

  defp failure_transition(%{state: :half_open} = circuit, now) do
    %{
      circuit
      | state: :open,
        error_count: @failure_threshold,
        window_started_at: now,
        opened_at: now,
        last_probe_at: now
    }
  end

  defp failure_transition(%{window_started_at: nil} = circuit, now) do
    maybe_open(%{circuit | error_count: 1, window_started_at: now}, now)
  end

  defp failure_transition(circuit, now) do
    within_window? =
      DateTime.diff(now, circuit.window_started_at || now, :second) < @failure_window_seconds

    next_count = if within_window?, do: circuit.error_count + 1, else: 1
    window_started_at = if within_window?, do: circuit.window_started_at, else: now

    maybe_open(
      %{
        circuit
        | error_count: next_count,
          window_started_at: window_started_at,
          last_probe_at: circuit.last_probe_at
      },
      now
    )
  end

  defp maybe_open(circuit, now) when circuit.error_count >= @failure_threshold do
    %{circuit | state: :open, opened_at: now}
  end

  defp maybe_open(circuit, _now), do: %{circuit | state: :closed, opened_at: nil}

  defp permit_for_state(:closed, _circuit, _tenant_id, _installation_id, _now, _opts), do: :allow

  defp permit_for_state(:half_open, _circuit, _tenant_id, installation_id, _now, opts) do
    if probe_leader?(installation_id, opts) do
      :allow
    else
      snooze_delay_ms = half_open_follower_snooze_ms(opts)
      emit_snooze(:half_open, installation_id, snooze_delay_ms, opts)
      {:snooze, snooze_delay_ms}
    end
  end

  defp permit_for_state(:open, circuit, tenant_id, installation_id, now, opts) do
    if ready_for_half_open_probe?(circuit, installation_id, now, opts) do
      attempt_half_open_transition(circuit, tenant_id, installation_id, now, opts)
    else
      snooze_delay_ms = snooze_ms(circuit, now, opts)
      emit_snooze(:open, installation_id, snooze_delay_ms, opts, circuit)
      {:snooze, snooze_delay_ms}
    end
  end

  defp ready_for_half_open_probe?(circuit, installation_id, now, opts) do
    cooldown_elapsed?(circuit, now) and probe_leader?(installation_id, opts)
  end

  defp attempt_half_open_transition(circuit, tenant_id, installation_id, now, opts) do
    case transition_to_half_open(circuit, now, opts) do
      {:ok, _updated} ->
        :allow

      {:error, :stale_generation} ->
        permit(tenant_id, installation_id, opts)

      {:error, _reason} ->
        snooze_delay_ms = snooze_ms(circuit, now, opts)
        emit_snooze(:open, installation_id, snooze_delay_ms, opts, circuit)
        {:snooze, snooze_delay_ms}
    end
  end

  defp transition_to_half_open(circuit, now, opts) do
    repo = repo(opts)

    case persist_transition(
           repo,
           circuit,
           %{circuit | state: :half_open, last_probe_at: now},
           now
         ) do
      {:ok, updated} ->
        invalidate_cache(circuit.tenant_id, circuit.installation_id)
        emit_transition(circuit, updated)
        {:ok, updated}

      {:error, reason} ->
        {:error, reason}
    end
  end

  defp persist_transition(repo, original, updated, now) do
    params = [
      original.tenant_id,
      original.installation_id,
      original.generation,
      Atom.to_string(updated.state),
      updated.error_count,
      updated.window_started_at,
      updated.opened_at,
      updated.last_probe_at,
      now
    ]

    case SQL.query(repo, @update_circuit_sql, params) do
      {:ok, %{rows: [[generation]]}} ->
        {:ok, %{updated | generation: generation}}

      {:ok, %{num_rows: 0}} ->
        case insert_initial(repo, updated, now) do
          {:ok, inserted} -> {:ok, inserted}
          {:error, :already_exists} -> {:error, :stale_generation}
          {:error, reason} -> {:error, reason}
        end

      {:error, error} ->
        {:error, error}
    end
  end

  defp insert_initial(repo, circuit, now) do
    params = [
      circuit.tenant_id,
      circuit.installation_id,
      Atom.to_string(circuit.state),
      circuit.error_count,
      circuit.window_started_at,
      circuit.opened_at,
      circuit.last_probe_at,
      now
    ]

    case SQL.query(repo, @insert_circuit_sql, params) do
      {:ok, _result} ->
        {:ok, %{circuit | generation: 1}}

      {:error, %Postgrex.Error{postgres: %{code: :unique_violation}}} ->
        {:error, :already_exists}

      {:error, error} ->
        {:error, error}
    end
  end

  defp fetch_db(tenant_id, installation_id, opts) do
    case SQL.query(repo(opts), @fetch_circuit_sql, [tenant_id, installation_id]) do
      {:ok, %{rows: [row | _]}} -> row_to_circuit(row)
      {:ok, %{rows: []}} -> default_circuit(tenant_id, installation_id)
      {:error, _error} -> default_circuit(tenant_id, installation_id)
    end
  end

  defp row_to_circuit([
         tenant_id,
         installation_id,
         state,
         error_count,
         window_started_at,
         opened_at,
         last_probe_at,
         generation
       ]) do
    %{
      tenant_id: tenant_id,
      installation_id: installation_id,
      state: load_state(state),
      error_count: error_count,
      window_started_at: normalize_datetime(window_started_at),
      opened_at: normalize_datetime(opened_at),
      last_probe_at: normalize_datetime(last_probe_at),
      generation: generation
    }
  end

  defp default_circuit(tenant_id, installation_id) do
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

  defp cooldown_elapsed?(%{opened_at: nil}, _now), do: true

  defp cooldown_elapsed?(%{opened_at: opened_at}, now) do
    DateTime.diff(now, opened_at, :millisecond) >= @open_cooldown_ms
  end

  defp emit_transition(original, updated) do
    if original.state != updated.state do
      Telemetry.emit(
        [:spine, :circuit, updated.state],
        %{count: 1},
        %{
          tenant_id: updated.tenant_id,
          installation_id: updated.installation_id,
          previous_state: original.state,
          next_state: updated.state,
          error_count: updated.error_count,
          circuit_generation: updated.generation
        }
      )
    end
  end

  defp emit_snooze(state, installation_id, snooze_delay_ms, opts, circuit \\ nil) do
    Telemetry.emit(
      [:spine, :circuit, :snooze],
      %{count: 1, snooze_ms: snooze_delay_ms},
      %{
        tenant_id: circuit && circuit.tenant_id,
        installation_id: installation_id,
        circuit_state: state,
        error_count: circuit && circuit.error_count,
        probe_owner: Keyword.get(opts, :probe_owner)
      }
    )
  end

  defp snooze_ms(%{opened_at: nil}, _now, opts), do: jitter(@min_snooze_ms, opts)

  defp snooze_ms(%{opened_at: opened_at}, now, opts) do
    remaining = @open_cooldown_ms - DateTime.diff(now, opened_at, :millisecond)
    jitter(max(remaining, @min_snooze_ms), opts)
  end

  defp jitter(base_ms, opts) do
    extra_ms = Keyword.get(opts, :jitter_ms, 250)
    max(min(base_ms + extra_ms, @max_snooze_ms), @min_snooze_ms)
  end

  defp half_open_follower_snooze_ms(opts) do
    extra_ms = Keyword.get(opts, :jitter_ms, 250)

    @half_open_follower_snooze_ms
    |> Kernel.+(extra_ms)
    |> min(@max_snooze_ms)
    |> max(@half_open_follower_snooze_ms)
  end

  defp probe_leader?(installation_id, opts) do
    repo = repo(opts)
    probe_owner = Keyword.get(opts, :probe_owner, default_probe_owner())
    allow_without_lease? = Keyword.get(opts, :allow_probe_without_runtime_lease?, true)

    case SQL.query(repo, @lease_holder_sql, [installation_id]) do
      {:ok, %{rows: [[holder]]}} -> holder == probe_owner
      {:ok, %{rows: []}} -> allow_without_lease?
      {:error, _error} -> allow_without_lease?
    end
  end

  defp default_probe_owner do
    Application.get_env(
      :mezzanine_execution_engine,
      :runtime_scheduler_probe_owner,
      Atom.to_string(node())
    )
  end

  defp read_cache(cache_key, now_ms) do
    case :persistent_term.get(cache_key, nil) do
      {cached_at_ms, circuit} when now_ms - cached_at_ms <= @cache_ttl_ms ->
        {:ok, circuit}

      _other ->
        :stale
    end
  end

  defp write_cache(cache_key, now_ms, circuit) do
    :persistent_term.put(cache_key, {now_ms, circuit})
  end

  defp invalidate_cache(tenant_id, installation_id) do
    cache_key = cache_key(tenant_id, installation_id)
    :persistent_term.erase(cache_key)
    CacheInvalidator.broadcast(cache_key)
  end

  defp cache_key(tenant_id, installation_id),
    do: {:mezzanine_lower_gateway_circuit, tenant_id, installation_id}

  defp load_state(value) when is_atom(value) and value in @states, do: value

  defp load_state(value) when is_binary(value) do
    Map.get(@state_lookup, value, :closed)
  end

  defp normalize_datetime(nil), do: nil
  defp normalize_datetime(%DateTime{} = value), do: DateTime.truncate(value, :microsecond)

  defp normalize_datetime(%NaiveDateTime{} = value) do
    value
    |> DateTime.from_naive!("Etc/UTC")
    |> DateTime.truncate(:microsecond)
  end

  defp repo(opts), do: Keyword.get(opts, :repo, Repo)
end

defmodule Mezzanine.LowerGatewayCircuit.CacheInvalidator do
  @moduledoc false

  use GenServer

  @group {:mezzanine, :lower_gateway_circuits}

  def start_link(opts \\ []) do
    GenServer.start_link(__MODULE__, opts, name: __MODULE__)
  end

  def broadcast(cache_key) do
    if Code.ensure_loaded?(:pg) do
      for pid <- :pg.get_members(@group) do
        send(pid, {:invalidate_circuit_cache, cache_key})
      end
    end

    :ok
  rescue
    _error -> :ok
  end

  @impl true
  def init(_opts) do
    maybe_join_group()

    {:ok, %{}}
  end

  @impl true
  def handle_info({:invalidate_circuit_cache, cache_key}, state) do
    :persistent_term.erase(cache_key)
    {:noreply, state}
  end

  defp maybe_join_group do
    cond do
      not Code.ensure_loaded?(:pg) ->
        :ok

      Process.whereis(:pg) == nil ->
        :ok

      true ->
        :pg.join(@group, self())
        :ok
    end
  rescue
    _error -> :ok
  catch
    :exit, _reason -> :ok
  end
end
