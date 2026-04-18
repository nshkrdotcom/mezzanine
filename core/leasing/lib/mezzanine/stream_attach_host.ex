defmodule Mezzanine.StreamAttachHost do
  @moduledoc """
  Durable-cursor stream invalidation host.
  """

  use GenServer

  alias Mezzanine.Leasing
  alias Mezzanine.StreamAttachLease
  alias Mezzanine.Telemetry

  @type option ::
          {:lease_id, Ecto.UUID.t()}
          | {:token, String.t()}
          | {:repo, module()}
          | {:poll_interval_ms, pos_integer()}
          | {:notify, pid()}
          | {:name, GenServer.name()}

  @spec start_link([option()]) :: GenServer.on_start()
  def start_link(opts) do
    name = Keyword.get(opts, :name)
    GenServer.start_link(__MODULE__, opts, name: name)
  end

  @impl true
  def init(opts) do
    poll_interval_ms =
      Keyword.get(
        opts,
        :poll_interval_ms,
        Application.get_env(:mezzanine_leasing, :default_poll_interval_ms, 2_000)
      )

    if poll_interval_ms > 2_000 do
      {:stop, {:poll_interval_too_large, poll_interval_ms}}
    else
      repo = Keyword.fetch!(opts, :repo)

      case build_poll_target(repo) do
        {:ok, poll_target} ->
          state = %{
            lease_id: Keyword.fetch!(opts, :lease_id),
            token: Keyword.fetch!(opts, :token),
            repo: repo,
            poll_target: poll_target,
            notify: Keyword.get(opts, :notify, self()),
            poll_interval_ms: poll_interval_ms,
            cursor: 0,
            lease_context: nil
          }

          {:ok, state, {:continue, :attach}}

        {:error, reason} ->
          {:stop, reason}
      end
    end
  end

  @impl true
  def handle_continue(:attach, state) do
    case Leasing.authorize_stream_attach(state.lease_id, state.token, repo: state.repo) do
      {:ok, lease} ->
        cursor = lease.last_invalidation_cursor || lease.issued_invalidation_cursor || 0
        lease_context = lease_context(lease)

        case Leasing.list_invalidations_after(
               cursor,
               Keyword.merge(
                 poll_target_opts(state),
                 lease_id: state.lease_id,
                 lease_kind: :stream
               )
             ) do
          {:ok, []} ->
            emit_stream_attach_active(lease_context, state, cursor)
            send(state.notify, {:stream_attached, state.lease_id, cursor})
            schedule_poll(state.poll_interval_ms)
            {:noreply, %{state | cursor: cursor, lease_context: lease_context}}

          {:ok, [invalidation | _rest] = invalidations} ->
            emit_gap_detected(lease_context, invalidations, cursor)
            emit_stream_terminated(lease_context, invalidation, cursor)

            :ok =
              Leasing.advance_stream_cursor(
                state.lease_id,
                invalidation.sequence_number,
                poll_target_opts(state)
              )

            send(
              state.notify,
              {:stream_invalidated, state.lease_id, invalidation.reason,
               invalidation.sequence_number}
            )

            {:stop, :normal,
             %{state | cursor: invalidation.sequence_number, lease_context: lease_context}}
        end

      {:error, {:lease_invalidated, reason, sequence_number}} ->
        maybe_emit_attach_invalidation(state, reason, sequence_number)
        send(state.notify, {:stream_invalidated, state.lease_id, reason, sequence_number})
        {:stop, :normal, state}

      {:error, reason} ->
        {:stop, reason, state}
    end
  end

  @impl true
  def handle_info(:poll, state) do
    case Leasing.list_invalidations_after(
           state.cursor,
           Keyword.merge(
             poll_target_opts(state),
             lease_id: state.lease_id,
             lease_kind: :stream
           )
         ) do
      {:ok, []} ->
        schedule_poll(state.poll_interval_ms)
        {:noreply, state}

      {:ok, [invalidation | _rest] = invalidations} ->
        emit_gap_detected(state.lease_context, invalidations, state.cursor)
        emit_stream_terminated(state.lease_context, invalidation, state.cursor)

        :ok =
          Leasing.advance_stream_cursor(
            state.lease_id,
            invalidation.sequence_number,
            poll_target_opts(state)
          )

        send(
          state.notify,
          {:stream_invalidated, state.lease_id, invalidation.reason, invalidation.sequence_number}
        )

        {:stop, :normal, %{state | cursor: invalidation.sequence_number}}
    end
  end

  defp emit_stream_attach_active(lease_context, state, cursor) do
    Telemetry.emit(
      [:stream, :attach, :active],
      %{count: 1},
      Map.merge(lease_context, %{
        lease_id: state.lease_id,
        current_cursor: cursor,
        poll_interval_ms: state.poll_interval_ms
      })
    )
  end

  defp emit_gap_detected(_lease_context, [_single], _cursor), do: :ok
  defp emit_gap_detected(nil, _invalidations, _cursor), do: :ok

  defp emit_gap_detected(lease_context, invalidations, cursor) do
    first = hd(invalidations)
    latest = List.last(invalidations)

    Telemetry.emit(
      [:lease, :invalidate, :gap_detected],
      %{count: 1, lag_ms: lag_ms(first.invalidated_at)},
      Map.merge(lease_context, %{
        lease_id: lease_context.lease_id,
        current_cursor: cursor,
        first_pending_sequence_number: first.sequence_number,
        latest_pending_sequence_number: latest.sequence_number,
        pending_invalidations: length(invalidations)
      })
    )
  end

  defp emit_stream_terminated(nil, _invalidation, _cursor), do: :ok

  defp emit_stream_terminated(lease_context, invalidation, cursor) do
    lag_ms = lag_ms(invalidation.invalidated_at)

    Telemetry.emit(
      [:stream, :attach, :terminated_by_invalidation],
      %{count: 1, lag_ms: lag_ms},
      Map.merge(lease_context, %{
        lease_id: lease_context.lease_id,
        current_cursor: cursor,
        invalidation_reason: invalidation.reason,
        invalidation_sequence_number: invalidation.sequence_number,
        invalidated_at: invalidation.invalidated_at
      })
    )
  end

  defp maybe_emit_attach_invalidation(state, reason, sequence_number) do
    with {lease_context, cursor} <- stream_lease_snapshot(state),
         invalidations <-
           fetch_invalidations(state.repo, state.lease_id, cursor, sequence_number, reason),
         [invalidation | _rest] <- invalidations do
      emit_gap_detected(lease_context, invalidations, cursor)
      emit_stream_terminated(lease_context, invalidation, cursor)
    end
  end

  defp schedule_poll(poll_interval_ms) do
    Process.send_after(self(), :poll, poll_interval_ms)
  end

  defp poll_target_opts(%{poll_target: {:repo, repo}}), do: [repo: repo]
  defp poll_target_opts(%{poll_target: {:connection, connection}}), do: [connection: connection]

  defp build_poll_target(repo) do
    if sandbox_repo?(repo) do
      {:ok, {:repo, repo}}
    else
      case start_poll_connection(repo) do
        {:ok, connection} -> {:ok, {:connection, connection}}
        {:error, reason} -> {:error, reason}
      end
    end
  end

  defp start_poll_connection(repo) do
    repo
    |> repo_connection_config()
    |> Postgrex.start_link()
  end

  defp repo_connection_config(repo) do
    repo.config()
    |> Keyword.take([
      :hostname,
      :port,
      :socket_dir,
      :database,
      :username,
      :password,
      :parameters,
      :ssl,
      :ssl_opts,
      :socket_options,
      :timeout,
      :connect_timeout
    ])
    |> Enum.reject(fn {_key, value} -> is_nil(value) end)
  end

  defp sandbox_repo?(repo) do
    repo.config()
    |> Keyword.get(:pool)
    |> Kernel.==(Ecto.Adapters.SQL.Sandbox)
  end

  defp fetch_stream_lease_snapshot(repo, lease_id) do
    case repo.get(StreamAttachLease, lease_id) do
      nil ->
        nil

      lease ->
        cursor = lease.last_invalidation_cursor || lease.issued_invalidation_cursor || 0
        {lease_context(lease), cursor}
    end
  end

  defp stream_lease_snapshot(%{lease_context: nil, repo: repo, lease_id: lease_id}) do
    fetch_stream_lease_snapshot(repo, lease_id)
  end

  defp stream_lease_snapshot(%{lease_context: lease_context, cursor: cursor}) do
    {lease_context, cursor}
  end

  defp fetch_invalidations(repo, lease_id, cursor, sequence_number, reason) do
    case Leasing.list_invalidations_after(cursor,
           repo: repo,
           lease_id: lease_id,
           lease_kind: :stream
         ) do
      {:ok, [_invalidation | _rest] = invalidations} ->
        invalidations

      _other ->
        [%{sequence_number: sequence_number, reason: reason, invalidated_at: DateTime.utc_now()}]
    end
  end

  defp lease_context(lease) do
    %{
      lease_id: lease.lease_id,
      trace_id: lease.trace_id,
      tenant_id: lease.tenant_id,
      installation_id: lease.installation_id,
      subject_id: lease.subject_id,
      execution_id: lease.execution_id,
      allowed_family: lease.allowed_family
    }
  end

  defp lag_ms(nil), do: 0

  defp lag_ms(%DateTime{} = invalidated_at) do
    DateTime.diff(DateTime.utc_now(), invalidated_at, :millisecond)
    |> max(0)
  end
end
