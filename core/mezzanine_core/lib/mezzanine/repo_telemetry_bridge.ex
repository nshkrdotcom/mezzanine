defmodule Mezzanine.RepoTelemetryBridge do
  @moduledoc """
  Bridges repo query telemetry into the canonical Stage 11 pool-pressure
  namespace.

  Repo-native query events stay repo-owned, but queue-pressure and checkout
  timeout visibility must land under `[:mezzanine, :db, :pool, ...]` so
  operator tooling joins on one contract.
  """

  use GenServer

  alias Mezzanine.Telemetry

  @type option ::
          {:repo, module()}
          | {:repo_name, atom() | String.t()}
          | {:query_event, [atom()]}

  @spec child_spec([option()]) :: Supervisor.child_spec()
  def child_spec(opts) do
    repo = Keyword.fetch!(opts, :repo)

    %{
      id: {__MODULE__, repo},
      start: {__MODULE__, :start_link, [opts]}
    }
  end

  @spec start_link([option()]) :: GenServer.on_start()
  def start_link(opts) do
    GenServer.start_link(__MODULE__, opts)
  end

  @spec handle_query_event([atom()], map(), map(), map()) :: :ok
  def handle_query_event(_event, measurements, metadata, context)
      when is_map(measurements) and is_map(metadata) and is_map(context) do
    emit_queue_time(measurements, metadata, context)
    emit_checkout_timeout(metadata, context)
    :ok
  end

  @impl true
  def init(opts) do
    handler_id = {__MODULE__, Keyword.fetch!(opts, :repo)}
    query_event = Keyword.fetch!(opts, :query_event)

    context = %{
      repo: Keyword.fetch!(opts, :repo),
      repo_name: opts |> Keyword.get(:repo_name) |> normalize_repo_name()
    }

    :ok = :telemetry.attach(handler_id, query_event, &__MODULE__.handle_query_event/4, context)

    {:ok, %{handler_id: handler_id}}
  end

  @impl true
  def terminate(_reason, %{handler_id: handler_id}) do
    :telemetry.detach(handler_id)
    :ok
  end

  defp emit_queue_time(measurements, metadata, context) do
    case queue_time_ms(measurements) do
      nil ->
        :ok

      queue_time_ms ->
        Telemetry.emit(
          [:db, :pool, :queue_time],
          %{count: 1, queue_time_ms: queue_time_ms},
          telemetry_metadata(metadata, context)
        )
    end
  end

  defp emit_checkout_timeout(metadata, context) do
    case Map.get(metadata, :result) do
      {:error, error} ->
        if checkout_timeout_error?(error) do
          Telemetry.emit(
            [:db, :pool, :checkout_timeout],
            %{count: 1},
            Map.merge(telemetry_metadata(metadata, context), %{
              error_message: Map.get(error, :message)
            })
          )
        end

      _other ->
        :ok
    end
  end

  defp telemetry_metadata(metadata, context) do
    %{
      repo_name: context.repo_name,
      repo_module: inspect(context.repo),
      source: Map.get(metadata, :source)
    }
  end

  defp queue_time_ms(%{queue_time: queue_time}) when is_integer(queue_time) and queue_time > 0 do
    System.convert_time_unit(queue_time, :native, :millisecond)
  end

  defp queue_time_ms(_measurements), do: nil

  defp checkout_timeout_error?(%{__struct__: struct, message: message})
       when is_atom(struct) and is_binary(message) do
    Atom.to_string(struct) == "Elixir.DBConnection.ConnectionError" and
      String.contains?(message, ["dropped from queue", "checkout timeout"])
  end

  defp checkout_timeout_error?(_error), do: false

  defp normalize_repo_name(nil), do: nil
  defp normalize_repo_name(repo_name) when is_atom(repo_name), do: Atom.to_string(repo_name)
  defp normalize_repo_name(repo_name) when is_binary(repo_name), do: repo_name
end
