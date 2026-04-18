defmodule Mezzanine.RepoTelemetryBridgeTest do
  use ExUnit.Case, async: true

  alias Mezzanine.RepoTelemetryBridge

  defmodule FakeRepo do
  end

  test "bridges repo queue time into the canonical db pool namespace" do
    handler_id = {__MODULE__, make_ref(), :queue_time}

    :ok =
      :telemetry.attach(
        handler_id,
        [:mezzanine, :db, :pool, :queue_time],
        &__MODULE__.handle_telemetry_event/4,
        self()
      )

    try do
      RepoTelemetryBridge.handle_query_event(
        [:mezzanine_execution_engine, :repo, :query],
        %{queue_time: System.convert_time_unit(5, :millisecond, :native)},
        %{source: "execution_records", result: {:ok, %{num_rows: 1}}},
        %{repo: FakeRepo, repo_name: "execution"}
      )

      assert_receive {:telemetry_event, [:mezzanine, :db, :pool, :queue_time],
                      %{count: 1, queue_time_ms: 5}, metadata}

      assert metadata.event_name == "db.pool.queue_time"
      assert metadata.repo_name == "execution"
      assert metadata.repo_module == inspect(FakeRepo)
      assert metadata.source == "execution_records"
    after
      :telemetry.detach(handler_id)
    end
  end

  test "bridges queued checkout failures into the canonical timeout namespace" do
    handler_id = {__MODULE__, make_ref(), :checkout_timeout}

    :ok =
      :telemetry.attach(
        handler_id,
        [:mezzanine, :db, :pool, :checkout_timeout],
        &__MODULE__.handle_telemetry_event/4,
        self()
      )

    try do
      RepoTelemetryBridge.handle_query_event(
        [:mezzanine_archival_engine, :repo, :query],
        %{},
        %{
          source: "archival_manifests",
          result:
            {:error,
             %{
               __struct__: :"Elixir.DBConnection.ConnectionError",
               message: "connection not available and request was dropped from queue after 1000ms"
             }}
        },
        %{repo: FakeRepo, repo_name: "archival"}
      )

      assert_receive {:telemetry_event, [:mezzanine, :db, :pool, :checkout_timeout], %{count: 1},
                      metadata}

      assert metadata.event_name == "db.pool.checkout_timeout"
      assert metadata.repo_name == "archival"
      assert metadata.repo_module == inspect(FakeRepo)
      assert metadata.source == "archival_manifests"
      assert metadata.error_message =~ "dropped from queue"
    after
      :telemetry.detach(handler_id)
    end
  end

  def handle_telemetry_event(event, measurements, metadata, pid) do
    send(pid, {:telemetry_event, event, measurements, metadata})
  end
end
