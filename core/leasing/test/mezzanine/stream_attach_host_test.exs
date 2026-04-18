defmodule Mezzanine.StreamAttachHostTest do
  use Mezzanine.Leasing.DataCase, async: false

  alias Mezzanine.Leasing
  alias Mezzanine.StreamAttachHost

  test "attaches and then terminates after durable invalidation" do
    telemetry_ids =
      attach_telemetry([
        [:mezzanine, :stream, :attach, :active],
        [:mezzanine, :stream, :attach, :terminated_by_invalidation]
      ])

    try do
      {:ok, lease} =
        Leasing.issue_stream_attach_lease(%{
          trace_id: "00-4bf92f3577b34da6a3ce929d0e0e4736-f067aa0ba902b7-01",
          tenant_id: "tenant-a",
          installation_id: Ecto.UUID.generate(),
          execution_id: Ecto.UUID.generate(),
          lineage_anchor: %{"submission_ref" => "sub-1"},
          allowed_family: "runtime_stream"
        })

      lease_id = lease.lease_id

      {:ok, pid} =
        StreamAttachHost.start_link(
          lease_id: lease.lease_id,
          token: lease.attach_token,
          repo: Repo,
          notify: self(),
          poll_interval_ms: 50
        )

      monitor_ref = Process.monitor(pid)

      assert_receive {:stream_attached, ^lease_id, 0}, 500

      assert_receive {:telemetry_event, [:mezzanine, :stream, :attach, :active], %{count: 1},
                      active_metadata}

      assert active_metadata.event_name == "stream.attach.active"
      assert active_metadata.lease_id == lease_id
      assert active_metadata.current_cursor == 0
      assert active_metadata.poll_interval_ms == 50

      assert {:ok, [_row]} =
               Leasing.invalidate_stream_attach_lease(lease.lease_id, "subject_cancelled",
                 repo: Repo
               )

      assert_receive {:stream_invalidated, ^lease_id, "subject_cancelled", sequence_number}, 500
      assert is_integer(sequence_number)

      assert_receive {:telemetry_event,
                      [:mezzanine, :stream, :attach, :terminated_by_invalidation],
                      %{count: 1, lag_ms: lag_ms}, terminated_metadata}

      assert is_integer(lag_ms)
      assert lag_ms >= 0
      assert terminated_metadata.event_name == "stream.attach.terminated_by_invalidation"
      assert terminated_metadata.lease_id == lease_id
      assert terminated_metadata.invalidation_reason == "subject_cancelled"
      assert terminated_metadata.invalidation_sequence_number == sequence_number

      assert_receive {:DOWN, ^monitor_ref, :process, ^pid, :normal}, 500
    after
      detach_telemetry(telemetry_ids)
    end
  end

  test "refuses reattachment when durable invalidation landed while host was offline" do
    telemetry_ids =
      attach_telemetry([[:mezzanine, :stream, :attach, :terminated_by_invalidation]])

    try do
      {:ok, lease} =
        Leasing.issue_stream_attach_lease(%{
          trace_id: "00-4bf92f3577b34da6a3ce929d0e0e4736-f067aa0ba902b7-01",
          tenant_id: "tenant-a",
          installation_id: Ecto.UUID.generate(),
          execution_id: Ecto.UUID.generate(),
          lineage_anchor: %{"submission_ref" => "sub-1"},
          allowed_family: "runtime_stream"
        })

      lease_id = lease.lease_id

      assert {:ok, [_row]} =
               Leasing.invalidate_stream_attach_lease(lease.lease_id, "offline_invalidation",
                 repo: Repo
               )

      {:ok, pid} =
        StreamAttachHost.start_link(
          lease_id: lease.lease_id,
          token: lease.attach_token,
          repo: Repo,
          notify: self(),
          poll_interval_ms: 50
        )

      monitor_ref = Process.monitor(pid)

      assert_receive {:stream_invalidated, ^lease_id, "offline_invalidation", sequence_number},
                     500

      assert_receive {:telemetry_event,
                      [:mezzanine, :stream, :attach, :terminated_by_invalidation],
                      %{count: 1, lag_ms: lag_ms}, metadata}

      assert is_integer(lag_ms)
      assert lag_ms >= 0
      assert metadata.event_name == "stream.attach.terminated_by_invalidation"
      assert metadata.lease_id == lease_id
      assert metadata.invalidation_reason == "offline_invalidation"
      assert metadata.invalidation_sequence_number == sequence_number

      assert_receive {:DOWN, ^monitor_ref, :process, ^pid, :normal}, 500
    after
      detach_telemetry(telemetry_ids)
    end
  end

  test "emits gap telemetry when multiple invalidations accumulated while offline" do
    telemetry_ids =
      attach_telemetry([
        [:mezzanine, :lease, :invalidate, :gap_detected],
        [:mezzanine, :stream, :attach, :terminated_by_invalidation]
      ])

    try do
      {:ok, lease} =
        Leasing.issue_stream_attach_lease(%{
          trace_id: "00-4bf92f3577b34da6a3ce929d0e0e4736-f067aa0ba902b7-01",
          tenant_id: "tenant-a",
          installation_id: Ecto.UUID.generate(),
          execution_id: Ecto.UUID.generate(),
          lineage_anchor: %{"submission_ref" => "sub-gap"},
          allowed_family: "runtime_stream"
        })

      lease_id = lease.lease_id

      assert {:ok, [_first]} =
               Leasing.invalidate_stream_attach_lease(lease.lease_id, "offline_invalidation_1",
                 repo: Repo
               )

      Process.sleep(5)

      assert {:ok, [_second]} =
               Leasing.invalidate_stream_attach_lease(lease.lease_id, "offline_invalidation_2",
                 repo: Repo
               )

      {:ok, pid} =
        StreamAttachHost.start_link(
          lease_id: lease.lease_id,
          token: lease.attach_token,
          repo: Repo,
          notify: self(),
          poll_interval_ms: 50
        )

      monitor_ref = Process.monitor(pid)

      assert_receive {:stream_invalidated, ^lease_id, "offline_invalidation_1", sequence_number},
                     500

      assert_receive {:telemetry_event, [:mezzanine, :lease, :invalidate, :gap_detected],
                      %{count: 1, lag_ms: lag_ms}, gap_metadata}

      assert is_integer(lag_ms)
      assert lag_ms >= 0
      assert gap_metadata.event_name == "lease.invalidate.gap_detected"
      assert gap_metadata.lease_id == lease_id
      assert gap_metadata.current_cursor == 0
      assert gap_metadata.first_pending_sequence_number == sequence_number
      assert gap_metadata.latest_pending_sequence_number == sequence_number + 1
      assert gap_metadata.pending_invalidations == 2

      assert_receive {:telemetry_event,
                      [:mezzanine, :stream, :attach, :terminated_by_invalidation],
                      %{count: 1, lag_ms: termination_lag_ms}, terminated_metadata}

      assert is_integer(termination_lag_ms)
      assert termination_lag_ms >= 0
      assert terminated_metadata.lease_id == lease_id
      assert terminated_metadata.invalidation_reason == "offline_invalidation_1"
      assert terminated_metadata.invalidation_sequence_number == sequence_number

      assert_receive {:DOWN, ^monitor_ref, :process, ^pid, :normal}, 500
    after
      detach_telemetry(telemetry_ids)
    end
  end

  test "refuses reattachment after a prior host already observed the invalidation" do
    {:ok, lease} =
      Leasing.issue_stream_attach_lease(%{
        trace_id: "00-4bf92f3577b34da6a3ce929d0e0e4736-f067aa0ba902b7-01",
        tenant_id: "tenant-a",
        installation_id: Ecto.UUID.generate(),
        execution_id: Ecto.UUID.generate(),
        lineage_anchor: %{"submission_ref" => "sub-1"},
        allowed_family: "runtime_stream"
      })

    lease_id = lease.lease_id

    {:ok, first_pid} =
      StreamAttachHost.start_link(
        lease_id: lease.lease_id,
        token: lease.attach_token,
        repo: Repo,
        notify: self(),
        poll_interval_ms: 50
      )

    first_monitor_ref = Process.monitor(first_pid)

    assert_receive {:stream_attached, ^lease_id, 0}, 500

    assert {:ok, [_row]} =
             Leasing.invalidate_stream_attach_lease(lease.lease_id, "subject_cancelled",
               repo: Repo
             )

    assert_receive {:stream_invalidated, ^lease_id, "subject_cancelled", _sequence_number}, 500
    assert_receive {:DOWN, ^first_monitor_ref, :process, ^first_pid, :normal}, 500

    {:ok, second_pid} =
      StreamAttachHost.start_link(
        lease_id: lease.lease_id,
        token: lease.attach_token,
        repo: Repo,
        notify: self(),
        poll_interval_ms: 50
      )

    second_monitor_ref = Process.monitor(second_pid)

    assert_receive {:stream_invalidated, ^lease_id, "subject_cancelled", _sequence_number}, 500
    assert_receive {:DOWN, ^second_monitor_ref, :process, ^second_pid, :normal}, 500
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
