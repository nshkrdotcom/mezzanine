defmodule Mezzanine.LeasingTest do
  use Mezzanine.Leasing.DataCase, async: false

  alias Mezzanine.LeaseInvalidation
  alias Mezzanine.Leasing

  test "issues and authorizes a scoped read lease" do
    telemetry_ids = attach_telemetry([[:mezzanine, :lease, :issued]])

    try do
      {:ok, lease} =
        Leasing.issue_read_lease(%{
          trace_id: "00-4bf92f3577b34da6a3ce929d0e0e4736-f067aa0ba902b7-01",
          tenant_id: "tenant-a",
          installation_id: Ecto.UUID.generate(),
          subject_id: Ecto.UUID.generate(),
          lineage_anchor: %{"submission_ref" => "sub-1"},
          allowed_family: "unified_trace",
          allowed_operations: [:fetch_run, :events],
          scope: %{"stream" => false}
        })

      assert_receive {:telemetry_event, [:mezzanine, :lease, :issued], %{count: 1}, metadata}
      assert metadata.event_name == "lease.issued"
      assert metadata.lease_id == lease.lease_id
      assert metadata.trace_id == lease.trace_id
      assert metadata.lease_kind == "read"
      assert metadata.allowed_family == "unified_trace"

      assert lease.issued_invalidation_cursor == 0

      assert {:ok, _authorized} =
               Leasing.authorize_read(lease.lease_id, lease.lease_token, :events)

      assert {:error, :unauthorized_operation} =
               Leasing.authorize_read(lease.lease_id, lease.lease_token, :run_artifacts)
    after
      detach_telemetry(telemetry_ids)
    end
  end

  test "invalidates subject-bound leases through durable rows" do
    telemetry_ids = attach_telemetry([[:mezzanine, :lease, :invalidated]])
    subject_id = Ecto.UUID.generate()

    try do
      {:ok, read_lease} =
        Leasing.issue_read_lease(%{
          trace_id: "00-4bf92f3577b34da6a3ce929d0e0e4736-f067aa0ba902b7-01",
          tenant_id: "tenant-a",
          installation_id: Ecto.UUID.generate(),
          subject_id: subject_id,
          lineage_anchor: %{"submission_ref" => "sub-2"},
          allowed_family: "unified_trace",
          allowed_operations: [:fetch_run]
        })

      {:ok, stream_lease} =
        Leasing.issue_stream_attach_lease(%{
          trace_id: "00-4bf92f3577b34da6a3ce929d0e0e4736-f067aa0ba902b7-01",
          tenant_id: "tenant-a",
          installation_id: Ecto.UUID.generate(),
          subject_id: subject_id,
          lineage_anchor: %{"submission_ref" => "sub-2"},
          allowed_family: "runtime_stream"
        })

      assert {:ok, rows} = Leasing.invalidate_subject_leases(subject_id, "subject_paused")

      assert Enum.map(rows, & &1.lease_id) |> Enum.sort() ==
               Enum.sort([read_lease.lease_id, stream_lease.lease_id])

      invalidation_events = receive_telemetry_events(2)

      assert Enum.sort(
               Enum.map(invalidation_events, fn {_event, _measurements, metadata} ->
                 metadata.lease_id
               end)
             ) ==
               Enum.sort([read_lease.lease_id, stream_lease.lease_id])

      Enum.each(invalidation_events, fn {event, measurements, metadata} ->
        assert event == [:mezzanine, :lease, :invalidated]
        assert measurements == %{count: 1}
        assert metadata.event_name == "lease.invalidated"
        assert metadata.subject_id == subject_id
        assert metadata.reason == "subject_paused"
      end)

      assert {:error, {:lease_invalidated, "subject_paused", sequence_number}} =
               Leasing.authorize_stream_attach(stream_lease.lease_id, stream_lease.attach_token)

      assert is_integer(sequence_number)

      assert [%LeaseInvalidation{reason: "subject_paused"} | _rest] =
               Repo.all(from(row in LeaseInvalidation, order_by: [asc: row.sequence_number]))
    after
      detach_telemetry(telemetry_ids)
    end
  end

  test "serializes concurrent invalidations with a gap-free cursor" do
    stream_lease_ids =
      Enum.map(1..10, fn index ->
        {:ok, lease} =
          Leasing.issue_stream_attach_lease(%{
            trace_id: "00-4bf92f3577b34da6a3ce929d0e0e4736-f067aa0ba902b7-01",
            tenant_id: "tenant-#{index}",
            installation_id: Ecto.UUID.generate(),
            execution_id: Ecto.UUID.generate(),
            lineage_anchor: %{"submission_ref" => "sub-#{index}"},
            allowed_family: "runtime_stream"
          })

        lease.lease_id
      end)

    stream_lease_ids
    |> Enum.map(fn lease_id ->
      Task.async(fn ->
        Leasing.invalidate_stream_attach_lease(lease_id, "stress_disconnect")
      end)
    end)
    |> Enum.each(&Task.await(&1, 5_000))

    sequence_numbers =
      Repo.all(
        from(row in LeaseInvalidation,
          select: row.sequence_number,
          order_by: [asc: row.sequence_number]
        )
      )

    assert sequence_numbers == Enum.to_list(1..10)
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

  defp receive_telemetry_events(count) do
    Enum.map(1..count, fn _index ->
      assert_receive {:telemetry_event, event, measurements, metadata}
      {event, measurements, metadata}
    end)
  end

  def handle_telemetry_event(event, measurements, metadata, pid) do
    send(pid, {:telemetry_event, event, measurements, metadata})
  end
end
