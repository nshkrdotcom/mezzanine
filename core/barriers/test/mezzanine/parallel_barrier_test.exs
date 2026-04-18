defmodule Mezzanine.ParallelBarrierTest do
  use Mezzanine.Barriers.DataCase, async: false

  alias Mezzanine.ParallelBarrier

  test "open/1 reuses the same barrier for the same subject and barrier key" do
    telemetry_ids = attach_telemetry([[:mezzanine, :barrier, :open]])

    attrs = %{
      subject_id: Ecto.UUID.generate(),
      barrier_key: "fanout:triage",
      join_step_ref: "triage_join",
      expected_children: 2,
      trace_id: "trace-barrier-open"
    }

    try do
      assert {:ok, barrier} = ParallelBarrier.open(attrs)
      assert {:ok, same_barrier} = ParallelBarrier.open(attrs)

      assert_receive {:telemetry_event, [:mezzanine, :barrier, :open], %{count: 1}, metadata}
      assert metadata.event_name == "barrier.open"
      assert metadata.subject_id == attrs.subject_id
      assert metadata.barrier_id == barrier.id
      assert metadata.expected_children == 2

      assert same_barrier.id == barrier.id
      assert same_barrier.expected_children == 2
      assert same_barrier.status == :open
    after
      detach_telemetry(telemetry_ids)
    end
  end

  test "expected_children is immutable after creation" do
    assert {:ok, barrier} =
             ParallelBarrier.open(%{
               subject_id: Ecto.UUID.generate(),
               barrier_key: "fanout:immutable",
               join_step_ref: "triage_join",
               expected_children: 2,
               trace_id: "trace-barrier-immutable"
             })

    assert_raise Postgrex.Error, fn ->
      Repo.query!(
        "UPDATE parallel_barriers SET expected_children = 3 WHERE id = $1::uuid",
        [dump_uuid!(barrier.id)]
      )
    end
  end

  test "record_child_completion/3 deduplicates child completions and closes once" do
    telemetry_ids = attach_telemetry([[:mezzanine, :barrier, :child_completion]])

    assert {:ok, barrier} =
             ParallelBarrier.open(%{
               subject_id: Ecto.UUID.generate(),
               barrier_key: "fanout:close-once",
               join_step_ref: "triage_join",
               expected_children: 2,
               trace_id: "trace-barrier-close"
             })

    barrier_id = barrier.id

    child_one = Ecto.UUID.generate()
    child_two = Ecto.UUID.generate()

    try do
      assert {:ok, first} = ParallelBarrier.record_child_completion(barrier_id, child_one)
      refute first.duplicate?
      refute first.closed_by_me
      assert first.completed_children == 1
      assert first.status == :open

      assert_receive {:telemetry_event, [:mezzanine, :barrier, :child_completion],
                      %{count: 1, latency_ms: _}, metadata}

      assert metadata.event_name == "barrier.child_completion"
      assert metadata.barrier_id == barrier_id
      assert metadata.execution_id == child_one
      assert metadata.duplicate == false
      assert metadata.closed_by_me == false

      assert {:ok, duplicate} = ParallelBarrier.record_child_completion(barrier_id, child_one)
      assert duplicate.duplicate?
      refute duplicate.closed_by_me
      assert duplicate.completed_children == 1
      assert duplicate.status == :open

      assert_receive {:telemetry_event, [:mezzanine, :barrier, :child_completion],
                      %{count: 1, latency_ms: _}, duplicate_metadata}

      assert duplicate_metadata.execution_id == child_one
      assert duplicate_metadata.duplicate == true
      assert duplicate_metadata.closed_by_me == false

      assert {:ok, closer} = ParallelBarrier.record_child_completion(barrier_id, child_two)
      refute closer.duplicate?
      assert closer.closed_by_me
      assert closer.completed_children == 2
      assert closer.status == :ready

      assert_receive {:telemetry_event, [:mezzanine, :barrier, :child_completion],
                      %{count: 1, latency_ms: _}, closer_metadata}

      assert closer_metadata.execution_id == child_two
      assert closer_metadata.duplicate == false
      assert closer_metadata.closed_by_me == true
      assert closer_metadata.barrier_status == :ready

      assert {:error, {:barrier_not_open, ^barrier_id, :ready}} =
               ParallelBarrier.record_child_completion(barrier_id, Ecto.UUID.generate())
    after
      detach_telemetry(telemetry_ids)
    end
  end

  defp dump_uuid!(uuid), do: Ecto.UUID.dump!(uuid)

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
