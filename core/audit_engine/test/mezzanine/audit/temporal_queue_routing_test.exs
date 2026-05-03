defmodule Mezzanine.Audit.TemporalQueueRoutingTest do
  use Mezzanine.Audit.DataCase, async: false

  alias Mezzanine.Audit.TemporalQueueRouting

  @installation_ref "installation://tenant-a/prod/pack-a"
  @tenant_ref "tenant://tenant-a"
  @node_instance_id "12345678-90ab-cdef-1234-567890abcdef"

  test "uses b32lower sha256 hash segments for typed-ref task queues" do
    segment = TemporalQueueRouting.hash_segment(@installation_ref)

    assert byte_size(segment) == 20
    assert b32lower_segment?(segment)

    assert TemporalQueueRouting.promotion_queue(@installation_ref) ==
             "mez.promotion.#{segment}"

    assert TemporalQueueRouting.workflow_runtime_queue(@tenant_ref) ==
             "mez.workflow_runtime.#{TemporalQueueRouting.hash_segment(@tenant_ref)}"
  end

  defp b32lower_segment?(segment) do
    segment
    |> :binary.bin_to_list()
    |> Enum.all?(fn byte -> byte in ?a..?z or byte in ?2..?7 end)
  end

  test "declares canonical static queues" do
    assert TemporalQueueRouting.decision_expiry_queue() == "mez.decision_expiry"
    assert TemporalQueueRouting.invalidation_cascade_queue() == "mez.invalidation_cascade"
  end

  test "builds audit-owned reverse lookup entries for hashed task queue segments" do
    entry = TemporalQueueRouting.reverse_lookup_entry(:installation, @installation_ref)

    assert %{
             hash_segment: segment,
             typed_ref: @installation_ref,
             ref_kind: :installation
           } = entry

    assert entry.queue == "mez.promotion.#{segment}"
  end

  test "persists the reverse lookup table owned by audit engine" do
    assert {:ok, record} =
             TemporalQueueRouting.upsert_reverse_lookup(:installation, @installation_ref)

    assert record.hash_segment == TemporalQueueRouting.hash_segment(@installation_ref)
    assert record.typed_ref == @installation_ref
    assert record.ref_kind == "installation"
    assert record.queue == TemporalQueueRouting.promotion_queue(@installation_ref)

    assert {:ok, fetched} = TemporalQueueRouting.fetch_reverse_lookup(record.hash_segment)
    assert fetched.typed_ref == @installation_ref

    assert has_index?("temporal_queue_reverse_lookups", ["hash_segment"])
    assert has_index?("temporal_queue_reverse_lookups", ["queue"])
  end

  test "worker identity uses node boot evidence, role, and task queue hash only" do
    queue = TemporalQueueRouting.promotion_queue(@installation_ref)

    assert "mez-a/12345678/temporal_worker/" <> task_queue_hash =
             TemporalQueueRouting.worker_identity(
               node_shortname: "mez-a",
               node_instance_id: @node_instance_id,
               worker_role: :temporal_worker,
               task_queue: queue
             )

    assert task_queue_hash == TemporalQueueRouting.hash_segment(@installation_ref)

    identity =
      TemporalQueueRouting.worker_identity(
        node_shortname: "mez-a",
        node_instance_id: @node_instance_id,
        worker_role: :temporal_worker,
        task_queue: queue
      )

    assert byte_size(identity) <= 96
    refute identity =~ "installation://"
    refute identity =~ "PID"
  end

  test "rejects worker identity fields that would leak raw refs or path segments" do
    assert {:error, :invalid_node_shortname} =
             TemporalQueueRouting.worker_identity(
               node_shortname: "mez/a",
               node_instance_id: @node_instance_id,
               worker_role: :temporal_worker,
               task_queue: TemporalQueueRouting.promotion_queue(@installation_ref)
             )
  end

  defp has_index?(table_name, columns) when is_binary(table_name) and is_list(columns) do
    columns_sql = Enum.join(columns, ", ")

    Repo.query!(
      """
      SELECT indexdef
      FROM pg_indexes
      WHERE schemaname = current_schema()
        AND tablename = $1
      """,
      [table_name]
    ).rows
    |> Enum.any?(fn [indexdef] ->
      String.contains?(indexdef, "(#{columns_sql})")
    end)
  end
end
