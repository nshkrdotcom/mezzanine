defmodule Mezzanine.Projections.StorePostgresTest do
  use Mezzanine.Projections.DataCase, async: false

  alias Mezzanine.Projections.Store

  @store_opts [profile: :integration_postgres]

  test "postgres adapter persists the generic lineage outbox contract" do
    attrs = %{
      id: "projection://tenant-a/subject-a",
      projection_ref: "projection://tenant-a/subject-a",
      operation_context_ref: "operation-context://tenant-a/request-a",
      subject_ref: "subject://document/1",
      trace_ref: "trace://tenant-a/run-a",
      metadata: %{role: :proof}
    }

    assert Store.adapter(@store_opts) == Mezzanine.Projections.Store.AshPostgres
    assert :ok = Store.preflight(@store_opts)
    assert {:ok, %{adapter: :ash_postgres}} = Store.health(@store_opts)

    assert {:ok, record} = Store.put_record(attrs, @store_opts)
    assert record.id == attrs.id
    assert record.events == []
    assert record.metadata == %{role: :proof}

    first_event = %{
      event_ref: "lineage://command/1",
      trace_ref: "trace://tenant-a/run-a",
      event_kind: :command_recorded,
      causal_order: 1,
      root_event?: true,
      metadata_refs: %{subject_ref: attrs.subject_ref}
    }

    second_event = %{
      event_ref: "lineage://projection/2",
      trace_ref: "trace://tenant-a/run-a",
      event_kind: :projection_updated,
      causal_order: 2,
      predecessor_event_refs: ["lineage://command/1"],
      projection_visible?: true
    }

    assert {:ok, appended_first} = Store.append_event(record.id, first_event, @store_opts)
    assert appended_first.sequence == 1
    assert appended_first.record_id == record.id
    assert appended_first.event_kind == :command_recorded

    assert {:ok, appended_second} = Store.append_event(record.id, second_event, @store_opts)
    assert appended_second.sequence == 2

    assert {:ok, fetched} = Store.fetch_record(record.id, @store_opts)
    assert fetched.projection_ref == attrs.projection_ref
    assert fetched.operation_context_ref == attrs.operation_context_ref
    assert fetched.metadata == %{role: :proof}

    assert Enum.map(fetched.events, & &1.sequence) == [1, 2]

    assert Enum.map(fetched.events, & &1.event_ref) == [
             first_event.event_ref,
             second_event.event_ref
           ]

    assert Enum.map(fetched.events, & &1.event_kind) == [
             :command_recorded,
             :projection_updated
           ]

    assert {:ok, updated} =
             Store.update_record(record.id, %{trace_ref: "trace://tenant-a/run-b"}, @store_opts)

    assert updated.trace_ref == "trace://tenant-a/run-b"

    assert Enum.map(updated.events, & &1.event_ref) == [
             first_event.event_ref,
             second_event.event_ref
           ]

    assert {:ok, reset} = Store.put_record(attrs, @store_opts)
    assert reset.events == []
    assert {:ok, fetched_after_reset} = Store.fetch_record(record.id, @store_opts)
    assert fetched_after_reset.events == []
  end
end
