defmodule Mezzanine.LeasingTest do
  use Mezzanine.Leasing.DataCase, async: false

  alias Mezzanine.LeaseInvalidation
  alias Mezzanine.Leasing
  alias Mezzanine.Leasing.AuthorizationScope

  @node_ref "node://mez_1@127.0.0.1/node-a"
  @commit_hlc %{"w" => 1_776_947_200_000_000_000, "l" => 0, "n" => @node_ref}

  test "issues and authorizes a scoped read lease" do
    telemetry_ids = attach_telemetry([[:mezzanine, :lease, :issued]])

    try do
      {:ok, lease} =
        Leasing.issue_read_lease(%{
          trace_id: "00-4bf92f3577b34da6a3ce929d0e0e4736-f067aa0ba902b7-01",
          tenant_id: "tenant-a",
          installation_id: Ecto.UUID.generate(),
          installation_revision: 12,
          activation_epoch: 8,
          lease_epoch: 4,
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
      assert lease.installation_revision == 12
      assert lease.activation_epoch == 8
      assert lease.lease_epoch == 4

      scope = authorization_scope(lease)

      assert {:ok, _authorized} =
               Leasing.authorize_read(scope, lease.lease_id, lease.lease_token, :events)

      assert {:error, :unauthorized_operation} =
               Leasing.authorize_read(scope, lease.lease_id, lease.lease_token, :run_artifacts)

      assert {:error, :tenant_mismatch} =
               Leasing.authorize_read(
                 %{scope | tenant_id: "tenant-b"},
                 lease.lease_id,
                 lease.lease_token,
                 :events
               )

      assert {:error, :lease_epoch_mismatch} =
               Leasing.authorize_read(
                 %{scope | lease_epoch: 3},
                 lease.lease_id,
                 lease.lease_token,
                 :events
               )
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
          installation_revision: 12,
          activation_epoch: 8,
          lease_epoch: 4,
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
          installation_revision: 12,
          activation_epoch: 8,
          lease_epoch: 4,
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
               Leasing.authorize_stream_attach(
                 authorization_scope(stream_lease),
                 stream_lease.lease_id,
                 stream_lease.attach_token
               )

      assert is_integer(sequence_number)

      assert [%LeaseInvalidation{reason: "subject_paused"} | _rest] =
               Repo.all(from(row in LeaseInvalidation, order_by: [asc: row.sequence_number]))
    after
      detach_telemetry(telemetry_ids)
    end
  end

  test "lease invalidation closes subject access graph edges with revoking authority" do
    subject_id = Ecto.UUID.generate()
    installation_id = Ecto.UUID.generate()
    revoking_authority_ref = governance_ref("lease-revocation")

    {:ok, read_lease} =
      Leasing.issue_read_lease(%{
        trace_id: "trace-graph-revocation",
        tenant_id: "tenant-graph",
        installation_id: installation_id,
        installation_revision: 12,
        activation_epoch: 8,
        lease_epoch: 4,
        subject_id: subject_id,
        lineage_anchor: %{"submission_ref" => "sub-graph"},
        allowed_family: "unified_trace",
        allowed_operations: [:fetch_run]
      })

    assert {:ok, [%LeaseInvalidation{}]} =
             Leasing.invalidate_subject_leases(subject_id, "subject_paused",
               access_graph_store: __MODULE__.AccessGraphRecorder,
               access_graph_test_pid: self(),
               revoking_authority_ref: revoking_authority_ref,
               source_node_ref: @node_ref,
               commit_hlc: @commit_hlc,
               trace_id: "trace-graph-revocation"
             )

    assert_receive {:access_graph_revoke_subject_edges, "tenant-graph", ^subject_id,
                    ^revoking_authority_ref, opts}

    assert Keyword.fetch!(opts, :cause) == "lease_revoked"
    assert Keyword.fetch!(opts, :source_node_ref) == @node_ref
    assert Keyword.fetch!(opts, :commit_hlc) == @commit_hlc
    assert Keyword.fetch!(opts, :trace_id) == "trace-graph-revocation"

    assert get_in(Keyword.fetch!(opts, :metadata), ["lease_revocations"]) == [
             %{
               "lease_kind" => "read",
               "reason" => "subject_paused",
               "revocation_ref" => "lease-revocation:read:#{read_lease.lease_id}:1"
             }
           ]
  end

  test "serializes concurrent invalidations with a gap-free cursor" do
    stream_lease_ids =
      Enum.map(1..10, fn index ->
        {:ok, lease} =
          Leasing.issue_stream_attach_lease(%{
            trace_id: "00-4bf92f3577b34da6a3ce929d0e0e4736-f067aa0ba902b7-01",
            tenant_id: "tenant-#{index}",
            installation_id: Ecto.UUID.generate(),
            installation_revision: index,
            activation_epoch: index,
            lease_epoch: index,
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

  defp authorization_scope(lease) do
    AuthorizationScope.new!(%{
      tenant_id: lease.tenant_id,
      installation_id: lease.installation_id,
      installation_revision: lease.installation_revision,
      activation_epoch: lease.activation_epoch,
      lease_epoch: lease.lease_epoch,
      subject_id: lease.subject_id,
      execution_id: lease.execution_id,
      trace_id: lease.trace_id,
      actor_ref: %{id: "lease-test"},
      authorized_at: DateTime.utc_now()
    })
  end

  defp governance_ref(id) do
    subject = %{
      kind: :install,
      id: "install-#{id}",
      metadata: %{phase: 7}
    }

    %{
      kind: :policy_decision,
      id: id,
      subject: subject,
      evidence: [
        %{
          kind: :install,
          id: "evidence-#{id}",
          packet_ref: "jido://v2/review_packet/install/#{id}",
          subject: subject,
          metadata: %{phase: 7}
        }
      ],
      metadata: %{phase: 7}
    }
  end

  defmodule AccessGraphRecorder do
    @moduledoc false

    def revoke_subject_edges(tenant_ref, subject_ref, revoking_authority_ref, opts) do
      opts
      |> Keyword.fetch!(:access_graph_test_pid)
      |> send(
        {:access_graph_revoke_subject_edges, tenant_ref, subject_ref, revoking_authority_ref,
         opts}
      )

      {:ok, %{epoch: 2, revoked_edges: []}}
    end
  end

  def handle_telemetry_event(event, measurements, metadata, pid) do
    send(pid, {:telemetry_event, event, measurements, metadata})
  end
end
