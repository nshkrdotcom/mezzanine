defmodule Mezzanine.SourceEngine.SourceRefreshRequestTest do
  use ExUnit.Case, async: true

  alias Mezzanine.SourceEngine.{SourceCursor, SourceRefreshRequest}

  test "requests source refresh without starting poll or reconcile effects" do
    assert {:ok, request} =
             SourceRefreshRequest.request(%{
               tenant_id: "tenant-1",
               installation_id: "installation-1",
               subject_id: "subject-1",
               source_binding_id: "linear-primary",
               trace_id: "trace-refresh",
               causation_id: "cause-refresh",
               actor_ref: %{kind: :operator, id: "ops", tenant_id: "tenant-1"},
               idempotency_key: "refresh:tenant-1:subject-1"
             })

    assert %SourceCursor{} = request.cursor
    assert request.cursor.source_binding_id == "linear-primary"
    assert request.cursor.refresh_requested?
    assert request.refresh_requested?
    refute request.lower_effect_started?
    refute request.reconcile_started?
    assert request.actor_ref["tenant_id"] == "tenant-1"
  end

  test "rejects actor tenant mismatch before producing a refresh cursor" do
    assert {:error, :operator_actor_tenant_mismatch} =
             SourceRefreshRequest.request(%{
               tenant_id: "tenant-1",
               installation_id: "installation-1",
               subject_id: "subject-1",
               source_binding_id: "linear-primary",
               trace_id: "trace-refresh-denied",
               causation_id: "cause-refresh-denied",
               actor_ref: %{kind: :operator, id: "ops", tenant_id: "tenant-2"},
               idempotency_key: "refresh:tenant-1:subject-1"
             })
  end
end
