defmodule Mezzanine.AgentRuntime.CoreTest do
  use ExUnit.Case, async: true

  alias Mezzanine.AgentRuntime.{
    ProfileBundle,
    ProfileRegistry,
    RuntimeCommandResult,
    RuntimeEventRow,
    RuntimeProjectionEnvelope,
    SubjectRef,
    ToolCatalogRef,
    WorkerRef,
    WorkspaceRef
  }

  test "strict refs reject unsafe selectors and raw paths" do
    assert {:ok, %SubjectRef{id: "subject-1"}} = SubjectRef.new("subject-1")

    assert {:ok, %ToolCatalogRef{id: "tool-catalog://fixture/local"}} =
             ToolCatalogRef.new("tool-catalog://fixture/local")

    assert {:ok, %WorkerRef{id: "worker://fixture/local"}} =
             WorkerRef.new("worker://fixture/local")

    assert {:error, :invalid_workspace_ref} = WorkspaceRef.new(%{id: "/home/user/project"})
    assert {:error, :invalid_tool_catalog_ref} = ToolCatalogRef.new(%{id: "/tmp/tools"})
    assert {:error, :invalid_worker_ref} = WorkerRef.new(%{id: "/tmp/worker"})
    assert {:error, :invalid_subject_ref} = SubjectRef.new(%{id: "subject-1", prompt: "raw"})
  end

  test "profile bundle requires all explicit slots and registry resolves without branching" do
    attrs = %{
      source_profile_ref: :synthetic_task,
      runtime_profile_ref: :execution_plane_fixture,
      tool_scope_ref: :local_coding_v1,
      evidence_profile_ref: :file_artifacts_v1,
      publication_profile_ref: :none,
      review_profile_ref: :operator_optional,
      memory_profile_ref: :none,
      projection_profile_ref: :runtime_readback_v1
    }

    assert {:ok, bundle} = ProfileBundle.new(attrs)
    assert ProfileBundle.dump(bundle)["memory_profile_ref"] == "none"

    assert {:error, :invalid_profile_bundle} =
             ProfileBundle.new(Map.delete(attrs, :runtime_profile_ref))

    entries =
      Enum.map(attrs, fn {slot, ref} ->
        %{slot: slot, ref: ref, module: __MODULE__, owner_ref: "pack://local/1.0.0"}
      end)

    assert {:ok, registry} = ProfileRegistry.new(entries)
    assert :ok = ProfileRegistry.validate_bundle(registry, bundle)
  end

  test "runtime rows dump stable public maps and reject raw provider payloads" do
    now = ~U[2026-04-27 00:00:00Z]

    assert {:ok, row} =
             RuntimeEventRow.new(%{
               event_ref: "event-1",
               event_seq: 1,
               event_kind: "lower.accepted",
               observed_at: now,
               tenant_ref: "tenant-1",
               installation_ref: "installation-1",
               subject_ref: "subject-1",
               run_ref: "run-1",
               level: "info",
               message_summary: "accepted"
             })

    assert RuntimeEventRow.dump(row)["event_ref"] == "event-1"

    assert {:error, :invalid_runtime_event_row} =
             RuntimeEventRow.new(%{
               event_ref: "event-1",
               event_seq: 1,
               event_kind: "bad",
               observed_at: now,
               tenant_ref: "tenant-1",
               installation_ref: "installation-1",
               subject_ref: "subject-1",
               run_ref: "run-1",
               level: "info",
               message_summary: "bad",
               raw_provider_payload: %{}
             })

    assert {:ok, command} =
             RuntimeCommandResult.new(%{
               command_ref: "command-1",
               command_kind: "refresh",
               status: "accepted",
               authority_state: "local",
               workflow_effect_state: "pending_signal",
               projection_state: "pending",
               trace_id: "trace-1",
               correlation_id: "corr-1",
               idempotency_key: "idem-1",
               message: "queued"
             })

    assert RuntimeCommandResult.dump(command)["command_ref"] == "command-1"

    assert {:ok, envelope} =
             RuntimeProjectionEnvelope.new(%{
               schema_ref: "runtime_state_snapshot.v1",
               schema_version: 1,
               projection_ref: "projection-1",
               projection_name: "state",
               projection_kind: "runtime_state_snapshot",
               tenant_ref: "tenant-1",
               installation_ref: "installation-1",
               profile_ref: "projection_profile:runtime_readback_v1",
               scope_ref: "scope-1",
               row_key: "state",
               updated_at: now,
               computed_at: now,
               payload: %{"ok" => true}
             })

    assert RuntimeProjectionEnvelope.dump(envelope)["payload"] == %{"ok" => true}
  end
end
