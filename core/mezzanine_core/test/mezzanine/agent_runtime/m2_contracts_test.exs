defmodule Mezzanine.AgentRuntime.M2ContractsTest do
  use ExUnit.Case, async: true

  alias Mezzanine.AgentRuntime.{
    AgentLoopCommand,
    AgentLoopProjection,
    AgentRunSpec,
    AgentTurnState,
    RuntimeEventRow,
    ToolActionReceipt,
    ToolActionRequest
  }

  test "AgentRunSpec is profile-neutral and rejects raw prompt/provider shortcuts" do
    assert {:ok, spec} = AgentRunSpec.new(agent_run_spec_attrs())
    assert spec.runtime_profile_ref == :execution_plane_fixture
    assert spec.memory_profile_ref == :none
    assert spec.max_turns == 3
    assert spec.session_ref == "session://local/1"
    assert spec.worker_ref == "worker://fixture/local"

    assert AgentRunSpec.dump(spec)["profile_bundle"]["runtime_profile_ref"] ==
             "execution_plane_fixture"

    assert {:error, :invalid_agent_run_spec} =
             agent_run_spec_attrs()
             |> Map.put(:prompt, "read the whole repo")
             |> AgentRunSpec.new()

    assert {:error, :invalid_agent_run_spec} =
             agent_run_spec_attrs()
             |> Map.put(:max_turns, 0)
             |> AgentRunSpec.new()
  end

  test "turn state transitions forward and duplicate replay keeps the existing state" do
    assert {:ok, turn} = AgentTurnState.new(turn_attrs())
    assert turn.state == :initialized

    assert {:ok, ready} = AgentTurnState.transition(turn, :context_ready)
    assert ready.state == :context_ready

    assert {:ok, duplicate} = AgentTurnState.transition(ready, :context_ready)
    assert duplicate == ready

    assert {:error, :invalid_agent_turn_transition} =
             AgentTurnState.transition(ready, :initialized)
  end

  test "tool action request and receipt carry refs, not raw payloads" do
    assert {:ok, request} = ToolActionRequest.new(action_request_attrs())
    assert request.tool_ref == "fixture.record_note"
    assert request.authority_context_ref == "authority-context://turn-1"

    assert {:error, :invalid_tool_action_request} =
             action_request_attrs()
             |> Map.delete(:authority_context_ref)
             |> ToolActionRequest.new()

    assert {:error, :invalid_tool_action_request} =
             action_request_attrs()
             |> Map.put(:raw_provider_payload, %{})
             |> ToolActionRequest.new()

    assert {:ok, receipt} = ToolActionReceipt.new(action_receipt_attrs())
    assert receipt.status == :succeeded
    assert receipt.lower_receipt_ref == "lower-receipt://turn-1"
  end

  test "agent loop command and projection dump to stable M1-readable maps" do
    assert {:ok, command} =
             AgentLoopCommand.new(%{
               command_ref: "command://turn-1",
               command_kind: :approve,
               run_ref: "run://local/1",
               actor_ref: "actor://operator",
               idempotency_key: "agent-run:decision:decision-1:command-1",
               payload_ref: "payload://approval/1",
               trace_id: "trace://local/1"
             })

    assert AgentLoopCommand.dump(command)["command_kind"] == "approve"

    assert {:ok, event} =
             RuntimeEventRow.new(%{
               event_ref: "event://agent-loop/run-1/1",
               event_seq: 1,
               event_kind: "turn.started",
               observed_at: ~U[2026-04-27 00:00:00Z],
               tenant_ref: "tenant://local",
               installation_ref: "installation://local",
               subject_ref: "subject://task/1",
               run_ref: "run://local/1",
               turn_ref: "turn://local/1/1",
               level: "info",
               message_summary: "turn started"
             })

    assert {:ok, projection} =
             AgentLoopProjection.new(%{
               run_ref: "run://local/1",
               subject_ref: "subject://task/1",
               workflow_ref: "workflow://agent-loop/local/1",
               session_ref: "session://local/1",
               workspace_ref: "workspace://local/1",
               worker_ref: "worker://fixture/local",
               terminal_state: "completed",
               current_turn_ref: "turn://local/1/1",
               status: "completed",
               turn_states: [turn_attrs()],
               action_requests: [action_request_attrs()],
               action_receipts: [action_receipt_attrs()],
               runtime_events: [event],
               command_results: [],
               receipt_ref_set: %{
                 "session_refs" => ["session://local/1"],
                 "turn_refs" => ["turn://local/1/1"],
                 "event_refs" => ["event://agent-loop/run-1/1"],
                 "workspace_refs" => ["workspace://local/1"],
                 "worker_refs" => ["worker://fixture/local"],
                 "lower_refs" => ["lower-receipt://turn-1"],
                 "authority_refs" => ["authority-decision://turn-1"],
                 "outcome_refs" => ["action-receipt://turn-1"]
               }
             })

    dumped = AgentLoopProjection.dump(projection)
    assert dumped["runtime_events"] |> hd() |> Map.fetch!("event_kind") == "turn.started"
    assert dumped["receipt_ref_set"]["worker_refs"] == ["worker://fixture/local"]
    refute Map.has_key?(dumped, "raw_provider_payload")
  end

  defp agent_run_spec_attrs do
    %{
      tenant_ref: "tenant://local",
      installation_ref: "installation://local",
      profile_ref: "profile://stack-coder/local-fixture/v1",
      subject_ref: "subject://task/1",
      run_ref: "run://local/1",
      session_ref: "session://local/1",
      workspace_ref: "workspace://local/1",
      worker_ref: "worker://fixture/local",
      trace_id: "trace://local/1",
      idempotency_key: "agent-run:start:tenant-local:installation-local:subject-task-1:req-1",
      objective: "objective://task/1",
      runtime_profile_ref: :execution_plane_fixture,
      tool_catalog_ref: "tool-catalog://fixture/local-coding-v1",
      authority_context_ref: "authority-context://run-1",
      memory_profile_ref: :none,
      artifact_policy_ref: "artifact-policy://fixture/temp",
      max_turns: 3,
      timeout_policy: %{turn_timeout_ms: 1_000},
      profile_bundle: %{
        source_profile_ref: :synthetic_task,
        runtime_profile_ref: :execution_plane_fixture,
        tool_scope_ref: :local_coding_v1,
        evidence_profile_ref: :file_artifacts_v1,
        publication_profile_ref: :none,
        review_profile_ref: :operator_optional,
        memory_profile_ref: :none,
        projection_profile_ref: :runtime_readback_v1
      }
    }
  end

  defp turn_attrs do
    %{
      turn_ref: "turn://local/1/1",
      run_ref: "run://local/1",
      subject_ref: "subject://task/1",
      turn_index: 1,
      state: :initialized,
      started_at: ~U[2026-04-27 00:00:00Z],
      context_refs: [],
      semantic_fact_refs: [],
      trace_id: "trace://local/1",
      snapshot_epoch: 1,
      budget_before: %{turns_remaining: 3},
      budget_after: %{turns_remaining: 2}
    }
  end

  defp action_request_attrs do
    %{
      action_ref: "action://turn-1",
      turn_ref: "turn://local/1/1",
      run_ref: "run://local/1",
      profile_ref: "profile://stack-coder/local-fixture/v1",
      tool_ref: "fixture.record_note",
      capability_ref: "capability://fixture/record-note",
      input_artifact_ref: "artifact://input/turn-1",
      authority_context_ref: "authority-context://turn-1",
      idempotency_key: "agent-run:authority:turn-1:action-hash",
      trace_id: "trace://local/1"
    }
  end

  defp action_receipt_attrs do
    %{
      receipt_ref: "action-receipt://turn-1",
      action_ref: "action://turn-1",
      turn_ref: "turn://local/1/1",
      status: :succeeded,
      lower_receipt_ref: "lower-receipt://turn-1",
      output_artifact_refs: ["artifact://output/turn-1"],
      evidence_refs: ["evidence://turn-1"],
      retry_posture: :none,
      trace_id: "trace://local/1"
    }
  end
end
