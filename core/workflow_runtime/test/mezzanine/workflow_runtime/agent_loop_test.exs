defmodule Mezzanine.WorkflowRuntime.AgentLoopTest do
  use ExUnit.Case, async: false
  use Temporalex.Testing

  alias Mezzanine.AgentRuntime.AgentLoopProjection
  alias Mezzanine.WorkflowRuntime.AgentLoop

  test "declares M2 contract, linkage strategy, and activity owners" do
    contract = AgentLoop.contract()

    assert contract.workflow_module == Mezzanine.Workflows.AgentLoop
    assert contract.mechanism == "M2"
    assert contract.lower_attempt_linkage.strategy == :outbox_activity_handoff

    assert contract.lower_attempt_linkage.lower_workflow ==
             Mezzanine.WorkflowRuntime.ExecutionLifecycleWorkflow

    assert :wake_and_pin in contract.activity_sequence
    assert contract.activity_owners.reflect == :outer_brain
    assert contract.activity_owners.govern == :citadel
    assert contract.activity_owners.submit_lower_run == :jido_integration
    assert contract.activity_owners.await_execution_outcome == :execution_plane
  end

  test "runs one deterministic provider-free turn and emits M1-readable projection rows" do
    assert {:ok, %AgentLoopProjection{} = projection} =
             AgentLoop.run(agent_run_spec_attrs())

    assert projection.terminal_state == "completed"
    assert [turn] = projection.turn_states
    assert turn.state == :completed
    assert [request] = projection.action_requests
    assert request.tool_ref == "fixture.record_note"
    assert [receipt] = projection.action_receipts
    assert receipt.status == :succeeded
    assert projection.session_ref == "session://local/1"
    assert projection.workspace_ref == "workspace://local/1"
    assert projection.worker_ref == "worker://fixture/local"

    assert projection.receipt_ref_set["session_refs"] == ["session://local/1"]
    assert projection.receipt_ref_set["workspace_refs"] == ["workspace://local/1"]
    assert projection.receipt_ref_set["worker_refs"] == ["worker://fixture/local"]
    assert projection.receipt_ref_set["lower_refs"] != []
    assert projection.receipt_ref_set["authority_refs"] != []
    assert projection.receipt_ref_set["outcome_refs"] != []

    event_kinds = Enum.map(projection.runtime_events, & &1.event_kind)
    assert "agent_run.accepted" in event_kinds
    assert "turn.started" in event_kinds
    assert "action.requested" in event_kinds
    assert "receipt.observed" in event_kinds
    assert "run.terminal" in event_kinds

    refute inspect(AgentLoopProjection.dump(projection)) =~ "raw_provider_payload"
    refute inspect(AgentLoopProjection.dump(projection)) =~ "/home/"
  end

  test "Temporal workflow calls explicit activity contracts" do
    assert {:ok, %AgentLoopProjection{} = projection} =
             run_workflow(Mezzanine.Workflows.AgentLoop, agent_run_spec_attrs(),
               activities: %{
                 Mezzanine.Activities.AgentLoopWakeAndPin => &AgentLoop.wake_and_pin_activity/1,
                 Mezzanine.Activities.AgentLoopRecall => &AgentLoop.recall_activity/1,
                 Mezzanine.Activities.AgentLoopAssembleContext =>
                   &AgentLoop.assemble_context_activity/1,
                 Mezzanine.Activities.AgentLoopReflect => &AgentLoop.reflect_activity/1,
                 Mezzanine.Activities.AgentLoopGovern => &AgentLoop.govern_activity/1,
                 Mezzanine.Activities.AgentLoopSubmitLowerRun =>
                   &AgentLoop.submit_lower_run_activity/1,
                 Mezzanine.Activities.AgentLoopAwaitExecutionOutcome =>
                   &AgentLoop.await_execution_outcome_activity/1,
                 Mezzanine.Activities.AgentLoopSemanticizeOutcome =>
                   &AgentLoop.semanticize_outcome_activity/1,
                 Mezzanine.Activities.AgentLoopCommitPrivateMemory =>
                   &AgentLoop.commit_private_memory_activity/1,
                 Mezzanine.Activities.AgentLoopAdvanceTurn => &AgentLoop.advance_turn_activity/1
               }
             )

    assert projection.status == "completed"
    assert_activity_called(Mezzanine.Activities.AgentLoopWakeAndPin)
    assert_activity_called(Mezzanine.Activities.AgentLoopReflect)
    assert_activity_called(Mezzanine.Activities.AgentLoopGovern)
    assert_activity_called(Mezzanine.Activities.AgentLoopSubmitLowerRun)
    assert_activity_called(Mezzanine.Activities.AgentLoopAdvanceTurn)
  end

  test "denied action becomes durable loop data and never submits lower work" do
    attrs = put_in(agent_run_spec_attrs(), [:fixture_script], "denied_write_then_allowed_read")

    assert {:ok, projection} = AgentLoop.run(attrs)
    assert projection.terminal_state == "blocked"
    assert projection.action_receipts |> hd() |> Map.fetch!(:status) == :denied
    assert Enum.any?(projection.runtime_events, &(&1.event_kind == "authority.denied"))
    refute Enum.any?(projection.runtime_events, &(&1.event_kind == "action.submitted"))
  end

  test "approval wait, duplicate signals, and early outcomes are replay-safe" do
    assert {:ok, projection} =
             AgentLoop.run(
               Map.put(agent_run_spec_attrs(), :fixture_script, "approval_wait_then_submit")
             )

    assert projection.terminal_state == "review_pending"
    assert Enum.any?(projection.runtime_events, &(&1.event_kind == "review.pending"))

    state = AgentLoop.initial_state(agent_run_spec_attrs())
    signal = %{signal_id: "signal-1", signal_name: "approve", idempotency_key: "idem-signal-1"}

    assert {:ok, approved} = AgentLoop.apply_signal(state, signal)
    assert {:ok, duplicate} = AgentLoop.apply_signal(approved, signal)
    assert duplicate.signal_state == "duplicate_suppressed"

    assert {:ok, buffered} =
             AgentLoop.buffer_outcome_signal(state, %{
               lower_submission_ref: "lower-submission://turn-1",
               submission_dedupe_key: "agent-run:lower:turn-1:decision-1",
               lower_receipt_ref: "lower-receipt://turn-1"
             })

    assert {:ok, outcome, consumed} =
             AgentLoop.consume_buffered_outcome(buffered, "lower-submission://turn-1")

    assert outcome.lower_receipt_ref == "lower-receipt://turn-1"
    assert consumed.buffered_outcomes == %{}
  end

  test "run detail readback can be built from an M2 projection using M1-shaped data" do
    assert {:ok, projection} = AgentLoop.run(agent_run_spec_attrs())
    assert {:ok, detail} = AgentLoop.to_runtime_run_detail(projection)

    assert detail["run_ref"] == projection.run_ref
    assert detail["turns"] != []
    assert detail["events"] |> Enum.map(& &1["event_kind"]) |> Enum.member?("run.terminal")
    assert detail["budget_state"]["turns_remaining"] == 2
    assert detail["receipt_ref_set"]["session_refs"] == ["session://local/1"]
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
      continue_as_new_turn_threshold: 50,
      fixture_script: "success_first_try",
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
end
