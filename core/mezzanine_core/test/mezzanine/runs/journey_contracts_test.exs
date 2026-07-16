defmodule Mezzanine.Runs.JourneyContractsTest do
  use ExUnit.Case, async: true

  alias Mezzanine.Runs.{
    AcceptCommand,
    Acceptance,
    Event,
    EventCursor,
    FirstTurn,
    WorkflowHandoff
  }

  @hash "sha256:" <> String.duplicate("b", 64)
  @now ~U[2026-07-15 12:00:00Z]

  defp first_turn do
    FirstTurn.new!(
      turn_ref: "turn://synapse/run-1/1",
      subject_ref: "subject://synapse/run-1",
      input_artifact_ref: "artifact://outer-brain/input-1",
      payload_digest: @hash,
      idempotency_key: "synapse:run-1:turn-1",
      sequence: 1,
      row_version: 1
    )
  end

  defp event(sequence) do
    Event.new!(
      event_ref: "event://mezzanine/run-1/#{sequence}",
      run_ref: "run://mezzanine/run-1",
      tenant_ref: "tenant://acme",
      event_type: :run_accepted,
      event_version: 1,
      sequence: sequence,
      command_ref: "command://mezzanine/run-1",
      correlation_ref: "correlation://synapse/run-1",
      payload_ref: "artifact://mezzanine/run-1-event-#{sequence}",
      payload_digest: @hash,
      recorded_at: @now,
      row_version: sequence
    )
  end

  test "accept command carries only refs, hashes, and the first-turn contract" do
    assert {:ok, command} =
             AcceptCommand.new(
               command_ref: "command://mezzanine/run-1",
               idempotency_key: "synapse:run-1",
               request_hash: @hash,
               tenant_ref: "tenant://acme",
               installation_ref: "installation://acme/synapse/prod",
               actor_ref: "actor://synapse/operator",
               subject_ref: "subject://synapse/run-1",
               run_ref: "run://mezzanine/run-1",
               trace_ref: "trace://synapse/run-1",
               correlation_ref: "correlation://synapse/run-1",
               authority_context_ref: "authority-context://synapse/run-1",
               runtime_profile_ref: "runtime-profile://nshkr/local-model",
               tool_catalog_ref: "tool-catalog://synapse/default",
               budget_ref: "budget://synapse/default",
               expected_revision: 0,
               first_turn: first_turn()
             )

    assert command.first_turn.sequence == 1
    assert AcceptCommand.dump(command)["request_hash"] == @hash
  end

  test "accept command rejects raw prompts and non-initial revisions" do
    base = %{
      command_ref: "command://mezzanine/run-1",
      idempotency_key: "synapse:run-1",
      request_hash: @hash,
      tenant_ref: "tenant://acme",
      installation_ref: "installation://acme/synapse/prod",
      actor_ref: "actor://synapse/operator",
      subject_ref: "subject://synapse/run-1",
      run_ref: "run://mezzanine/run-1",
      trace_ref: "trace://synapse/run-1",
      correlation_ref: "correlation://synapse/run-1",
      authority_context_ref: "authority-context://synapse/run-1",
      runtime_profile_ref: "runtime-profile://nshkr/local-model",
      tool_catalog_ref: "tool-catalog://synapse/default",
      budget_ref: "budget://synapse/default",
      expected_revision: 0,
      first_turn: first_turn()
    }

    assert {:error, :invalid_accept_command} = AcceptCommand.new(Map.put(base, :prompt, "secret"))

    assert {:error, :invalid_accept_command} =
             base |> Map.put(:expected_revision, 2) |> AcceptCommand.new()
  end

  test "cursor advances only through contiguous events from the same run" do
    cursor =
      EventCursor.new!(
        run_ref: "run://mezzanine/run-1",
        last_event_ref: "event://mezzanine/run-1/1",
        sequence: 1
      )

    assert {:ok, advanced} = EventCursor.advance(cursor, event(2))
    assert advanced.sequence == 2
    assert {:error, :non_contiguous_event} = EventCursor.advance(cursor, event(3))
  end

  test "workflow handoff exposes ambiguity and terminal transition rules" do
    handoff =
      WorkflowHandoff.new!(
        outbox_ref: "outbox://mezzanine/run-1/start",
        event_ref: "event://mezzanine/run-1/1",
        run_ref: "run://mezzanine/run-1",
        workflow_ref: "workflow://temporal/run-1",
        workflow_type: "mezzanine.agent-run.v1",
        temporal_namespace: "nshkr-production",
        task_queue: "nshkr.mezzanine.agent-run.v1",
        idempotency_key: "workflow:start:run-1",
        state: "pending",
        attempt: 0
      )

    assert {:ok, dispatched} = WorkflowHandoff.transition(handoff, :dispatched)
    assert dispatched.attempt == 1

    assert {:ok, ambiguous} =
             WorkflowHandoff.transition(dispatched, :ambiguous, "error://temporal/lost-reply")

    assert {:ok, acknowledged} = WorkflowHandoff.transition(ambiguous, :acknowledged)

    assert {:error, :invalid_workflow_handoff_transition} =
             WorkflowHandoff.transition(acknowledged, :dispatched)
  end

  test "acceptance binds the returned cursor to the committed event" do
    assert {:ok, acceptance} =
             Acceptance.new(
               command_ref: "command://mezzanine/run-1",
               run_ref: "run://mezzanine/run-1",
               turn_ref: "turn://synapse/run-1/1",
               event_ref: "event://mezzanine/run-1/1",
               workflow_outbox_ref: "outbox://mezzanine/run-1/start",
               cursor: %{
                 run_ref: "run://mezzanine/run-1",
                 last_event_ref: "event://mezzanine/run-1/1",
                 sequence: 1
               },
               run_revision: 1,
               state: :accepted
             )

    assert acceptance.cursor.sequence == 1
  end
end
