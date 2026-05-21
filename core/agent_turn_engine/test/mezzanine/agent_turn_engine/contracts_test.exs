defmodule Mezzanine.AgentTurnEngine.ContractsTest do
  use ExUnit.Case, async: true

  alias Mezzanine.AgentTurnEngine.{
    AgentConversationEvent,
    AgentExecutionEvent,
    AgentPendingInteraction,
    AgentRunCursor,
    AgentTurnLedger,
    ExecutionReplay
  }

  @now ~U[2026-05-20 12:00:00Z]

  test "ledger accepts only product-safe refs and bounded status atoms" do
    assert {:ok, %AgentTurnLedger{status: :initialized, next_seq: 1}} =
             AgentTurnLedger.new(valid_ledger_attrs())

    assert {:error, {:invalid, :tenant_ref, :required}} =
             valid_ledger_attrs()
             |> Map.delete(:tenant_ref)
             |> AgentTurnLedger.new()

    assert {:error, {:invalid, :authority_ref, {:expected_prefix, "authority://"}}} =
             valid_ledger_attrs()
             |> Map.put(:authority_ref, "authority")
             |> AgentTurnLedger.new()

    assert {:error, {:invalid, :status, {:one_of, _allowed}}} =
             valid_ledger_attrs()
             |> Map.put(:status, :waiting_for_a2a_runtime)
             |> AgentTurnLedger.new()
  end

  test "ledger rejects raw credentials, static lower selectors, and raw endpoints" do
    assert {:error, {:invalid, :credential_material, :forbidden_key}} =
             valid_ledger_attrs()
             |> Map.put(:credential_material, "secret")
             |> AgentTurnLedger.new()

    assert {:error, {:invalid, :lower_selector, :forbidden_key}} =
             valid_ledger_attrs()
             |> Map.put(:lower_selector, "ExecutionPlane.Process.Transport")
             |> AgentTurnLedger.new()

    assert {:error, {:invalid, :payload_ref, :raw_endpoint}} =
             valid_ledger_attrs()
             |> Map.put(:payload_ref, "https://provider.example/raw")
             |> AgentTurnLedger.new()
  end

  test "conversation event validates visibility, redaction, refs, and sequence" do
    assert {:ok, %AgentConversationEvent{event_type: :assistant_message_available, seq: 1}} =
             AgentConversationEvent.new(valid_conversation_attrs())

    assert {:error, {:invalid, :seq, :positive_integer}} =
             valid_conversation_attrs()
             |> Map.put(:seq, 0)
             |> AgentConversationEvent.new()

    assert {:error, {:invalid, :visibility, {:one_of, _allowed}}} =
             valid_conversation_attrs()
             |> Map.put(:visibility, :provider_private)
             |> AgentConversationEvent.new()

    assert {:error, {:invalid, :raw_prompt, :forbidden_key}} =
             valid_conversation_attrs()
             |> Map.put(:raw_prompt, "hidden chain text")
             |> AgentConversationEvent.new()
  end

  test "execution event rejects protocol-specific leakage while allowing internal source classes" do
    assert {:ok, %AgentExecutionEvent{source: :execution_plane, seq: 2}} =
             AgentExecutionEvent.new(valid_execution_attrs())

    assert {:error, {:invalid, :protocol_module, :forbidden_key}} =
             valid_execution_attrs()
             |> Map.put(:protocol_module, "A2A.Bridge")
             |> AgentExecutionEvent.new()

    assert {:error, {:invalid, :payload_ref, :forbidden_value}} =
             valid_execution_attrs()
             |> Map.put(:payload_ref, "generated AX proto")
             |> AgentExecutionEvent.new()
  end

  test "cursor, replay, and pending interaction validate replay-safe refs" do
    assert {:ok, %AgentRunCursor{last_seq_seen: 7}} =
             AgentRunCursor.new(valid_cursor_attrs())

    assert {:error, {:invalid, :last_seq_seen, :non_negative_integer}} =
             valid_cursor_attrs()
             |> Map.put(:last_seq_seen, -1)
             |> AgentRunCursor.new()

    assert {:ok, %ExecutionReplay{lower_reexecution_allowed?: false}} =
             ExecutionReplay.new(valid_replay_attrs())

    assert {:error, {:invalid, :lower_reexecution_allowed?, :retry_policy_required}} =
             valid_replay_attrs()
             |> Map.put(:replay_kind, :catchup)
             |> Map.put(:lower_reexecution_allowed?, true)
             |> ExecutionReplay.new()

    assert {:ok, %AgentPendingInteraction{status: :open, kind: :approval_required}} =
             AgentPendingInteraction.new(valid_pending_attrs())

    assert {:error, {:invalid, :decision_ref, {:expected_prefix, "decision://"}}} =
             valid_pending_attrs()
             |> Map.put(:decision_ref, "review://wrong")
             |> AgentPendingInteraction.new()
  end

  def valid_ledger_attrs do
    %{
      ledger_ref: "agent-ledger://run-1",
      tenant_ref: "tenant://tenant-1",
      installation_ref: "installation://installation-1",
      subject_ref: "subject://subject-1",
      platform_run_ref: "run://platform-run-1",
      platform_execution_ref: "execution://execution-1",
      actor_ref: "actor://operator-1",
      authority_ref: "authority://policy-1",
      idempotency_key: "idem-ledger-1",
      status: :initialized,
      next_seq: 1,
      last_reduced_seq: 0,
      last_conversation_seq: 0,
      last_execution_seq: 0,
      cursor_ref: nil,
      replay_ref: nil,
      pending_interaction_ref: nil,
      created_at: @now,
      updated_at: @now
    }
  end

  def valid_conversation_attrs do
    %{
      event_ref: "agent-conv-event://event-1",
      ledger_ref: "agent-ledger://run-1",
      seq: 1,
      event_type: :assistant_message_available,
      visibility: :product,
      summary: "Assistant response is available.",
      payload_ref: "payload://conversation-1",
      redaction_class: :safe,
      authority_ref: "authority://policy-1",
      evidence_refs: ["evidence://summary-1"],
      occurred_at: @now
    }
  end

  def valid_execution_attrs do
    %{
      event_ref: "agent-exec-event://event-2",
      ledger_ref: "agent-ledger://run-1",
      seq: 2,
      event_type: :runtime_receipt_received,
      source: :execution_plane,
      idempotency_key: "idem-exec-2",
      causation_ref: "agent-conv-event://event-1",
      lower_receipt_ref: "receipt://lower-1",
      payload_hash: "sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
      payload_ref: "payload://execution-1",
      redaction_class: :internal,
      occurred_at: @now
    }
  end

  def valid_cursor_attrs do
    %{
      cursor_ref: "agent-cursor://cursor-1",
      ledger_ref: "agent-ledger://run-1",
      tenant_ref: "tenant://tenant-1",
      actor_ref: "actor://operator-1",
      last_seq_seen: 7,
      visibility: :operator,
      issued_at: @now,
      expires_at: DateTime.add(@now, 3600, :second)
    }
  end

  def valid_replay_attrs do
    %{
      replay_ref: "agent-replay://replay-1",
      ledger_ref: "agent-ledger://run-1",
      replay_kind: :catchup,
      from_seq: 0,
      to_seq: 7,
      lower_reexecution_allowed?: false,
      idempotency_key: "idem-replay-1",
      authority_ref: "authority://policy-1",
      evidence_refs: ["evidence://summary-1"],
      status: :planned,
      created_at: @now
    }
  end

  def valid_pending_attrs do
    %{
      pending_ref: "agent-pending://pending-1",
      ledger_ref: "agent-ledger://run-1",
      decision_ref: "decision://review-1",
      tenant_ref: "tenant://tenant-1",
      actor_ref: "actor://operator-1",
      kind: :approval_required,
      prompt_summary: "Approve file write?",
      requested_action_ref: "action://file-write-1",
      authority_ref: "authority://policy-1",
      opened_seq: 3,
      status: :open,
      expires_at: DateTime.add(@now, 3600, :second),
      resolved_at: nil
    }
  end
end
