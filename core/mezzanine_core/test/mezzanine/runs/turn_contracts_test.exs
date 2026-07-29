defmodule Mezzanine.Runs.TurnContractsTest do
  use ExUnit.Case, async: true

  alias Mezzanine.Runs.{TurnAcceptance, TurnCommand, TurnProjection}

  @hash "sha256:" <> String.duplicate("a", 64)

  test "validates a reference-only durable turn command" do
    assert {:ok, command} = TurnCommand.new(command_attrs())
    assert command.kind == :user_input

    assert {:error, :invalid_turn_command} =
             command_attrs()
             |> Map.put(:params, %{raw_payload: "forbidden"})
             |> TurnCommand.new()

    assert {:error, :invalid_turn_command} =
             command_attrs()
             |> Map.put(:kind, :provider_future_kind)
             |> TurnCommand.new()
  end

  test "binds turn acceptance to the same durable run cursor" do
    attrs = %{
      command_ref: "command://mezzanine/turn/one",
      run_ref: "run://mezzanine/one",
      turn_ref: "turn://mezzanine/one/2",
      event_ref: "event://mezzanine/one/2",
      signal_outbox_ref: "outbox://mezzanine/turn/one",
      cursor: %{
        run_ref: "run://mezzanine/one",
        last_event_ref: "event://mezzanine/one/2",
        sequence: 2
      },
      run_revision: 2,
      state: :accepted,
      idempotent_replay?: false
    }

    assert {:ok, acceptance} = TurnAcceptance.new(attrs)
    assert acceptance.state == "accepted"

    assert {:error, :invalid_turn_acceptance} =
             attrs
             |> put_in([:cursor, :run_ref], "run://mezzanine/other")
             |> TurnAcceptance.new()
  end

  test "validates refs-only ordered turn projection state" do
    now = DateTime.utc_now()

    attrs = %{
      turn_ref: "turn://mezzanine/one/2",
      run_ref: "run://mezzanine/one",
      tenant_ref: "tenant://default",
      subject_ref: "subject://synapse/one",
      input_artifact_ref: "artifact://synapse/one/turn-2",
      payload_digest: @hash,
      sequence: 2,
      status: :accepted,
      provider_attempt_ref: nil,
      row_version: 1,
      updated_at: now
    }

    assert {:ok, projection} = TurnProjection.new(attrs)
    assert projection.status == "accepted"
    assert projection.updated_at == now

    assert {:error, :invalid_turn_projection} =
             attrs
             |> Map.put(:input_artifact_ref, nil)
             |> TurnProjection.new()
  end

  defp command_attrs do
    %{
      command_ref: "command://mezzanine/turn/one",
      idempotency_key: "synapse:turn:one",
      request_hash: @hash,
      tenant_ref: "tenant://default",
      actor_ref: "actor://synapse/operator",
      authority_ref: "authority://mezzanine/agent-intake",
      run_ref: "run://mezzanine/one",
      turn_ref: "turn://mezzanine/one/2",
      trace_ref: "trace://synapse/one",
      correlation_ref: "correlation://synapse/one",
      kind: :user_input,
      payload_ref: "payload://synapse/turn/one",
      payload_digest: @hash,
      cursor_ref: "event://mezzanine/one/1",
      pending_ref: nil,
      params: %{input_summary: "Continue"}
    }
  end
end
