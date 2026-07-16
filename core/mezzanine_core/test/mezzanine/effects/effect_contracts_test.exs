defmodule Mezzanine.Effects.EffectContractsTest do
  use ExUnit.Case, async: true

  alias Mezzanine.Effects.{EffectRecord, Lifecycle}

  defp authorized do
    EffectRecord.new!(
      effect_ref: "effect://mezzanine/codex/run-1",
      run_ref: "run://mezzanine/run-1",
      turn_ref: "turn://mezzanine/run-1/2",
      command_ref: "command://mezzanine/codex/run-1",
      decision_ref: "decision://citadel/codex/run-1",
      grant_ref: "grant://citadel/codex/run-1",
      review_ref: "review://mezzanine/codex/run-1",
      idempotency_key: "codex:run-1:effect-1",
      target_ref: "target://nshkr/local-process",
      status: :authorized,
      row_version: 1
    )
  end

  test "binds an authorized effect to exact authority and review refs" do
    record = authorized()
    assert record.status == "authorized"
    assert EffectRecord.dump(record)["review_ref"] == "review://mezzanine/codex/run-1"

    assert {:error, :invalid_effect_record} =
             record
             |> Map.from_struct()
             |> Map.put(:raw_prompt, "sentinel-secret")
             |> EffectRecord.new()
  end

  test "requires an attempt before dispatch and opaque execution identity before running" do
    assert {:error, :invalid_effect_transition} =
             Lifecycle.transition(authorized(), :dispatching, expected_row_version: 1)

    assert {:ok, dispatching} =
             Lifecycle.transition(authorized(), :dispatching,
               expected_row_version: 1,
               attempt_ref: "attempt://jido/codex/run-1/1"
             )

    assert {:ok, running} =
             Lifecycle.transition(dispatching, :running,
               expected_row_version: 2,
               execution_ref: "session://asm/codex/run-1/generation-1"
             )

    assert running.row_version == 3
  end

  test "completed effects require a terminal receipt and result artifact" do
    running =
      authorized()
      |> Lifecycle.transition(:dispatching,
        expected_row_version: 1,
        attempt_ref: "attempt://jido/codex/run-1/1"
      )
      |> elem(1)
      |> Lifecycle.transition(:running,
        expected_row_version: 2,
        execution_ref: "session://asm/codex/run-1/generation-1"
      )
      |> elem(1)

    assert {:error, :invalid_effect_transition} =
             Lifecycle.transition(running, :completed,
               expected_row_version: 3,
               receipt_ref: "receipt://cli-core/codex/run-1"
             )

    assert {:ok, completed} =
             Lifecycle.transition(running, :completed,
               expected_row_version: 3,
               receipt_ref: "receipt://cli-core/codex/run-1",
               result_artifact_ref: "artifact://outer-brain/codex-result-1"
             )

    assert completed.status == "completed"
  end

  test "ambiguous outcome is explicit and can only resolve with terminal evidence" do
    dispatching =
      authorized()
      |> Lifecycle.transition(:dispatching,
        expected_row_version: 1,
        attempt_ref: "attempt://jido/codex/run-1/1"
      )
      |> elem(1)

    assert {:error, :invalid_effect_transition} =
             Lifecycle.transition(dispatching, :ambiguous, expected_row_version: 2)

    assert {:ok, ambiguous} =
             Lifecycle.transition(dispatching, :ambiguous,
               expected_row_version: 2,
               ambiguity_state: :dispatch_unknown
             )

    assert {:error, :invalid_effect_transition} =
             Lifecycle.transition(ambiguous, :completed,
               expected_row_version: 2,
               receipt_ref: "receipt://cli-core/codex/run-1",
               result_artifact_ref: "artifact://outer-brain/codex-result-1"
             )

    assert {:ok, completed} =
             Lifecycle.transition(ambiguous, :completed,
               expected_row_version: 3,
               execution_ref: "session://asm/codex/run-1/generation-1",
               receipt_ref: "receipt://cli-core/codex/run-1",
               ambiguity_state: nil,
               result_artifact_ref: "artifact://outer-brain/codex-result-1"
             )

    assert completed.status == "completed"
  end
end
