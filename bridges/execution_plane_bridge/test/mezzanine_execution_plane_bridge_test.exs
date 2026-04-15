defmodule Mezzanine.ExecutionPlaneBridgeTest do
  use ExUnit.Case, async: true

  alias Mezzanine.ExecutionPlaneBridge
  alias MezzanineOpsModel.Intent.{EffectIntent, ReadIntent, RunIntent}

  test "dispatch_run returns a typed not-supported error" do
    intent =
      RunIntent.new!(%{
        intent_id: "run-ep-1",
        program_id: "program-1",
        work_id: "work-1",
        capability: "linear.issue.execute",
        input: %{}
      })

    assert {:error, {:not_supported, %{bridge: :execution_plane, intent_type: :run}}} =
             ExecutionPlaneBridge.dispatch_run(intent)
  end

  test "dispatch_effect returns a typed not-supported error" do
    intent =
      EffectIntent.new!(%{
        intent_id: "effect-ep-1",
        effect_type: :connector_effect,
        subject: "issue"
      })

    assert {:error, {:not_supported, %{bridge: :execution_plane, intent_type: :effect}}} =
             ExecutionPlaneBridge.dispatch_effect(intent)
  end

  test "dispatch_read returns a typed not-supported error" do
    intent =
      ReadIntent.new!(%{
        intent_id: "read-ep-1",
        read_type: :connector_read,
        subject: "issue"
      })

    assert {:error, {:not_supported, %{bridge: :execution_plane, intent_type: :read}}} =
             ExecutionPlaneBridge.dispatch_read(intent)
  end
end
