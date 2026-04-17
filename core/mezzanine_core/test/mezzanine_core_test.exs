defmodule MezzanineCoreTest do
  use ExUnit.Case, async: true

  alias Mezzanine.Intent.{EffectIntent, ReadIntent, RunIntent}

  test "describes the initial reusable posture" do
    assert %{
             role: :business_semantics_substrate,
             posture: :configurable
           } = MezzanineCore.identity()
  end

  test "declares the first configuration axes" do
    assert :workflow in MezzanineCore.configuration_axes()
    assert :tenancy in MezzanineCore.configuration_axes()
  end

  test "exposes the frozen boundary generation posture" do
    assert [
             Mezzanine.Boundary.GenerationManifest,
             Mezzanine.Boundary.GenerationSpec
           ] == MezzanineCore.contract_modules()
  end

  test "owns the neutral lower-intent structs shared by active bridges" do
    assert {:ok, run_intent} =
             RunIntent.new(%{
               intent_id: "intent-run-1",
               program_id: "program-1",
               work_id: "work-1",
               capability: "linear.issue.execute"
             })

    assert {:ok, read_intent} =
             ReadIntent.new(%{
               intent_id: "intent-read-1",
               read_type: :lower_fact,
               subject: %{execution_id: "exec-1"}
             })

    assert {:ok, effect_intent} =
             EffectIntent.new(%{
               intent_id: "intent-effect-1",
               effect_type: :connector_effect,
               subject: "issue"
             })

    assert %RunIntent{capability: "linear.issue.execute"} = run_intent
    assert %ReadIntent{read_type: :lower_fact} = read_intent
    assert %EffectIntent{effect_type: :connector_effect} = effect_intent
  end
end
