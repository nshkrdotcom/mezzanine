defmodule MezzanineCoreTest do
  use ExUnit.Case, async: true

  alias Mezzanine.Intent.{EffectIntent, ReadIntent, RunIntent}
  alias Mezzanine.Telemetry

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

  test "emits the canonical Stage 11 telemetry namespace with dotted event metadata" do
    handler_id = {__MODULE__, make_ref()}

    :telemetry.attach(
      handler_id,
      [:mezzanine, :dispatch, :accepted],
      &__MODULE__.handle_telemetry_event/4,
      self()
    )

    try do
      assert :ok =
               Telemetry.emit(
                 [:dispatch, :accepted],
                 %{count: 1, latency_ms: 12},
                 %{
                   trace_id: "trace-1",
                   subject_id: "subject-1",
                   execution_id: "execution-1",
                   submission_dedupe_key: "dedupe-1",
                   tenant_id: "tenant-1"
                 }
               )

      assert_receive {:telemetry_event, [:mezzanine, :dispatch, :accepted], measurements,
                      metadata}

      assert measurements == %{count: 1, latency_ms: 12}
      assert metadata.event_name == "dispatch.accepted"
      assert metadata.trace_id == "trace-1"
      assert metadata.subject_id == "subject-1"
      assert metadata.execution_id == "execution-1"
      assert metadata.submission_dedupe_key == "dedupe-1"
      assert metadata.tenant_id == "tenant-1"
      assert Map.has_key?(metadata, :decision_id)
      assert Map.has_key?(metadata, :lease_id)
    after
      :telemetry.detach(handler_id)
    end
  end

  def handle_telemetry_event(event, measurements, metadata, pid) do
    send(pid, {:telemetry_event, event, measurements, metadata})
  end
end
