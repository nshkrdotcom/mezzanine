defmodule Mezzanine.WorkflowRuntime.ObanTemporalCutoverTest do
  use ExUnit.Case, async: true

  alias Mezzanine.WorkflowRuntime.DurableOrchestrationDecision
  alias Mezzanine.WorkflowRuntime.RecoveryControl
  alias Mezzanine.WorkflowRuntime.RecoverySignalDispatcher

  @runtime_root Path.expand("../../../../..", __DIR__)

  test "recovery control replaces the volatile final-cutover and direct signal modules" do
    refute File.exists?(
             Path.join(
               @runtime_root,
               "core/workflow_runtime/lib/mezzanine/workflow_runtime/final_temporal_cutover.ex"
             )
           )

    refute File.exists?(
             Path.join(
               @runtime_root,
               "core/workflow_runtime/lib/mezzanine/workflow_runtime/operator_signal_control.ex"
             )
           )

    assert RecoveryControl.migration_version() == 20_260_728_130_000
    assert function_exported?(RecoverySignalDispatcher, :dispatch_once, 0)
    refute function_exported?(RecoveryControl, :dispatch_operator_signal, 1)
  end

  test "Oban is no longer the signal-control truth or delivery scheduler" do
    retained = DurableOrchestrationDecision.retained_oban_roles()
    scope = DurableOrchestrationDecision.oban_scope()

    refute Enum.any?(retained, &(&1.role == :workflow_signal_outbox))
    refute Enum.any?(scope, &(&1[:queue] == :workflow_signal_outbox))

    assert Enum.any?(retained, &(&1.role == :workflow_start_outbox))
    assert Enum.any?(retained, &(&1.role == :claim_check_gc))
  end
end
