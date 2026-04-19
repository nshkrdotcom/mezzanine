defmodule Mezzanine.WorkflowRuntime.ObanTemporalCutoverTest do
  use ExUnit.Case, async: true

  alias Mezzanine.WorkflowRuntime.DurableOrchestrationDecision
  alias Mezzanine.WorkflowRuntime.FinalTemporalCutover

  @mezzanine_root Path.expand("../../../../..", __DIR__)

  test "final cutover manifest keeps Oban only for approved local duties" do
    assert %{
             contract_name: "Mezzanine.FinalTemporalCutoverManifest.v1",
             oban_scope_contract: "Mezzanine.ObanTemporalScope.v1",
             temporal_boundary: Mezzanine.WorkflowRuntime,
             temporalex_boundary: Mezzanine.WorkflowRuntime.TemporalexBoundary,
             retained_oban_queues: retained_queues,
             retired_oban_saga_workers: retired_workers
           } = FinalTemporalCutover.manifest()

    assert retained_queues == [:claim_check_gc, :workflow_signal_outbox, :workflow_start_outbox]

    assert Enum.map(retired_workers, & &1.worker) == [
             "Mezzanine.ExecutionDispatchWorker",
             "Mezzanine.ExecutionReceiptWorker",
             "Mezzanine.ExecutionReconcileWorker",
             "Mezzanine.JoinAdvanceWorker",
             "Mezzanine.LifecycleContinuationWorker",
             "Mezzanine.ExecutionCancelWorker"
           ]

    refute Enum.any?(
             DurableOrchestrationDecision.oban_scope(),
             &(&1.classification == :invalid_saga_orchestration)
           )
  end

  test "source scan rejects active Oban saga workers and invalid saga queues" do
    assert FinalTemporalCutover.active_oban_worker_modules(@mezzanine_root) == [
             "Mezzanine.WorkflowRuntime.ClaimCheckGcWorker",
             "Mezzanine.WorkflowRuntime.WorkflowSignalOutboxWorker",
             "Mezzanine.WorkflowRuntime.WorkflowStarterOutboxWorker"
           ]

    assert FinalTemporalCutover.invalid_oban_queue_configs(@mezzanine_root) == []
    assert FinalTemporalCutover.invalid_oban_saga_references(@mezzanine_root) == []
  end
end
