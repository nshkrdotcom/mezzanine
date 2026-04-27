defmodule Mezzanine.WorkflowRuntime.DurableOrchestrationDecisionTest do
  use ExUnit.Case, async: true

  alias Mezzanine.WorkflowRuntime.DurableOrchestrationDecision
  alias Mezzanine.WorkflowRuntime.TemporalexBoundary
  alias Mezzanine.WorkflowRuntime.TemporalRegistry

  @mezzanine_root Path.expand("../../../../..", __DIR__)
  @temporalex_root "/home/home/p/g/n/temporalex"

  test "declares direct temporalex runtime ownership and local path dependencies" do
    assert DurableOrchestrationDecision.integration_mode() == :direct_temporalex_beam_workers
    assert DurableOrchestrationDecision.temporalex_root() == @temporalex_root

    assert %{
             endpoint: "127.0.0.1:7233",
             namespace: "default",
             adapter: Mezzanine.WorkflowRuntime.TemporalexAdapter,
             supervisor: Mezzanine.WorkflowRuntime.TemporalSupervisor,
             rust_core_posture: "Temporal Rust Core via temporalex Rustler NIF"
           } = DurableOrchestrationDecision.runtime_refs()

    assert %{dependency: {:temporalex, path: "../temporalex"}} =
             Enum.find(
               DurableOrchestrationDecision.temporalex_dependency_paths(),
               &(&1.mix_exs == "mix.exs")
             )

    assert %{dependency: {:temporalex, path: "../../../temporalex"}} =
             Enum.find(
               DurableOrchestrationDecision.temporalex_dependency_paths(),
               &(&1.mix_exs == "core/workflow_runtime/mix.exs")
             )

    assert File.read!(Path.join(@mezzanine_root, "mix.exs")) =~
             ~s({:temporalex, path: "../temporalex"})

    assert File.read!(Path.join(@mezzanine_root, "core/workflow_runtime/mix.exs")) =~
             ~s({:temporalex, path: "../../../temporalex"})
  end

  test "registers final workflow and activity modules through temporalex" do
    workflow_modules = TemporalRegistry.workflows()
    activity_modules = TemporalRegistry.activities()

    assert workflow_modules == [
             Mezzanine.Workflows.AgentLoop,
             Mezzanine.Workflows.AgentRun,
             Mezzanine.Workflows.ExecutionAttempt,
             Mezzanine.Workflows.DecisionReview,
             Mezzanine.Workflows.JoinBarrier,
             Mezzanine.Workflows.IncidentReconstruction
           ]

    assert Mezzanine.Activities.StartLowerExecution in activity_modules
    assert Mezzanine.Activities.CallOuterBrain in activity_modules
    assert Mezzanine.Activities.AgentLoopWakeAndPin in activity_modules
    assert Mezzanine.Activities.AgentLoopReflect in activity_modules
    assert Mezzanine.Activities.AgentLoopAdvanceTurn in activity_modules
    assert Mezzanine.Activities.SubmitJidoLowerActivity in activity_modules
    assert Mezzanine.Activities.ExecutionSideEffectActivity in activity_modules
    assert Mezzanine.Activities.SemanticPayloadBoundaryActivity in activity_modules
    assert Mezzanine.Activities.CompensateCancelledRun in activity_modules
    assert Mezzanine.Activities.CleanupWorkspace in activity_modules
    assert Mezzanine.Activities.PublishSource in activity_modules
    assert Mezzanine.Activities.MaterializeEvidence in activity_modules
    assert Mezzanine.Activities.CreateReview in activity_modules

    for workflow <- workflow_modules do
      assert Code.ensure_loaded?(workflow)
      assert function_exported?(workflow, :__workflow_type__, 0)
      assert function_exported?(workflow, :run, 1)
    end

    for activity <- activity_modules do
      assert Code.ensure_loaded?(activity)
      assert function_exported?(activity, :__activity_type__, 0)
      assert function_exported?(activity, :perform, 1)
    end
  end

  test "workflow and activity modules are Temporalex runtime modules, not skeletons" do
    expected_workflow_queues = %{
      Mezzanine.Workflows.AgentLoop => "mezzanine.agentic",
      Mezzanine.Workflows.AgentRun => "mezzanine.agentic",
      Mezzanine.Workflows.ExecutionAttempt => "mezzanine.hazmat",
      Mezzanine.Workflows.DecisionReview => "mezzanine.review",
      Mezzanine.Workflows.JoinBarrier => "mezzanine.agentic",
      Mezzanine.Workflows.IncidentReconstruction => "mezzanine.agentic"
    }

    for {workflow, task_queue} <- expected_workflow_queues do
      assert workflow.__workflow_defaults__()[:task_queue] == task_queue
      assert function_exported?(workflow, :handle_query, 3)
    end

    expected_activity_queues = %{
      Mezzanine.Activities.AgentLoopWakeAndPin => "mezzanine.agentic",
      Mezzanine.Activities.AgentLoopRecall => "mezzanine.semantic",
      Mezzanine.Activities.AgentLoopAssembleContext => "mezzanine.semantic",
      Mezzanine.Activities.AgentLoopReflect => "mezzanine.semantic",
      Mezzanine.Activities.AgentLoopGovern => "mezzanine.agentic",
      Mezzanine.Activities.AgentLoopSubmitLowerRun => "mezzanine.hazmat",
      Mezzanine.Activities.AgentLoopAwaitExecutionOutcome => "mezzanine.hazmat",
      Mezzanine.Activities.AgentLoopSemanticizeOutcome => "mezzanine.semantic",
      Mezzanine.Activities.AgentLoopCommitPrivateMemory => "mezzanine.agentic",
      Mezzanine.Activities.AgentLoopAdvanceTurn => "mezzanine.agentic",
      Mezzanine.Activities.StartLowerExecution => "mezzanine.hazmat",
      Mezzanine.Activities.RecordEvidence => "mezzanine.agentic",
      Mezzanine.Activities.RequestDecision => "mezzanine.agentic",
      Mezzanine.Activities.CallOuterBrain => "mezzanine.semantic",
      Mezzanine.Activities.ReconcileLowerRun => "mezzanine.agentic",
      Mezzanine.Activities.CompensateCancelledRun => "mezzanine.hazmat",
      Mezzanine.Activities.SubmitJidoLowerActivity => "mezzanine.hazmat",
      Mezzanine.Activities.ExecutionSideEffectActivity => "mezzanine.hazmat",
      Mezzanine.Activities.SemanticPayloadBoundaryActivity => "mezzanine.semantic",
      Mezzanine.Activities.CleanupWorkspace => "mezzanine.agentic",
      Mezzanine.Activities.PublishSource => "mezzanine.agentic",
      Mezzanine.Activities.MaterializeEvidence => "mezzanine.agentic",
      Mezzanine.Activities.CreateReview => "mezzanine.review"
    }

    for {activity, task_queue} <- expected_activity_queues do
      defaults = activity.__activity_defaults__()

      assert defaults[:task_queue] == task_queue
      assert is_integer(defaults[:start_to_close_timeout])
      assert defaults[:retry_policy][:max_attempts] == 3
      assert %Temporalex.RetryPolicy{} = Temporalex.RetryPolicy.from_opts(defaults[:retry_policy])
    end

    source =
      File.read!(
        Path.join(
          @mezzanine_root,
          "core/workflow_runtime/lib/mezzanine/workflow_runtime/durable_orchestration_decision.ex"
        )
      )

    refute source =~ "workflow skeleton"
    refute source =~ "activity skeleton"
    assert source =~ "execute_activity(Mezzanine.Activities.CallOuterBrain"
    assert source =~ ~s(task_queue: "mezzanine.semantic")
    assert source =~ "execute_activity(Mezzanine.Activities.RequestDecision"
    assert source =~ ~s(task_queue: "mezzanine.agentic")
  end

  test "declares the concrete Temporalex adapter and worker supervision contract" do
    assert DurableOrchestrationDecision.runtime_adapter() ==
             Mezzanine.WorkflowRuntime.TemporalexAdapter

    assert %{
             application: Mezzanine.WorkflowRuntime.Application,
             supervisor: Mezzanine.WorkflowRuntime.TemporalSupervisor,
             worker_child: Temporalex,
             workflow_runtime_impl_config:
               {:mezzanine_core, :workflow_runtime_impl,
                Mezzanine.WorkflowRuntime.TemporalexAdapter},
             temporal_config_app: :mezzanine_workflow_runtime,
             temporal_config_key: :temporal,
             enabled_default: false,
             endpoint: "127.0.0.1:7233",
             namespace: "default"
           } = DurableOrchestrationDecision.temporal_supervision()

    assert "mezzanine.hazmat" in DurableOrchestrationDecision.temporal_supervision().task_queues
  end

  test "keeps public workflow boundary stable while mapping internally to Temporalex.Client" do
    assert TemporalexBoundary.client_module() == Temporalex.Client

    assert %{
             start_workflow: {Temporalex.Client, :start_workflow, 4},
             signal_workflow: {Temporalex.Client, :signal_workflow, 5},
             query_workflow: {Temporalex.Client, :query_workflow, 5},
             cancel_workflow: {Temporalex.Client, :cancel_workflow, 3},
             terminate_workflow: {Temporalex.Client, :terminate_workflow, 3},
             describe_workflow: {Temporalex.Client, :describe_workflow, 3},
             list_workflows: {Temporalex.Client, :list_workflows, 3}
           } = TemporalexBoundary.client_calls()

    assert Code.ensure_loaded?(Temporalex.Testing)
    assert Code.ensure_loaded?(Mezzanine.WorkflowRuntime.TemporalexBoundary)
  end

  test "search attributes are registry-defined scalar values only" do
    registry = DurableOrchestrationDecision.search_attribute_registry()

    assert Enum.all?(
             registry,
             &(&1.type in DurableOrchestrationDecision.allowed_search_attribute_types())
           )

    assert Enum.all?(registry, &String.starts_with?(&1.key, "phase4."))
    refute Enum.any?(registry, &String.contains?(&1.key, "raw"))

    assert DurableOrchestrationDecision.scalar_search_attribute_value?("workflow-ref")
    assert DurableOrchestrationDecision.scalar_search_attribute_value?(true)
    assert DurableOrchestrationDecision.scalar_search_attribute_value?(42)
    assert DurableOrchestrationDecision.scalar_search_attribute_value?(0.98)
    assert DurableOrchestrationDecision.scalar_search_attribute_value?(["tag-a", "tag-b"])
    refute DurableOrchestrationDecision.scalar_search_attribute_value?(%{json: "forbidden"})
  end

  test "Oban scope is explicit and preserves only local bounded roles" do
    retained = DurableOrchestrationDecision.retained_oban_roles()
    classifications = DurableOrchestrationDecision.oban_scope()

    assert Enum.any?(
             retained,
             &(&1.role == :workflow_start_outbox and
                 &1.outcome_persistence == Mezzanine.WorkflowRuntime.OutboxPersistence and
                 &1.classification == :valid_outbox)
           )

    assert Enum.any?(
             retained,
             &(&1.role == :workflow_signal_outbox and
                 &1.worker == Mezzanine.WorkflowRuntime.WorkflowSignalOutboxWorker and
                 &1.outcome_persistence == Mezzanine.WorkflowRuntime.OutboxPersistence and
                 &1.classification == :valid_outbox)
           )

    assert Enum.any?(
             retained,
             &(&1.role == :claim_check_gc and
                 &1.worker == Mezzanine.WorkflowRuntime.ClaimCheckGcWorker and
                 &1.classification == :valid_claim_check_gc)
           )

    refute Enum.any?(retained, &(&1.queue == :decision_expiry))
    refute Enum.any?(classifications, &(to_string(&1[:worker]) =~ "DecisionExpiryWorker"))

    refute Enum.any?(classifications, &(&1.classification == :invalid_saga_orchestration))

    retired = DurableOrchestrationDecision.retired_oban_saga_workers()

    assert Enum.map(retired, & &1.worker) == [
             "Mezzanine.ExecutionDispatchWorker",
             "Mezzanine.ExecutionReceiptWorker",
             "Mezzanine.ExecutionReconcileWorker",
             "Mezzanine.JoinAdvanceWorker",
             "Mezzanine.LifecycleContinuationWorker",
             "Mezzanine.ExecutionCancelWorker"
           ]

    assert Enum.all?(retired, &(&1.classification == :retired_temporal_saga))
    assert Enum.all?(retired, &(&1.replacement_milestone == 31))

    valid = DurableOrchestrationDecision.valid_oban_classifications()
    assert Enum.all?(retained, &(&1.classification in valid))
  end

  test "operator signals and decision timers are registered as workflow-owned controls" do
    registry = DurableOrchestrationDecision.operator_signal_registry()

    assert Enum.any?(
             registry,
             &match?(%{signal_name: "operator.cancel", signal_version: "operator-cancel.v1"}, &1)
           )

    assert Enum.any?(
             registry,
             &match?(%{signal_name: "operator.pause", signal_version: "operator-pause.v1"}, &1)
           )

    assert Enum.any?(
             registry,
             &match?(%{signal_name: "operator.resume", signal_version: "operator-resume.v1"}, &1)
           )

    assert Enum.any?(
             registry,
             &match?(%{signal_name: "operator.retry", signal_version: "operator-retry.v1"}, &1)
           )

    assert Enum.any?(
             registry,
             &match?(%{signal_name: "operator.replan", signal_version: "operator-replan.v1"}, &1)
           )

    assert Enum.any?(
             registry,
             &match?(%{signal_name: "operator.rework", signal_version: "operator-rework.v1"}, &1)
           )

    assert %{
             contract: "Mezzanine.WorkflowDecisionTimer.v1",
             timer_semantics: :temporal_workflow_timer,
             forbidden_queue: :decision_expiry
           } = DurableOrchestrationDecision.decision_timer_policy()
  end

  test "workflow history policy keeps claim-check bodies and raw SDK objects out of history" do
    policy = DurableOrchestrationDecision.workflow_history_policy()

    assert :routing_facts in policy.allowed
    assert :claim_check_body in policy.forbidden
    assert :temporalex_struct in policy.forbidden
    assert :raw_history_event in policy.forbidden
    assert :review_required in policy.routing_fact_fields
    assert :next_step in policy.routing_fact_fields

    assert DurableOrchestrationDecision.complete?()
  end
end
