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
             Mezzanine.Workflows.AgentRun,
             Mezzanine.Workflows.ExecutionAttempt,
             Mezzanine.Workflows.DecisionReview,
             Mezzanine.Workflows.JoinBarrier,
             Mezzanine.Workflows.IncidentReconstruction
           ]

    assert Mezzanine.Activities.StartLowerExecution in activity_modules
    assert Mezzanine.Activities.CallOuterBrain in activity_modules
    assert Mezzanine.Activities.CompensateCancelledRun in activity_modules

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
             &(&1.role == :workflow_start_outbox and &1.classification == :valid_outbox)
           )

    assert Enum.any?(
             retained,
             &(&1.role == :workflow_signal_outbox and &1.classification == :valid_outbox)
           )

    assert Enum.any?(
             retained,
             &(&1.role == :claim_check_gc and &1.classification == :valid_claim_check_gc)
           )

    assert Enum.any?(classifications, &(&1.worker == Mezzanine.ExecutionDispatchWorker))
    assert Enum.any?(classifications, &(&1.worker == Mezzanine.ExecutionCancelWorker))

    invalid = Enum.filter(classifications, &(&1.classification == :invalid_saga_orchestration))
    assert invalid != []
    assert Enum.all?(invalid, &(&1.replacement_milestone == 31))

    valid = DurableOrchestrationDecision.valid_oban_classifications()
    assert Enum.all?(retained, &(&1.classification in valid))
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
