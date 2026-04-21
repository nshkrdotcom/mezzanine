defmodule Mezzanine.WorkflowRuntime.TemporalSupervisorTest do
  use ExUnit.Case, async: true

  alias Mezzanine.WorkflowRuntime.TemporalSupervisor

  test "stays inert unless Temporal workers are explicitly enabled" do
    assert TemporalSupervisor.child_specs(enabled?: false) == []
  end

  test "builds Mezzanine-owned Temporalex child specs from the registry" do
    specs =
      TemporalSupervisor.task_queue_specs(
        enabled?: true,
        address: "127.0.0.1:7233",
        namespace: "default",
        instance_base: Mezzanine.WorkflowRuntime.TestTemporal,
        max_concurrent_workflow_tasks: 7,
        max_concurrent_activity_tasks: 11
      )

    assert Enum.map(specs, & &1.task_queue) == [
             "mezzanine.agentic",
             "mezzanine.hazmat",
             "mezzanine.review",
             "mezzanine.semantic"
           ]

    hazmat = Enum.find(specs, &(&1.task_queue == "mezzanine.hazmat"))

    assert hazmat.name == Mezzanine.WorkflowRuntime.TestTemporal.MezzanineHazmat
    assert hazmat.connection == Mezzanine.WorkflowRuntime.TestTemporal.MezzanineHazmat.Connection
    assert Mezzanine.Workflows.ExecutionAttempt in hazmat.workflows
    assert Mezzanine.Activities.StartLowerExecution in hazmat.activities
    assert Mezzanine.Activities.SubmitJidoLowerActivity in hazmat.activities

    assert %{
             id: Mezzanine.WorkflowRuntime.TestTemporal.MezzanineHazmat,
             start: {Temporalex, :start_link, [opts]},
             type: :supervisor
           } = hazmat.child_spec

    assert Keyword.fetch!(opts, :name) == Mezzanine.WorkflowRuntime.TestTemporal.MezzanineHazmat
    assert Keyword.fetch!(opts, :address) == "http://127.0.0.1:7233"
    assert Keyword.fetch!(opts, :namespace) == "default"
    assert Keyword.fetch!(opts, :task_queue) == "mezzanine.hazmat"
    assert Keyword.fetch!(opts, :max_concurrent_workflow_tasks) == 7
    assert Keyword.fetch!(opts, :max_concurrent_activity_tasks) == 11
  end

  test "child_specs returns unique supervisor children when enabled" do
    children =
      TemporalSupervisor.child_specs(
        enabled?: true,
        instance_base: Mezzanine.WorkflowRuntime.TestTemporal
      )

    assert Enum.map(children, & &1.id) == [
             Mezzanine.WorkflowRuntime.TestTemporal.MezzanineAgentic,
             Mezzanine.WorkflowRuntime.TestTemporal.MezzanineHazmat,
             Mezzanine.WorkflowRuntime.TestTemporal.MezzanineReview,
             Mezzanine.WorkflowRuntime.TestTemporal.MezzanineSemantic
           ]
  end
end
