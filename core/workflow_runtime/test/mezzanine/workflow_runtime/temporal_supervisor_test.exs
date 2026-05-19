defmodule Mezzanine.WorkflowRuntime.TemporalSupervisorTest do
  use ExUnit.Case, async: false

  alias Mezzanine.RuntimeProfile
  alias Mezzanine.RuntimeProfileStore
  alias Mezzanine.WorkflowRuntime.TemporalSupervisor

  test "stays inert unless Temporal workers are explicitly enabled" do
    assert TemporalSupervisor.child_specs(enabled?: false) == []
    assert TemporalSupervisor.preflight(enabled?: false) == :ok
  end

  test "fails early when live Temporal workers are enabled without substrate proof" do
    assert {:error,
            {:temporal_substrate_unavailable, %{address: "127.0.0.1:7233", namespace: "default"}}} =
             TemporalSupervisor.preflight(enabled?: true)

    assert :ok =
             TemporalSupervisor.preflight(
               enabled?: true,
               substrate_available?: true,
               address: "127.0.0.1:7233",
               namespace: "default"
             )
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

    assert hazmat.connection ==
             Temporalex.connection_name(Mezzanine.WorkflowRuntime.TestTemporal.MezzanineHazmat)

    assert Mezzanine.Workflows.ExecutionAttempt in hazmat.workflows

    assert Mezzanine.Workflows.AgentLoop in Enum.find(
             specs,
             &(&1.task_queue == "mezzanine.agentic")
           ).workflows

    assert Mezzanine.Workflows.OperationGraphRun in Enum.find(
             specs,
             &(&1.task_queue == "mezzanine.agentic")
           ).workflows

    assert Mezzanine.Activities.StartLowerExecution in hazmat.activities
    assert Mezzanine.Activities.AgentLoopSubmitLowerRun in hazmat.activities
    assert Mezzanine.Activities.SubmitJidoLowerActivity in hazmat.activities
    assert Mezzanine.Activities.ExecuteOperationGraphNode in hazmat.activities

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

  test "rejects unknown Temporal instance bases before module construction" do
    assert_raise ArgumentError,
                 "unknown Temporal instance base: Mezzanine.WorkflowRuntime.RuntimeConfiguredBase",
                 fn ->
                   TemporalSupervisor.instance_name("mezzanine.hazmat",
                     enabled?: true,
                     instance_base: Mezzanine.WorkflowRuntime.RuntimeConfiguredBase
                   )
                 end
  end

  test "rejects unknown Temporal task queues before module construction" do
    assert_raise ArgumentError, "unknown Temporal task queue: \"provider.selected.queue\"", fn ->
      TemporalSupervisor.instance_name("provider.selected.queue",
        enabled?: true,
        instance_base: Mezzanine.WorkflowRuntime.TestTemporal
      )
    end
  end

  test "uses source-owned Temporal instance registry without runtime module concatenation" do
    source =
      Path.expand("../../../lib/mezzanine/workflow_runtime/temporal_supervisor.ex", __DIR__)
      |> File.read!()

    refute String.contains?(source, "Module" <> ".concat")

    assert TemporalSupervisor.instance_name("mezzanine.semantic",
             enabled?: true,
             instance_base: Mezzanine.WorkflowRuntime.Phase6Temporal
           ) == Mezzanine.WorkflowRuntime.Phase6Temporal.MezzanineSemantic
  end

  test "resolves StackLab source-owned Temporal bases through the explicit registry" do
    assert TemporalSupervisor.instance_name("mezzanine.hazmat",
             enabled?: true,
             instance_base: Mezzanine.WorkflowRuntime.PrelimTemporal
           ) == Mezzanine.WorkflowRuntime.PrelimTemporal.MezzanineHazmat

    assert TemporalSupervisor.instance_name("mezzanine.hazmat",
             enabled?: true,
             instance_base: Mezzanine.WorkflowRuntime.StackLabPhase6Temporal
           ) == Mezzanine.WorkflowRuntime.StackLabPhase6Temporal.MezzanineHazmat
  end

  test "non-governed runtime config uses the supervised boot profile" do
    profile =
      RuntimeProfile.empty()
      |> RuntimeProfile.put(:mezzanine_workflow_runtime, :temporal,
        enabled?: true,
        address: "temporal.example.internal:7233",
        namespace: "profile-selected",
        instance_base: Mezzanine.WorkflowRuntime.TestTemporal
      )

    assert {:ok, previous_profile} = RuntimeProfileStore.replace_profile(profile)
    on_exit(fn -> RuntimeProfileStore.replace_profile(previous_profile) end)

    config = TemporalSupervisor.runtime_config()

    assert Keyword.fetch!(config, :enabled?)
    assert Keyword.fetch!(config, :address) == "temporal.example.internal:7233"
    assert Keyword.fetch!(config, :namespace) == "profile-selected"
    assert Keyword.fetch!(config, :instance_base) == Mezzanine.WorkflowRuntime.TestTemporal
  end

  test "governed runtime config ignores boot-profile Temporal credentials" do
    profile =
      RuntimeProfile.empty()
      |> RuntimeProfile.put(:mezzanine_workflow_runtime, :temporal,
        enabled?: true,
        address: "temporal.example.internal:7233",
        namespace: "profile-selected",
        api_key: "profile-token"
      )

    assert {:ok, previous_profile} = RuntimeProfileStore.replace_profile(profile)
    on_exit(fn -> RuntimeProfileStore.replace_profile(previous_profile) end)

    config = TemporalSupervisor.runtime_config(governed?: true, namespace: "explicit")

    refute Keyword.has_key?(config, :api_key)
    assert Keyword.fetch!(config, :enabled?) == false
    assert Keyword.fetch!(config, :namespace) == "explicit"
    assert Keyword.fetch!(config, :address) == "127.0.0.1:7233"
  end
end
