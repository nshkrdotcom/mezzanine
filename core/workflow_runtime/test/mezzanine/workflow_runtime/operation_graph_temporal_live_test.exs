defmodule Mezzanine.WorkflowRuntime.OperationGraphTemporalLiveTest do
  use ExUnit.Case, async: false

  @moduletag :live_temporal

  alias Mezzanine.WorkflowRuntime.TemporalSupervisor
  alias Mezzanine.WorkflowRuntime.TemporalexAdapter

  test "OperationGraphRun drains through supervised live Temporal workers" do
    config = live_temporal_config()

    {:ok, supervisor} =
      Supervisor.start_link(TemporalSupervisor.child_specs(config), strategy: :one_for_one)

    on_exit(fn ->
      stop_supervisor(supervisor)
    end)

    connection = TemporalSupervisor.connection_name("mezzanine.agentic", config)
    workflow_id = "operation-graph-live-proof-#{System.unique_integer([:positive])}"

    assert {:ok, handle} =
             Temporalex.Client.start_workflow(
               connection,
               Mezzanine.Workflows.OperationGraphRun,
               workflow_input(),
               id: workflow_id,
               task_queue: "mezzanine.agentic",
               timeout: 30_000
             )

    assert {:ok, result} = Temporalex.Client.get_result(handle, timeout: 60_000)
    assert result.workflow_state == "completed"
    assert result.recorded_node_refs == ["node://source", "node://review", "node://publication"]
    assert length(result.terminal_event_refs) == 3

    assert {:ok, description} =
             TemporalexAdapter.describe_workflow(%{
               connection: connection,
               workflow_id: workflow_id,
               workflow_run_id: handle.run_id,
               timeout_ms: 10_000
             })

    assert description.status != "unknown"

    assert {:ok, history_ref} =
             TemporalexAdapter.fetch_workflow_history_ref(%{
               connection: connection,
               workflow_id: workflow_id,
               workflow_run_id: handle.run_id,
               timeout_ms: 10_000
             })

    assert history_ref.workflow_ref == "temporal-workflow://#{workflow_id}/#{handle.run_id}"
    assert history_ref.history_ref == "temporal-history://#{workflow_id}/#{handle.run_id}"
    assert String.starts_with?(history_ref.history_hash, "sha256:")
  end

  defp live_temporal_config do
    [
      enabled?: true,
      substrate_available?: true,
      address: "127.0.0.1:7233",
      namespace: "default",
      instance_base: Mezzanine.WorkflowRuntime.StackLabPhase6Temporal,
      max_concurrent_workflow_tasks: 3,
      max_concurrent_activity_tasks: 3
    ]
  end

  defp stop_supervisor(supervisor) do
    if Process.alive?(supervisor) do
      Supervisor.stop(supervisor, :normal, 5_000)
    end
  catch
    :exit, _reason -> :ok
  end

  defp workflow_input do
    %{
      operation_graph: %{
        graph_ref: "operation-graph://live-temporal/proof",
        nodes: [
          node!("node://source", "role://source", :source_read, 1),
          node!("node://review", "role://review", :runtime_operation, 2),
          node!("node://publication", "role://publication", :source_write, 3)
        ],
        dependencies: [
          dependency!("node://source", "node://review"),
          dependency!("node://review", "node://publication")
        ]
      },
      facts: %{},
      schedule_attrs: %{
        workflow_run_ref: "workflow-run://live-temporal/proof",
        operation_context_ref: "operation-context://live-temporal/proof",
        operation_plans_by_node_ref: %{
          "node://source" => "operation-plan://live/source",
          "node://review" => "operation-plan://live/review",
          "node://publication" => "operation-plan://live/publication"
        }
      },
      activity_result_attrs_by_node_ref: %{
        "node://source" => result_attrs!("source"),
        "node://review" => result_attrs!("review"),
        "node://publication" => result_attrs!("publication")
      }
    }
  end

  defp node!(node_ref, role_ref, operation_class, order_key) do
    %{
      node_ref: node_ref,
      operation_role_ref: role_ref,
      operation_class: operation_class,
      projection_order_key: order_key
    }
  end

  defp dependency!(from_node_ref, to_node_ref) do
    %{
      dependency_ref: "dependency://#{from_node_ref}/#{to_node_ref}",
      from_node_ref: from_node_ref,
      to_node_ref: to_node_ref,
      relation: :blocks_on_success,
      completion_policy: :required
    }
  end

  defp result_attrs!(node_name) do
    %{
      event_ref: "event://live/#{node_name}/succeeded",
      status: :succeeded,
      result_ref: "result://live/#{node_name}"
    }
  end
end
