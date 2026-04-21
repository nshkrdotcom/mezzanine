defmodule Mezzanine.WorkflowRuntime.TemporalexAdapterTest do
  use ExUnit.Case, async: false

  alias Mezzanine.WorkflowRuntime.TemporalexAdapter

  defmodule FakeBoundary do
    def start_workflow(conn, workflow_module, args, opts) do
      send(self(), {:start_workflow, conn, workflow_module, args, opts})

      {:ok,
       %Temporalex.WorkflowHandle{
         workflow_id: Keyword.fetch!(opts, :id),
         run_id: "run-temporal-001",
         conn: conn
       }}
    end

    def signal_workflow(conn, workflow_id, signal_name, args, opts) do
      send(self(), {:signal_workflow, conn, workflow_id, signal_name, args, opts})
      :ok
    end

    def query_workflow(conn, workflow_id, query_name, args, opts) do
      send(self(), {:query_workflow, conn, workflow_id, query_name, args, opts})
      {:ok, %{status: "running", state_ref: "temporal-query://wf-001/operator_state.v1"}}
    end

    def cancel_workflow(conn, workflow_id, opts) do
      send(self(), {:cancel_workflow, conn, workflow_id, opts})
      :ok
    end

    def describe_workflow(conn, workflow_id, opts) do
      send(self(), {:describe_workflow, conn, workflow_id, opts})

      {:ok,
       %{
         "run_id" => "run-temporal-001",
         "status" => "Running",
         "search_attributes" => %{"phase4.workflow_type" => "agent_run"}
       }}
    end
  end

  defmodule ErrorBoundary do
    def start_workflow(_conn, _workflow_module, _args, _opts), do: {:error, :timeout}
  end

  setup do
    previous_boundary =
      Application.get_env(:mezzanine_workflow_runtime, :temporalex_boundary)

    Application.put_env(:mezzanine_workflow_runtime, :temporalex_boundary, FakeBoundary)

    on_exit(fn ->
      if previous_boundary do
        Application.put_env(:mezzanine_workflow_runtime, :temporalex_boundary, previous_boundary)
      else
        Application.delete_env(:mezzanine_workflow_runtime, :temporalex_boundary)
      end
    end)
  end

  test "starts workflows through TemporalexBoundary and returns a Mezzanine DTO" do
    assert {:ok, receipt} = TemporalexAdapter.start_workflow(start_request())

    assert receipt.workflow_id == "wf-001"
    assert receipt.workflow_run_id == "run-temporal-001"
    assert receipt.workflow_ref == "temporal-workflow://wf-001/run-temporal-001"
    assert receipt.start_state == "started"
    assert receipt.failure_class == "none"
    refute Map.has_key?(Map.from_struct(receipt), :raw_temporalex_result)
    refute Map.has_key?(Map.from_struct(receipt), :temporalex_struct)

    assert_received {:start_workflow, Mezzanine.WorkflowRuntime.TestTemporal.Connection,
                     Mezzanine.Workflows.AgentRun, args, opts}

    assert args.tenant_ref == "tenant-acme"
    assert Keyword.fetch!(opts, :id) == "wf-001"
    assert Keyword.fetch!(opts, :task_queue) == "mezzanine.agentic"
    assert Keyword.fetch!(opts, :timeout) == 10_000
    assert Keyword.fetch!(opts, :search_attributes)["phase4.tenant_ref"] == "tenant-acme"
  end

  test "signals, queries, cancels, describes, and history refs stay public-safe" do
    assert {:ok, signal} = TemporalexAdapter.signal_workflow(signal_request())
    assert signal.status == "delivered_to_temporal"
    assert signal.signal_ref == "temporal-signal://wf-001/sig-001"

    assert_received {:signal_workflow, Mezzanine.WorkflowRuntime.TestTemporal.Connection,
                     "wf-001", "operator.cancel", signal_args, signal_opts}

    assert signal_args.signal_payload_ref == "claim://signal-payload"
    assert Keyword.fetch!(signal_opts, :run_id) == "run-temporal-001"

    assert {:ok, query} = TemporalexAdapter.query_workflow(query_request())
    assert query.query_name == "operator_state.v1"
    assert query.summary.status == "running"

    assert {:ok, cancel} = TemporalexAdapter.cancel_workflow(cancel_request())
    assert cancel.status == "cancel_requested"

    assert {:ok, description} = TemporalexAdapter.describe_workflow(describe_request())
    assert description.status == "Running"
    assert description.search_attributes["phase4.workflow_type"] == "agent_run"

    assert {:ok, history_ref} = TemporalexAdapter.fetch_workflow_history_ref(describe_request())
    assert history_ref.history_ref == "temporal-history://wf-001/run-temporal-001"
    assert String.starts_with?(history_ref.history_hash, "sha256:")
  end

  test "normalizes Temporalex errors before they cross the facade" do
    Application.put_env(:mezzanine_workflow_runtime, :temporalex_boundary, ErrorBoundary)

    assert {:error, {:temporal_unavailable, :timeout}} =
             TemporalexAdapter.start_workflow(start_request())
  end

  defp start_request do
    %{
      connection: Mezzanine.WorkflowRuntime.TestTemporal.Connection,
      workflow_id: "wf-001",
      workflow_type: "agent_run",
      workflow_version: "agent-run.v1",
      workflow_module: Mezzanine.Workflows.AgentRun,
      task_queue: "mezzanine.agentic",
      args: %{
        tenant_ref: "tenant-acme",
        resource_ref: "resource-001",
        command_id: "cmd-001",
        trace_id: "trace-001",
        correlation_id: "corr-001"
      },
      search_attributes: %{"phase4.tenant_ref" => "tenant-acme"},
      idempotency_key: "idem-001",
      trace_id: "trace-001",
      release_manifest_ref: "phase5-m2am"
    }
  end

  defp signal_request do
    %{
      connection: Mezzanine.WorkflowRuntime.TestTemporal.Connection,
      workflow_id: "wf-001",
      workflow_run_id: "run-temporal-001",
      signal_id: "sig-001",
      signal_name: "operator.cancel",
      signal_version: "operator-cancel.v1",
      signal_payload_ref: "claim://signal-payload",
      signal_payload_hash: "sha256:signal",
      idempotency_key: "idem-signal-001",
      trace_id: "trace-001"
    }
  end

  defp query_request do
    %{
      connection: Mezzanine.WorkflowRuntime.TestTemporal.Connection,
      workflow_id: "wf-001",
      workflow_run_id: "run-temporal-001",
      query_name: "operator_state.v1",
      trace_id: "trace-001"
    }
  end

  defp cancel_request do
    %{
      connection: Mezzanine.WorkflowRuntime.TestTemporal.Connection,
      workflow_id: "wf-001",
      workflow_run_id: "run-temporal-001",
      reason: "operator_cancel",
      trace_id: "trace-001"
    }
  end

  defp describe_request do
    %{
      connection: Mezzanine.WorkflowRuntime.TestTemporal.Connection,
      workflow_id: "wf-001",
      workflow_run_id: "run-temporal-001",
      trace_id: "trace-001"
    }
  end
end
