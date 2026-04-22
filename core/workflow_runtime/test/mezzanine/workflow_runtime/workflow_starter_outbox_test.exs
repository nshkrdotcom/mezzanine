defmodule Mezzanine.WorkflowRuntime.WorkflowStarterOutboxTest do
  use ExUnit.Case, async: false

  alias Mezzanine.Idempotency
  alias Mezzanine.WorkflowRuntime.WorkflowStarterOutbox
  alias Mezzanine.WorkflowRuntime.WorkflowStarterOutboxWorker

  defmodule SuccessfulRuntime do
    @behaviour Mezzanine.WorkflowRuntime

    @impl true
    def start_workflow(request) do
      Mezzanine.WorkflowStartReceipt.new(%{
        workflow_ref: "workflow-ref://#{request.workflow_id}",
        workflow_id: request.workflow_id,
        workflow_run_id: "run-001",
        workflow_type: request.workflow_type,
        workflow_version: request.workflow_version,
        tenant_ref: request.args.tenant_ref,
        resource_ref: request.args.resource_ref,
        command_id: request.args.command_id,
        idempotency_key: request.idempotency_key,
        trace_id: request.trace_id,
        correlation_id: request.args.correlation_id,
        release_manifest_ref: request.release_manifest_ref,
        start_state: "started",
        duplicate?: false,
        retry_class: "none",
        failure_class: "none"
      })
    end

    @impl true
    def signal_workflow(_request), do: {:error, :not_used}

    @impl true
    def query_workflow(_request), do: {:error, :not_used}

    @impl true
    def cancel_workflow(_request), do: {:error, :not_used}

    @impl true
    def describe_workflow(_request), do: {:error, :not_used}

    @impl true
    def fetch_workflow_history_ref(_request), do: {:error, :not_used}
  end

  defmodule RecordingOutboxStore do
    @behaviour Mezzanine.WorkflowRuntime.OutboxPersistence

    @impl true
    def record_start_outcome(original_row, outcome_row) do
      send(self(), {:record_start_outcome, original_row, outcome_row})
      :ok
    end

    @impl true
    def record_signal_outcome(_original_row, _outcome_row), do: {:error, :not_used}
  end

  defmodule FailingOutboxStore do
    @behaviour Mezzanine.WorkflowRuntime.OutboxPersistence

    @impl true
    def record_start_outcome(_original_row, _outcome_row), do: {:error, :store_down}

    @impl true
    def record_signal_outcome(_original_row, _outcome_row), do: {:error, :not_used}
  end

  setup do
    previous = Application.get_env(:mezzanine_core, :workflow_runtime_impl)
    previous_outbox = Application.get_env(:mezzanine_workflow_runtime, :outbox_persistence)

    Application.put_env(:mezzanine_core, :workflow_runtime_impl, SuccessfulRuntime)

    Application.put_env(:mezzanine_workflow_runtime, :outbox_persistence,
      store: RecordingOutboxStore
    )

    on_exit(fn ->
      if previous do
        Application.put_env(:mezzanine_core, :workflow_runtime_impl, previous)
      else
        Application.delete_env(:mezzanine_core, :workflow_runtime_impl)
      end

      if previous_outbox do
        Application.put_env(:mezzanine_workflow_runtime, :outbox_persistence, previous_outbox)
      else
        Application.delete_env(:mezzanine_workflow_runtime, :outbox_persistence)
      end
    end)
  end

  test "defines the starter outbox table/resource shape and retained Oban queue" do
    contract = WorkflowStarterOutbox.schema_contract()

    assert contract.contract_name == "Mezzanine.WorkflowStarterOutbox.v1"
    assert contract.table_name == "workflow_start_outbox"
    assert contract.oban_queue == :workflow_start_outbox
    assert contract.worker_module == WorkflowStarterOutboxWorker
    assert contract.runtime_boundary == Mezzanine.WorkflowRuntime

    for field <- [
          :tenant_ref,
          :installation_ref,
          :principal_ref,
          :resource_ref,
          :authority_packet_ref,
          :permission_decision_ref,
          :idempotency_key,
          :trace_id,
          :correlation_id,
          :workflow_type,
          :workflow_id,
          :release_manifest_ref
        ] do
      assert field in contract.required_fields
    end
  end

  test "requires enterprise scope and records the same local transaction plan" do
    assert {:ok, row} = WorkflowStarterOutbox.new_row(row_attrs())
    assert row.dispatch_state == "queued"
    assert row.retry_count == 0

    assert {:ok, plan} = WorkflowStarterOutbox.same_transaction_plan(row)

    assert Enum.map(plan.operations, & &1.op) == [
             :persist_accepted_command_receipt,
             :insert_workflow_start_outbox_row,
             :insert_oban_dispatch_job
           ]

    assert :temporalex_client_call in plan.forbidden_inside_transaction

    dispatch_job = Enum.find(plan.operations, &(&1.op == :insert_oban_dispatch_job))
    assert dispatch_job.queue == :workflow_start_outbox
    assert dispatch_job.worker == WorkflowStarterOutboxWorker
    assert dispatch_job.args["workflow_id"] == row.workflow_id
    assert dispatch_job.unique == WorkflowStarterOutbox.unique_declaration()

    assert {:error, {:missing_required_fields, missing}} =
             WorkflowStarterOutbox.new_row(Map.delete(row_attrs(), :permission_decision_ref))

    assert :permission_decision_ref in missing
  end

  test "builds deterministic workflow id and duplicate-safe start scope" do
    attrs = row_attrs()
    deterministic = WorkflowStarterOutbox.deterministic_workflow_id(attrs)

    assert deterministic ==
             "tenant:tenant-acme:resource:resource-work-1:workflow:agent_run:command:cmd-091:release:phase4-v6-milestone26-durable-workflow-starter-outbox"

    assert {:ok, left} =
             WorkflowStarterOutbox.new_row(Map.put(attrs, :workflow_id, deterministic))

    assert {:ok, right} =
             WorkflowStarterOutbox.new_row(Map.put(attrs, :workflow_id, deterministic))

    assert WorkflowStarterOutbox.duplicate_start_safe?(left, right)

    refute WorkflowStarterOutbox.duplicate_start_safe?(
             left,
             Map.put(right, :idempotency_key, "conflicting-idempotency-key")
           )
  end

  test "dispatch request calls Mezzanine.WorkflowRuntime without raw payloads or SDK structs" do
    assert {:ok, row} = WorkflowStarterOutbox.new_row(row_attrs())
    assert {:ok, request} = WorkflowStarterOutbox.start_request(row)

    assert request.workflow_id == row.workflow_id
    assert request.workflow_module == Mezzanine.Workflows.AgentRun
    assert request.task_queue == "mezzanine.agentic"
    assert request.workflow_input_ref == row.workflow_input_ref
    assert request.search_attributes["phase4.tenant_ref"] == row.tenant_ref
    refute Map.has_key?(request, :raw_payload)
    refute Map.has_key?(request, :temporalex_struct)

    assert :ok =
             WorkflowStarterOutboxWorker.perform(%Oban.Job{
               args: WorkflowStarterOutbox.dispatch_job_args(row)
             })

    assert_received {:record_start_outcome, original_row, outcome_row}
    assert original_row["outbox_id"] == row.outbox_id
    assert outcome_row.dispatch_state == "started"
    assert outcome_row.workflow_run_id == "run-001"
  end

  test "dispatch request carries idempotency correlation evidence for canonical starts" do
    canonical_key = canonical_root_key()

    assert {:ok, row} =
             row_attrs()
             |> Map.merge(%{
               canonical_idempotency_key: canonical_key,
               idempotency_key: canonical_key,
               causation_id: "cause-091",
               client_retry_key: "client-retry-091",
               platform_envelope_idempotency_key: canonical_key
             })
             |> WorkflowStarterOutbox.new_row()

    assert {:ok, request} = WorkflowStarterOutbox.start_request(row)

    assert request.idempotency_correlation["contract_name"] ==
             "Mezzanine.IdempotencyCorrelationEvidence.v1"

    assert request.idempotency_correlation["canonical_idempotency_key"] == canonical_key
    assert request.idempotency_correlation["client_retry_key"] == "client-retry-091"
    assert request.idempotency_correlation["platform_envelope_idempotency_key"] == canonical_key
    assert request.idempotency_correlation["temporal_workflow_id"] == row.workflow_id
    assert request.idempotency_correlation["temporal_start_idempotency_key"] == canonical_key
    assert request.idempotency_correlation["trace_id"] == row.trace_id
    assert request.idempotency_correlation["causation_id"] == "cause-091"
    assert request.idempotency_correlation["tenant_id"] == row.tenant_ref
    assert request.idempotency_correlation["release_manifest_ref"] == row.release_manifest_ref

    dispatch_job_args = WorkflowStarterOutbox.dispatch_job_args(row)
    assert dispatch_job_args["canonical_idempotency_key"] == canonical_key
    assert dispatch_job_args["client_retry_key"] == "client-retry-091"

    assert {:ok, receipt} =
             Mezzanine.WorkflowStartReceipt.new(%{
               workflow_ref: "workflow-ref://#{row.workflow_id}",
               workflow_id: row.workflow_id,
               workflow_run_id: "run-091",
               workflow_type: row.workflow_type,
               workflow_version: row.workflow_version,
               tenant_ref: row.tenant_ref,
               resource_ref: row.resource_ref,
               command_id: row.command_id,
               idempotency_key: row.idempotency_key,
               trace_id: row.trace_id,
               start_state: "started"
             })

    assert {:ok, started} = WorkflowStarterOutbox.classify_start_result(row, {:ok, receipt})
    assert started.idempotency_correlation["temporal_workflow_run_id"] == "run-091"
  end

  test "retained starter worker does not ack when outcome persistence fails" do
    Application.put_env(:mezzanine_workflow_runtime, :outbox_persistence,
      store: FailingOutboxStore
    )

    assert {:ok, row} = WorkflowStarterOutbox.new_row(row_attrs())

    assert {:error, {:outbox_outcome_not_persisted, :store_down}} =
             WorkflowStarterOutboxWorker.perform(%Oban.Job{
               args: WorkflowStarterOutbox.dispatch_job_args(row)
             })
  end

  test "classifies success, duplicate start, retryable failure, and terminal failure" do
    assert {:ok, row} = WorkflowStarterOutbox.new_row(row_attrs())

    assert {:ok, receipt} =
             Mezzanine.WorkflowStartReceipt.new(%{
               workflow_ref: "workflow-ref://wf-091",
               workflow_id: row.workflow_id,
               workflow_run_id: "run-091",
               workflow_type: row.workflow_type,
               workflow_version: row.workflow_version,
               tenant_ref: row.tenant_ref,
               resource_ref: row.resource_ref,
               command_id: row.command_id,
               idempotency_key: row.idempotency_key,
               trace_id: row.trace_id,
               start_state: "started"
             })

    assert {:ok, started} = WorkflowStarterOutbox.classify_start_result(row, {:ok, receipt})
    assert started.dispatch_state == "started"
    assert started.workflow_run_id == "run-091"

    assert {:ok, duplicate} =
             WorkflowStarterOutbox.classify_start_result(
               row,
               {:error, {:already_started, "run-091"}}
             )

    assert duplicate.dispatch_state == "duplicate_started"
    assert duplicate.last_error_class == "duplicate_start_existing_workflow"

    assert {:retry, retryable} =
             WorkflowStarterOutbox.classify_start_result(
               row,
               {:error, {:temporalex, :unavailable}}
             )

    assert retryable.dispatch_state == "retryable_failure"
    assert retryable.retry_count == 1

    assert {:error, terminal} =
             WorkflowStarterOutbox.classify_start_result(
               row,
               {:error, {:conflict, :payload_hash_mismatch}}
             )

    assert terminal.dispatch_state == "terminal_failure"

    assert terminal.last_error_class ==
             {:terminal_conflicting_duplicate_start, :payload_hash_mismatch}
  end

  test "exposes stuck rows to operator projection and incident bundle fields" do
    assert {:ok, row} = WorkflowStarterOutbox.new_row(row_attrs())

    projection = WorkflowStarterOutbox.operator_projection(row)
    assert projection.outbox_id == row.outbox_id
    assert projection.dispatch_state == "queued"
    assert projection.trace_id == row.trace_id

    incident = WorkflowStarterOutbox.incident_bundle_fields(row)
    assert incident.command_receipt_ref == row.command_receipt_ref
    assert incident.workflow_id == row.workflow_id
    assert incident.release_manifest_ref == row.release_manifest_ref
  end

  defp row_attrs do
    %{
      outbox_id: "wso-091",
      tenant_ref: "tenant-acme",
      installation_ref: "installation-main",
      workspace_ref: "workspace-main",
      project_ref: "project-main",
      environment_ref: "env-prod",
      principal_ref: "principal-operator",
      resource_ref: "resource-work-1",
      command_envelope_ref: "command-envelope-091",
      command_receipt_ref: "command-receipt-091",
      command_id: "cmd-091",
      workflow_type: "agent_run",
      workflow_id:
        "tenant:tenant-acme:resource:resource-work-1:workflow:agent_run:command:cmd-091:release:phase4-v6-milestone26-durable-workflow-starter-outbox",
      workflow_version: "agent-run.v1",
      workflow_input_version: "workflow-input.v1",
      workflow_input_ref: "claim://workflow-input/091",
      authority_packet_ref: "authpkt-091",
      permission_decision_ref: "decision-091",
      idempotency_key: "idem-091",
      dedupe_scope: "tenant-acme:resource-work-1:agent_run:cmd-091",
      trace_id: "trace-091",
      correlation_id: "corr-091",
      release_manifest_ref: "phase4-v6-milestone26-durable-workflow-starter-outbox",
      payload_hash: String.duplicate("d", 64),
      payload_ref: "claim://workflow-payload/091"
    }
  end

  defp canonical_root_key do
    Idempotency.canonical_key!(%{
      tenant_id: "tenant-acme",
      installation_id: "installation-main",
      operation_family: "workflow.start",
      operation_ref: "cmd-091",
      causation_id: "cause-091",
      authority_decision_ref: "decision-091",
      subject_ref: "resource-work-1",
      payload_hash: String.duplicate("d", 64),
      source_event_position: "command-envelope-091"
    })
  end
end
