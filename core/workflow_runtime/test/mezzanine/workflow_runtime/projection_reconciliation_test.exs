defmodule Mezzanine.WorkflowRuntime.ProjectionReconciliationTest do
  use ExUnit.Case, async: true

  alias Mezzanine.WorkflowRuntime.ProjectionReconciliation

  defmodule CompactRuntime do
    @behaviour Mezzanine.WorkflowRuntime

    @impl true
    def start_workflow(_request), do: {:error, :not_used}

    @impl true
    def signal_workflow(_request), do: {:error, :not_used}

    @impl true
    def query_workflow(request) do
      send(self(), {:query_workflow, request})

      {:ok,
       %Mezzanine.WorkflowQueryResult{
         workflow_ref: "workflow://#{request.workflow_id}",
         query_name: request.query_name,
         state_ref: "workflow-state://#{request.workflow_id}",
         summary: %{
           workflow_state: "accepted_active",
           workflow_version: "execution-attempt.v1",
           last_observed_workflow_event_ref: "event-42"
         },
         trace_id: "trace-201"
       }}
    end

    @impl true
    def cancel_workflow(_request), do: {:error, :not_used}

    @impl true
    def describe_workflow(request) do
      send(self(), {:describe_workflow, request})

      {:ok,
       %Mezzanine.WorkflowDescription{
         workflow_ref: "workflow://#{request.workflow_id}",
         status: "running",
         search_attributes: %{
           "phase4.workflow_version" => "execution-attempt.v1",
           "phase4.release_manifest_ref" => request.release_manifest_ref
         },
         trace_id: "trace-201"
       }}
    end

    @impl true
    def fetch_workflow_history_ref(_request), do: {:error, :raw_history_not_used}
  end

  test "declares the Temporal/Postgres reconciliation profile and row classes" do
    profile = ProjectionReconciliation.profile()

    assert profile.contract_name == "Mezzanine.WorkflowProjectionReconciliation.v1"
    assert profile.workflow_master == :temporal
    refute profile.temporal_lookup.raw_history_allowed?

    for field <- [
          :workflow_id,
          :workflow_type,
          :workflow_version,
          :workflow_run_id,
          :postgres_projection_owner,
          :projection_row_ids,
          :last_observed_workflow_event_ref,
          :reconciliation_status,
          :drift_class,
          :safe_operator_action,
          :release_manifest_ref
        ] do
      assert field in profile.fields
    end

    assert %{row_class: :local_outbox_delivery_evidence, workflow_master?: false} =
             Enum.find(profile.row_classifications, &(&1.table == "workflow_start_outbox"))

    assert %{row_class: :fact_and_operator_projection, workflow_master?: false} =
             Enum.find(profile.row_classifications, &(&1.table == "execution_records"))
  end

  test "candidate query joins execution projections to workflow-start outbox evidence" do
    query = ProjectionReconciliation.candidate_query()

    assert query =~ "FROM execution_records er"
    assert query =~ "FROM workflow_start_outbox wso"
    assert query =~ "wso.workflow_type = 'execution_attempt'"
    assert query =~ "COALESCE(wo.workflow_id, ep.expected_workflow_id) AS workflow_id"

    for state <- [
          "queued",
          "in_flight",
          "accepted_active",
          "pending_dispatch",
          "dispatching_retry",
          "awaiting_receipt"
        ] do
      assert query =~ "'#{state}'"
    end
  end

  test "Temporal lookups use compact describe and query requests, not history export" do
    candidate = %{
      workflow_id: "tenant:tenant-1:execution:exec-1:attempt:1",
      workflow_run_id: "run-201"
    }

    assert [
             %{operation: :describe_workflow, workflow_id: workflow_id, run_id: "run-201"},
             %{
               operation: :query_workflow,
               run_id: "run-201",
               query_name: "execution.lifecycle_state"
             }
           ] = ProjectionReconciliation.temporal_lookup_requests(candidate)

    assert workflow_id == "tenant:tenant-1:execution:exec-1:attempt:1"

    assert {:ok, compact} =
             ProjectionReconciliation.lookup_temporal_state(candidate, CompactRuntime)

    assert compact.workflow_id == workflow_id
    assert compact.workflow_run_id == "run-201"
    assert compact.query_name == "execution.lifecycle_state"
    refute compact.raw_history?
    assert compact.description.status == "running"
    assert compact.query.summary.workflow_state == "accepted_active"

    assert_received {:describe_workflow,
                     %{
                       workflow_id: ^workflow_id,
                       run_id: "run-201",
                       release_manifest_ref:
                         "phase5-v7-milestone2-temporal-postgres-reconciliation"
                     }}

    assert_received {:query_workflow,
                     %{
                       workflow_id: ^workflow_id,
                       run_id: "run-201",
                       query_name: "execution.lifecycle_state"
                     }}
  end

  test "drift actions cover safe automatic and operator repair for all classes" do
    actions = ProjectionReconciliation.drift_actions()

    assert Enum.map(actions, & &1.drift_class) == [
             :projection_lag,
             :orphan_projection,
             :orphan_workflow,
             :conflicting_terminal,
             :version_skew
           ]

    assert Enum.all?(actions, &Map.has_key?(&1, :automatic_repair))
    assert Enum.all?(actions, &Map.has_key?(&1, :operator_repair))
    assert Enum.all?(actions, &Map.has_key?(&1, :safe_operator_action))
  end

  test "outbox drain and retirement gates deny legacy workflow-start ownership" do
    assert %{
             retryable_states: ["queued", "retryable_failure", "dispatching"],
             evidence_only_states: ["started", "duplicate_started"],
             start_authority: :mezzanine_workflow_runtime_idempotency,
             forbidden_worker_authority: :workflow_lifecycle_decision
           } = ProjectionReconciliation.outbox_drain_plan()

    retirement_gate = ProjectionReconciliation.workflow_starter_retirement_gate()

    assert :no_non_workflow_runtime_enqueue_writers_by_source_scan in retirement_gate
  end

  test "dispatch-state reduction profile preserves old values only as read aliases" do
    reduction = ProjectionReconciliation.dispatch_state_reduction_profile()

    assert reduction.active_targets == [:queued, :in_flight, :accepted_active]
    refute reduction.new_legacy_writes_allowed?
    assert reduction.legacy_aliases.pending_dispatch == :queued
    assert reduction.legacy_aliases.dispatching_retry == :in_flight
    assert reduction.legacy_aliases.awaiting_receipt == :accepted_active
    assert :last_dispatch_error_kind in reduction.evidence_fields
    assert :lower_receipt in reduction.evidence_fields
  end

  test "source contains only the WorkflowRuntime-owned starter outbox insert plan" do
    source_root = Path.expand("../../..", __DIR__)

    insert_refs =
      source_root
      |> Path.join("lib/**/*.ex")
      |> Path.wildcard()
      |> Enum.flat_map(fn path ->
        text = File.read!(path)

        if String.contains?(text, ":insert_workflow_start_outbox_row") do
          [Path.relative_to(path, source_root)]
        else
          []
        end
      end)

    assert insert_refs == ["lib/mezzanine/workflow_runtime/workflow_starter_outbox.ex"]
  end
end
