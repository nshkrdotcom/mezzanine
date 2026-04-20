defmodule Mezzanine.WorkflowRuntime.WorkflowLifecycleCompensationTest do
  use ExUnit.Case, async: true

  alias Mezzanine.WorkflowRuntime.WorkflowLifecycleCompensation

  defmodule SignalRuntime do
    @behaviour Mezzanine.WorkflowRuntime

    @impl true
    def start_workflow(_request), do: {:error, :not_used}

    @impl true
    def signal_workflow(request) do
      send(self(), {:workflow_signal, request})

      {:ok,
       %{
         signal_ref: "temporal-signal://#{request.workflow_id}/#{request.signal_id}",
         status: "delivered_to_temporal",
         dispatch_state: "delivered_to_temporal",
         raw_temporalex_result: :forbidden,
         task_token: :forbidden
       }}
    end

    @impl true
    def query_workflow(_request), do: {:error, :not_used}

    @impl true
    def cancel_workflow(_request), do: {:error, :not_used}

    @impl true
    def describe_workflow(_request), do: {:error, :not_used}

    @impl true
    def fetch_workflow_history_ref(_request), do: {:error, :not_used}
  end

  test "profile declares workflow lifecycle ownership and excludes local mutation targets" do
    profile = WorkflowLifecycleCompensation.profile()

    assert profile.compensation_owner == :workflow_lifecycle
    assert profile.workflow_truth_owner == :temporal
    assert profile.signal_boundary == Mezzanine.WorkflowRuntime
    assert profile.route_targets == [:workflow_signal, :workflow_activity]
    assert profile.lifecycle_continuation_role == :retry_dead_letter_visibility_only
    assert "owner_command" in profile.forbidden_target_kinds

    assert profile.release_manifest_ref ==
             "phase5-v7-m02ac-workflow-lifecycle-compensation-routing"

    assert profile.signal_routes.retry.signal_name == "workflow.compensation.retry"
    assert profile.signal_routes.cancel.signal_name == "workflow.compensation.cancel"

    assert profile.activity_routes.cancel.activity_module ==
             Mezzanine.Activities.CompensateCancelledRun
  end

  test "builds workflow signal routes through WorkflowRuntime only" do
    assert {:ok, route} = WorkflowLifecycleCompensation.route(signal_compensation_attrs())

    assert route.route_kind == :workflow_signal
    assert route.compensation_owner == :workflow_lifecycle
    assert route.compensation_kind == :retry
    assert route.signal_boundary == Mezzanine.WorkflowRuntime
    assert route.lifecycle_continuation_role == :retry_dead_letter_visibility_only

    assert route.request.workflow_id == "workflow-201"
    assert route.request.signal_name == "workflow.compensation.retry"
    assert route.request.signal_version == "workflow-compensation-retry.v1"
    assert route.request.idempotency_key == "idem-compensation-201"
    assert route.request.compensation_ref == "compensation:workflow:201"
    assert route.request.release_manifest_ref == "phase5_hardening_metrics[30]"
    refute Map.has_key?(route.request, :raw_payload)
    refute Map.has_key?(route.request, :temporalex_struct)
    refute Map.has_key?(route.request, :task_token)
  end

  test "dispatches workflow signal compensation through the runtime boundary" do
    assert {:ok, delivered} =
             WorkflowLifecycleCompensation.dispatch_signal(
               signal_compensation_attrs(),
               SignalRuntime
             )

    assert delivered.runtime_receipt.dispatch_state == "delivered_to_temporal"
    refute Map.has_key?(delivered.runtime_receipt, :raw_temporalex_result)
    refute Map.has_key?(delivered.runtime_receipt, :task_token)

    assert_received {:workflow_signal,
                     %{
                       workflow_id: "workflow-201",
                       signal_name: "workflow.compensation.retry",
                       idempotency_key: "idem-compensation-201"
                     }}
  end

  test "routes workflow lifecycle activity compensation without local dispatch authority" do
    attrs =
      signal_compensation_attrs()
      |> Map.put(:compensation_kind, :cancel)
      |> Map.put(:owner_command_or_signal, %{
        kind: "workflow_activity",
        workflow_id: "workflow-201",
        activity: "compensate_cancelled_run",
        idempotency_key: "idem-compensation-cancel-201"
      })

    assert {:ok, route} = WorkflowLifecycleCompensation.route(attrs)

    assert route.route_kind == :workflow_activity
    assert route.activity_owner == :workflow_lifecycle
    assert route.activity_module == Mezzanine.Activities.CompensateCancelledRun
    assert route.input.idempotency_key == "idem-compensation-cancel-201"
    refute Map.has_key?(route.input, :raw_payload)

    assert {:error, :workflow_activity_must_run_inside_temporal_workflow} =
             WorkflowLifecycleCompensation.dispatch_signal(attrs, SignalRuntime)
  end

  test "rejects non-workflow owners, owner commands, unknown signals, and missing target fields" do
    assert {:error, {:invalid_compensation_owner, "execution_ledger"}} =
             signal_compensation_attrs()
             |> Map.put(:compensation_owner, "execution_ledger")
             |> WorkflowLifecycleCompensation.route()

    assert {:error, {:forbidden_target_kind, "owner_command"}} =
             signal_compensation_attrs()
             |> Map.put(:owner_command_or_signal, %{
               kind: "owner_command",
               owner: "workflow_lifecycle",
               command: "retry"
             })
             |> WorkflowLifecycleCompensation.route()

    assert {:error, {:unexpected_workflow_signal, "operator.retry"}} =
             signal_compensation_attrs()
             |> put_in([:owner_command_or_signal, :signal], "operator.retry")
             |> WorkflowLifecycleCompensation.route()

    assert {:error, {:missing_target_fields, ["idempotency_key"]}} =
             signal_compensation_attrs()
             |> update_in([:owner_command_or_signal], &Map.delete(&1, :idempotency_key))
             |> WorkflowLifecycleCompensation.route()
  end

  defp signal_compensation_attrs do
    %{
      compensation_ref: "compensation:workflow:201",
      source_context: "workflow_lifecycle",
      source_event_ref: "workflow-event://workflow-201/failure",
      failed_step_ref: "await_receipt_signal",
      tenant_id: "tenant-acme",
      installation_id: "installation-main",
      trace_id: "trace-201",
      causation_id: "cause-201",
      canonical_idempotency_key: "idem-root-201",
      compensation_owner: "workflow_lifecycle",
      compensation_kind: "retry",
      owner_command_or_signal: %{
        kind: "workflow_signal",
        workflow_id: "workflow-201",
        workflow_run_id: "run-201",
        signal: "workflow.compensation.retry",
        idempotency_key: "idem-compensation-201"
      },
      precondition: "workflow is accepted_active and lower receipt did not arrive before SLA",
      side_effect_scope: "workflow lifecycle signal only",
      retry_policy: %{max_attempts: 3, backoff_ms: 5_000},
      dead_letter_ref: "dead-letter:workflow:201",
      operator_action_ref: nil,
      audit_or_evidence_ref: "audit:workflow-compensation:201",
      release_manifest_ref: "phase5_hardening_metrics[30]"
    }
  end
end
