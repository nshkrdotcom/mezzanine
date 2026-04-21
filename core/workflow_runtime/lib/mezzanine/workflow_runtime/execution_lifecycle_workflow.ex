defmodule Mezzanine.WorkflowRuntime.ExecutionLifecycleWorkflow do
  @moduledoc """
  Phase 4 execution lifecycle workflow contract.

  The workflow is deterministic and ref-oriented. It records compact activity
  refs, lower refs, routing facts, and terminal policy records while lower
  domain truth stays in the owning repositories.
  """

  alias Mezzanine.WorkflowExecutionLifecycleInput
  alias Mezzanine.WorkflowReceiptSignal
  alias Mezzanine.WorkflowSignalReceipt
  alias Mezzanine.WorkflowTerminalReceiptPolicy

  @release_manifest_ref "phase4-v6-milestone27-execution-lifecycle-workflow"
  @workflow_contract "Mezzanine.WorkflowExecutionLifecycleInput.v1"
  @signal_contract "Mezzanine.WorkflowReceiptSignal.v1"
  @terminal_policy_contract "Mezzanine.WorkflowTerminalReceiptPolicy.v1"

  @activity_sequence [
    :compile_citadel_authority,
    :submit_jido_lower_run,
    :await_receipt_signal,
    :persist_terminal_receipt
  ]

  @doc "Static contract shape for Scenario 93, 94, and 95."
  @spec contract() :: map()
  def contract do
    %{
      workflow_module: Mezzanine.Workflows.ExecutionAttempt,
      workflow_contract: @workflow_contract,
      receipt_signal_contract: @signal_contract,
      terminal_policy_contract: @terminal_policy_contract,
      task_queue: "mezzanine.hazmat",
      activity_sequence: @activity_sequence,
      activity_owners: %{
        compile_citadel_authority: :citadel,
        submit_jido_lower_run: :jido_integration,
        execution_plane_side_effect: :execution_plane,
        persist_terminal_receipt: :mezzanine
      },
      query_boundary: Mezzanine.WorkflowRuntime,
      signal_boundary: Mezzanine.WorkflowRuntime,
      release_manifest_ref: @release_manifest_ref
    }
  end

  @doc "Build a workflow input from the enterprise pre-cut envelope."
  @spec new_input(map() | keyword()) ::
          {:ok, WorkflowExecutionLifecycleInput.t()} | {:error, term()}
  def new_input(attrs), do: attrs |> with_release_ref() |> WorkflowExecutionLifecycleInput.new()

  @doc "Run the deterministic execution-attempt workflow to the receipt wait point."
  @spec run(map() | keyword()) :: {:ok, map()} | {:error, term()}
  def run(attrs) do
    with {:ok, input} <- new_input(attrs),
         {:ok, authority} <- compile_citadel_authority_activity(input),
         {:ok, lower} <- submit_jido_lower_run_activity(input) do
      {:ok, runtime_result(input, authority, lower)}
    end
  end

  @doc "Build the compact workflow result from durable activity outputs."
  @spec runtime_result(WorkflowExecutionLifecycleInput.t(), map(), map()) :: map()
  def runtime_result(%WorkflowExecutionLifecycleInput{} = input, authority, lower) do
    authority = normalize(authority)
    lower = normalize(lower)

    %{
      workflow_id: input.workflow_id,
      workflow_type: input.workflow_type,
      workflow_version: input.workflow_version,
      workflow_state: "accepted_active",
      activity_refs: [
        authority.activity_call_ref,
        lower.activity_call_ref
      ],
      lower_refs: [lower.lower_submission_ref],
      trace_id: input.trace_id,
      resource_ref: input.resource_ref,
      routing_facts: input.routing_facts,
      history_policy: "compact_refs_only"
    }
  end

  @doc "Initial in-memory workflow state used by pure tests and query projections."
  @spec initial_state(map() | keyword()) :: map()
  def initial_state(attrs) do
    attrs = normalize(attrs)

    %{
      workflow_id: attrs.workflow_id,
      workflow_run_id: attrs[:workflow_run_id],
      workflow_state: "accepted_active",
      tenant_ref: attrs.tenant_ref,
      resource_ref: attrs.resource_ref,
      authority_packet_ref: attrs.authority_packet_ref,
      permission_decision_ref: attrs.permission_decision_ref,
      idempotency_key: attrs.idempotency_key,
      trace_id: attrs.trace_id,
      correlation_id: attrs.correlation_id,
      release_manifest_ref: Map.get(attrs, :release_manifest_ref, @release_manifest_ref),
      seen_signal_keys: MapSet.new(),
      signal_state: "pending"
    }
  end

  @doc "Citadel authority compilation activity result."
  @spec compile_citadel_authority_activity(map() | struct()) :: {:ok, map()} | {:error, term()}
  def compile_citadel_authority_activity(attrs) do
    with {:ok, input} <- new_input(attrs) do
      {:ok,
       %{
         activity: :compile_citadel_authority,
         activity_call_ref: "activity://#{input.workflow_id}/compile-authority",
         owner_repo: :citadel,
         authority_packet_ref: input.authority_packet_ref,
         permission_decision_ref: input.permission_decision_ref,
         trace_id: input.trace_id,
         routing_facts: input.routing_facts,
         result_ref: "authority-result://#{input.permission_decision_ref}"
       }}
    end
  end

  @doc "Jido lower submission activity with execution-plane side-effect ownership."
  @spec submit_jido_lower_run_activity(map() | struct()) :: {:ok, map()} | {:error, term()}
  def submit_jido_lower_run_activity(attrs) do
    with {:ok, input} <- new_input(attrs) do
      {:ok,
       %{
         activity: :submit_jido_lower_run,
         activity_call_ref: "activity://#{input.workflow_id}/submit-lower",
         owner_repo: :jido_integration,
         execution_plane_owner_repo: :execution_plane,
         lower_submission_ref: input.lower_submission_ref,
         lower_idempotency_key: input.lower_idempotency_key,
         idempotency_key: input.lower_idempotency_key,
         authority_packet_ref: input.authority_packet_ref,
         permission_decision_ref: input.permission_decision_ref,
         lease_broker: Mezzanine.ActivityLeaseBroker,
         trace_id: input.trace_id,
         routing_facts: input.routing_facts,
         result_ref: "lower-result://#{input.lower_submission_ref}"
       }}
    end
  end

  @doc "Terminal receipt persistence activity result."
  @spec persist_terminal_receipt_activity(map() | struct()) :: {:ok, map()} | {:error, term()}
  def persist_terminal_receipt_activity(attrs) do
    attrs = normalize(attrs)

    required = [
      :workflow_id,
      :terminal_state,
      :terminal_event_ref,
      :lower_receipt_ref,
      :trace_id,
      :release_manifest_ref
    ]

    case missing_required(attrs, required) do
      [] ->
        {:ok,
         %{
           activity: :persist_terminal_receipt,
           activity_call_ref: "activity://#{attrs.workflow_id}/persist-terminal-receipt",
           owner_repo: :mezzanine,
           terminal_state: attrs.terminal_state,
           terminal_event_ref: attrs.terminal_event_ref,
           lower_receipt_ref: attrs.lower_receipt_ref,
           trace_id: attrs.trace_id,
           release_manifest_ref: attrs.release_manifest_ref,
           result_ref: "terminal-receipt://#{attrs.workflow_id}/#{attrs.lower_receipt_ref}"
         }}

      missing ->
        {:error, {:missing_required_fields, missing}}
    end
  end

  @doc "Normalize a lower receipt into the workflow signal contract."
  @spec receipt_signal(map() | keyword()) :: {:ok, WorkflowReceiptSignal.t()} | {:error, term()}
  def receipt_signal(attrs), do: attrs |> with_release_ref() |> WorkflowReceiptSignal.new()

  @doc "Deliver a receipt signal through the workflow runtime and return the local receipt projection."
  @spec deliver_receipt_signal(map() | keyword()) :: {:ok, map()} | {:error, term()}
  def deliver_receipt_signal(attrs) do
    normalized = attrs |> with_release_ref()

    with {:ok, signal} <- WorkflowReceiptSignal.new(normalized),
         request <- receipt_signal_request(signal),
         {:ok, runtime_receipt} <- Mezzanine.WorkflowRuntime.signal_workflow(request),
         {:ok, signal_receipt} <- local_signal_receipt(signal, normalized, runtime_receipt) do
      {:ok,
       %{
         signal: signal,
         runtime_receipt: sanitize_runtime_receipt(runtime_receipt),
         signal_receipt: signal_receipt
       }}
    end
  end

  @doc "Apply a receipt signal with signal-idempotency suppression."
  @spec apply_receipt_signal(map(), WorkflowReceiptSignal.t()) :: {:ok, map()} | {:error, term()}
  def apply_receipt_signal(state, %WorkflowReceiptSignal{} = signal) do
    seen = Map.get(state, :seen_signal_keys, MapSet.new())

    if MapSet.member?(seen, signal.idempotency_key) do
      {:ok, Map.put(state, :signal_state, "duplicate_suppressed")}
    else
      {:ok,
       state
       |> Map.put(:workflow_state, terminal_state(signal))
       |> Map.put(:signal_state, "accepted")
       |> Map.put(:last_receipt_ref, signal.lower_receipt_ref)
       |> Map.put(:seen_signal_keys, MapSet.put(seen, signal.idempotency_key))
       |> Map.put(:routing_facts, signal.routing_facts)}
    end
  end

  @doc "Classify a receipt that arrives after terminal workflow state."
  @spec terminal_receipt_policy(map(), WorkflowReceiptSignal.t()) ::
          {:ok, WorkflowTerminalReceiptPolicy.t()} | {:error, term()}
  def terminal_receipt_policy(state, %WorkflowReceiptSignal{} = signal) do
    WorkflowTerminalReceiptPolicy.new(%{
      tenant_ref: signal.tenant_ref,
      installation_ref: signal.installation_ref,
      workspace_ref: signal.workspace_ref,
      project_ref: signal.project_ref,
      environment_ref: signal.environment_ref,
      principal_ref: signal.principal_ref,
      system_actor_ref: signal.system_actor_ref,
      resource_ref: signal.resource_ref,
      workflow_id: signal.workflow_id,
      workflow_run_id: signal.workflow_run_id,
      terminal_state: Map.get(state, :workflow_state, "terminal"),
      terminal_event_ref: "workflow-event://#{signal.workflow_id}/terminal",
      late_receipt_ref: signal.lower_receipt_ref,
      policy_result: "quarantined_late_receipt",
      incident_ref: "incident://#{signal.workflow_id}/late-receipt/#{signal.lower_receipt_ref}",
      authority_packet_ref: signal.authority_packet_ref,
      permission_decision_ref: signal.permission_decision_ref,
      idempotency_key: signal.idempotency_key,
      trace_id: signal.trace_id,
      correlation_id: signal.correlation_id,
      release_manifest_ref: signal.release_manifest_ref
    })
  end

  @doc "Query operator state through the public workflow runtime boundary."
  @spec query_operator_state(map() | keyword()) :: {:ok, map()} | {:error, term()}
  def query_operator_state(attrs) do
    attrs = normalize(attrs)

    request = %{
      workflow_id: attrs.workflow_id,
      query_name: "operator_state.v1",
      tenant_ref: attrs.tenant_ref,
      resource_ref: attrs.resource_ref,
      trace_id: attrs.trace_id,
      release_manifest_ref: Map.get(attrs, :release_manifest_ref, @release_manifest_ref)
    }

    with {:ok, query} <- Mezzanine.WorkflowRuntime.query_workflow(request) do
      {:ok,
       query
       |> normalize()
       |> Map.drop([:raw_temporalex_result, :temporalex_struct, :raw_history_event])}
    end
  end

  @doc "Replay-safe worker failover posture for Scenario 93."
  @spec worker_failover_recovery(map() | keyword()) :: {:ok, map()} | {:error, term()}
  def worker_failover_recovery(attrs) do
    with {:ok, input} <- new_input(attrs) do
      {:ok,
       %{
         recovery_mode: "temporal_replay",
         task_queue: "mezzanine.hazmat",
         workflow_id: input.workflow_id,
         workflow_version: input.workflow_version,
         idempotency_key: input.idempotency_key,
         lower_idempotency_key: input.lower_idempotency_key,
         last_completed_event_ref: "workflow-event://#{input.workflow_id}/last-completed",
         next_task_ref: "workflow-task://#{input.workflow_id}/next",
         stranded?: false
       }}
    end
  end

  @doc "Operator incident fields for execution lifecycle reconstruction."
  @spec incident_bundle_fields(map() | keyword()) :: map()
  def incident_bundle_fields(attrs) do
    attrs
    |> normalize()
    |> Map.take([
      :tenant_ref,
      :resource_ref,
      :workflow_id,
      :workflow_run_id,
      :command_receipt_ref,
      :lower_submission_ref,
      :authority_packet_ref,
      :permission_decision_ref,
      :trace_id,
      :correlation_id,
      :release_manifest_ref
    ])
  end

  defp terminal_state(%WorkflowReceiptSignal{terminal?: true, receipt_state: state}), do: state
  defp terminal_state(_signal), do: "accepted_active"

  defp receipt_signal_request(%WorkflowReceiptSignal{} = signal) do
    %{
      workflow_id: signal.workflow_id,
      workflow_run_id: signal.workflow_run_id,
      signal_id: signal.signal_id,
      signal_name: signal.signal_name,
      signal_version: signal.signal_version,
      signal_payload_ref: "workflow-signal://#{signal.workflow_id}/#{signal.signal_id}",
      idempotency_key: signal.idempotency_key,
      tenant_ref: signal.tenant_ref,
      resource_ref: signal.resource_ref,
      authority_packet_ref: signal.authority_packet_ref,
      permission_decision_ref: signal.permission_decision_ref,
      trace_id: signal.trace_id,
      correlation_id: signal.correlation_id,
      release_manifest_ref: signal.release_manifest_ref
    }
  end

  defp local_signal_receipt(%WorkflowReceiptSignal{} = signal, attrs, runtime_receipt) do
    runtime_receipt = normalize(runtime_receipt)

    WorkflowSignalReceipt.new(%{
      tenant_ref: signal.tenant_ref,
      installation_ref: signal.installation_ref,
      workspace_ref: signal.workspace_ref,
      project_ref: signal.project_ref,
      environment_ref: signal.environment_ref,
      principal_ref: signal.principal_ref,
      system_actor_ref: signal.system_actor_ref,
      resource_ref: signal.resource_ref,
      signal_id: signal.signal_id,
      workflow_id: signal.workflow_id,
      workflow_run_id: signal.workflow_run_id,
      signal_name: signal.signal_name,
      signal_version: signal.signal_version,
      signal_sequence: 0,
      command_id: Map.get(attrs, :command_id, signal.signal_id),
      authority_packet_ref: signal.authority_packet_ref,
      permission_decision_ref: signal.permission_decision_ref,
      idempotency_key: signal.idempotency_key,
      trace_id: signal.trace_id,
      correlation_id: signal.correlation_id,
      release_manifest_ref: signal.release_manifest_ref,
      payload_hash: "sha256:#{signal.idempotency_key}",
      payload_ref: "workflow-signal://#{signal.workflow_id}/#{signal.signal_id}",
      authority_state: "authorized",
      local_state: "accepted",
      dispatch_state:
        Map.get(runtime_receipt, :dispatch_state, Map.get(runtime_receipt, :status)),
      workflow_effect_state: "pending_ack",
      projection_state: "pending"
    })
  end

  defp sanitize_runtime_receipt(receipt) do
    receipt
    |> normalize()
    |> Map.drop([:raw_temporalex_result, :temporalex_struct, :raw_history_event, :task_token])
  end

  defp with_release_ref(attrs) do
    attrs
    |> normalize()
    |> Map.put_new(:release_manifest_ref, @release_manifest_ref)
  end

  defp normalize(attrs) when is_list(attrs), do: attrs |> Map.new() |> normalize_keys()
  defp normalize(%_{} = struct), do: struct |> Map.from_struct() |> normalize_keys()
  defp normalize(map) when is_map(map), do: normalize_keys(map)

  defp normalize_keys(map) do
    map
    |> Enum.map(fn {key, value} -> {normalize_key(key), value} end)
    |> Map.new()
  end

  defp normalize_key(key) when is_atom(key), do: key
  defp normalize_key(key) when is_binary(key), do: String.to_existing_atom(key)

  defp missing_required(attrs, required), do: Enum.reject(required, &present?(Map.get(attrs, &1)))

  defp present?(value) when is_binary(value), do: String.trim(value) != ""
  defp present?(value), do: not is_nil(value)
end
