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
  alias Mezzanine.Intent.RunIntent

  @release_manifest_ref "phase4-v6-milestone27-execution-lifecycle-workflow"
  @workflow_contract "Mezzanine.WorkflowExecutionLifecycleInput.v1"
  @signal_contract "Mezzanine.WorkflowReceiptSignal.v1"
  @terminal_policy_contract "Mezzanine.WorkflowTerminalReceiptPolicy.v1"
  @operator_signals [
    "operator.cancel",
    "operator.pause",
    "operator.resume",
    "operator.retry",
    "operator.replan",
    "operator.rework"
  ]

  @activity_sequence [
    :compile_citadel_authority,
    :submit_jido_lower_run,
    :await_receipt_signal,
    :persist_terminal_receipt,
    :cleanup_workspace,
    :publish_source,
    :materialize_evidence,
    :create_review
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
        persist_terminal_receipt: :mezzanine,
        cleanup_workspace: :mezzanine,
        publish_source: :jido_integration,
        materialize_evidence: :mezzanine,
        create_review: :mezzanine
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
         {:ok, lower} <- submit_jido_lower_run_activity(Map.put(Map.from_struct(input), :citadel_authority, authority)) do
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
    with {:ok, input} <- new_input(attrs),
         {:ok, run_intent} <- citadel_run_intent(input),
         {:ok, compile_attrs} <- citadel_compile_attrs(input),
         {:ok, compiled} <- compile_citadel_submission(run_intent, compile_attrs, policy_packs(input)) do
      {:ok,
       %{
         activity: :compile_citadel_authority,
         activity_call_ref: "activity://#{input.workflow_id}/compile-authority",
         owner_repo: :citadel,
         authority_packet_ref: input.authority_packet_ref,
         permission_decision_ref: input.permission_decision_ref,
         compiled_submission_ref: "citadel-compiled-submission://#{compile_attrs.execution_id}",
         citadel_decision_hash: Map.get(compiled, :decision_hash),
         invocation_request: get_in(compiled, [:lower_intent, :invocation_request]),
         trace_id: input.trace_id,
         routing_facts: input.routing_facts,
         result_ref: "authority-result://#{input.permission_decision_ref}"
       }}
    end
  end

  @doc "Jido lower submission activity with execution-plane side-effect ownership."
  @spec submit_jido_lower_run_activity(map() | struct()) :: {:ok, map()} | {:error, term()}
  def submit_jido_lower_run_activity(attrs) do
    attrs = normalize(attrs)

    with {:ok, input} <- new_input(attrs),
         {:ok, invocation} <- authorized_lower_invocation(input, Map.get(attrs, :citadel_authority)),
         {:ok, dispatched} <- invoke_authorized_lower(invocation) do
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
         provider_submission: dispatched,
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
        lower_receipt = lower_receipt_payload(attrs)
        projection_result = reduce_terminal_receipt(attrs, lower_receipt)

        {:ok,
         %{
           activity: :persist_terminal_receipt,
           activity_call_ref: "activity://#{attrs.workflow_id}/persist-terminal-receipt",
           owner_repo: :mezzanine,
           terminal_state: attrs.terminal_state,
           terminal_event_ref: attrs.terminal_event_ref,
           lower_receipt_ref: attrs.lower_receipt_ref,
           lower_receipt: lower_receipt,
           projection_result: projection_result,
           trace_id: attrs.trace_id,
           release_manifest_ref: attrs.release_manifest_ref,
           result_ref: "terminal-receipt://#{attrs.workflow_id}/#{attrs.lower_receipt_ref}"
         }}

      missing ->
        {:error, {:missing_required_fields, missing}}
    end
  end

  @doc "Terminal workspace cleanup activity result."
  @spec cleanup_workspace_activity(map() | struct()) :: {:ok, map()} | {:error, term()}
  def cleanup_workspace_activity(attrs) do
    attrs = normalize(attrs)

    case missing_required(attrs, [:workflow_id, :workspace_ref, :trace_id, :release_manifest_ref]) do
      [] ->
        {:ok,
         %{
           activity: :cleanup_workspace,
           activity_call_ref: "activity://#{attrs.workflow_id}/cleanup-workspace",
           owner_repo: :mezzanine,
           workspace_ref: attrs.workspace_ref,
           cleanup_policy: Map.get(attrs, :workspace_cleanup_policy, "terminal_policy"),
           trace_id: attrs.trace_id,
           release_manifest_ref: attrs.release_manifest_ref,
           result_ref: "workspace-cleanup://#{attrs.workflow_id}/#{attrs.workspace_ref}"
         }}

      missing ->
        {:error, {:missing_required_fields, missing}}
    end
  end

  @doc "Terminal source publication activity result."
  @spec publish_source_activity(map() | struct()) :: {:ok, map()} | {:error, term()}
  def publish_source_activity(attrs) do
    attrs = normalize(attrs)

    case missing_required(attrs, [:workflow_id, :resource_ref, :trace_id, :release_manifest_ref]) do
      [] ->
        source_publish_ref =
          Map.get(
            attrs,
            :source_publish_ref,
            "source-publish://#{attrs.workflow_id}/#{attrs.resource_ref}"
          )

        {:ok,
         %{
           activity: :publish_source,
           activity_call_ref: "activity://#{attrs.workflow_id}/publish-source",
           owner_repo: :jido_integration,
           source_publish_ref: source_publish_ref,
           resource_ref: attrs.resource_ref,
           trace_id: attrs.trace_id,
           release_manifest_ref: attrs.release_manifest_ref,
           result_ref: source_publish_ref
         }}

      missing ->
        {:error, {:missing_required_fields, missing}}
    end
  end

  @doc "Terminal evidence materialization activity result."
  @spec materialize_evidence_activity(map() | struct()) :: {:ok, map()} | {:error, term()}
  def materialize_evidence_activity(attrs) do
    attrs = normalize(attrs)

    case missing_required(attrs, [
           :workflow_id,
           :lower_receipt_ref,
           :trace_id,
           :release_manifest_ref
         ]) do
      [] ->
        evidence_ref =
          Map.get(
            attrs,
            :evidence_ref,
            "evidence://#{attrs.workflow_id}/#{attrs.lower_receipt_ref}"
          )

        {:ok,
         %{
           activity: :materialize_evidence,
           activity_call_ref: "activity://#{attrs.workflow_id}/materialize-evidence",
           owner_repo: :mezzanine,
           evidence_ref: evidence_ref,
           lower_receipt_ref: attrs.lower_receipt_ref,
           trace_id: attrs.trace_id,
           release_manifest_ref: attrs.release_manifest_ref,
           result_ref: evidence_ref
         }}

      missing ->
        {:error, {:missing_required_fields, missing}}
    end
  end

  @doc "Terminal review creation activity result."
  @spec create_review_activity(map() | struct()) :: {:ok, map()} | {:error, term()}
  def create_review_activity(attrs) do
    attrs = normalize(attrs)

    case missing_required(attrs, [
           :workflow_id,
           :lower_receipt_ref,
           :trace_id,
           :release_manifest_ref
         ]) do
      [] ->
        review_ref =
          Map.get(attrs, :review_ref, "review://#{attrs.workflow_id}/#{attrs.lower_receipt_ref}")

        {:ok,
         %{
           activity: :create_review,
           activity_call_ref: "activity://#{attrs.workflow_id}/create-review",
           owner_repo: :mezzanine,
           review_ref: review_ref,
           lower_receipt_ref: attrs.lower_receipt_ref,
           review_required: get_in(attrs, [:routing_facts, :review_required]),
           trace_id: attrs.trace_id,
           release_manifest_ref: attrs.release_manifest_ref,
           result_ref: review_ref
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

  @doc "Compact policy fields used by the execution turn loop."
  @spec execution_control_policy(map() | keyword()) :: map()
  def execution_control_policy(attrs) do
    attrs = normalize(attrs)

    %{
      retry_policy: Map.get(attrs, :retry_policy, %{max_attempts: 3}),
      max_turns: Map.get(attrs, :max_turns, :unbounded),
      stall_timeout_ms: Map.get(attrs, :stall_timeout_ms),
      non_interactive_policy: %{
        approval_required: :failure,
        input_required: :blocked
      },
      operator_signals: @operator_signals
    }
  end

  @doc "Deterministic turn-loop decision used by workflow tests and replay-safe reducers."
  @spec turn_loop_decision(map() | keyword()) ::
          {:continue | :stop | :retry | :blocked | :failure | :finalize, map()}
  def turn_loop_decision(attrs) do
    attrs = normalize(attrs)

    cond do
      Map.get(attrs, :receipt_state) == "input_required" ->
        {:blocked, %{reason: :input_required, safe_action: :operator_review}}

      Map.get(attrs, :receipt_state) == "approval_required" ->
        {:failure, %{reason: :approval_required, safe_action: :operator_review}}

      max_turns_reached?(attrs) ->
        {:stop, %{reason: :max_turns_reached, safe_action: :finalize_or_review}}

      Map.get(attrs, :stalled?) == true ->
        {:retry, %{reason: :stall_timeout, safe_action: :retry_or_cancel}}

      Map.get(attrs, :source_state) == "terminal" ->
        {:finalize, %{reason: :source_terminal, safe_action: :terminal_cleanup}}

      true ->
        {:continue, %{reason: :active_state_continuation, safe_action: :next_turn}}
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

  defp max_turns_reached?(attrs) do
    case {Map.get(attrs, :turn_count), Map.get(attrs, :max_turns)} do
      {turn_count, max_turns} when is_integer(turn_count) and is_integer(max_turns) ->
        turn_count >= max_turns

      _other ->
        false
    end
  end

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

  defp reduce_terminal_receipt(attrs, lower_receipt) do
    reducer =
      Application.get_env(
        :mezzanine_workflow_runtime,
        :receipt_reducer,
        Module.concat([Mezzanine, Projections, ReceiptReducer])
      )

    reducer_attrs =
      %{
        installation_id: parse_installation_ref(attrs.installation_ref),
        subject_id: subject_id_from_attrs(attrs),
        execution_id: execution_id_from_attrs(attrs),
        trace_id: attrs.trace_id,
        causation_id: attrs.correlation_id,
        receipt_id: Map.get(attrs, :signal_id, attrs.lower_receipt_ref),
        receipt_state: Map.get(attrs, :receipt_state, attrs.terminal_state),
        lower_receipt_ref: attrs.lower_receipt_ref,
        lower_receipt: lower_receipt,
        required_evidence: get_in(attrs, [:routing_facts, :required_evidence]) || []
      }

    if Code.ensure_loaded?(reducer) and function_exported?(reducer, :reduce, 1) do
      case reducer.reduce(reducer_attrs) do
        {:ok, result} -> result
        {:error, reason} -> %{projection_name: "operator_subject_runtime", reducer_error: reason}
      end
    else
      %{projection_name: "operator_subject_runtime", reducer_loaded?: false}
    end
  end

  defp lower_receipt_payload(attrs) do
    routing = Map.get(attrs, :routing_facts, %{}) || %{}

    %{
      receipt_id: Map.get(attrs, :signal_id, attrs.lower_receipt_ref),
      receipt_state: Map.get(attrs, :receipt_state, attrs.terminal_state),
      lower_receipt_ref: attrs.lower_receipt_ref,
      run_id: Map.get(attrs, :lower_run_ref),
      attempt_id: Map.get(attrs, :lower_attempt_ref),
      lower_event_ref: Map.get(attrs, :lower_event_ref),
      provider_object_refs: Map.get(routing, :provider_object_refs) || Map.get(routing, "provider_object_refs") || [],
      evidence_artifact_refs:
        Map.get(routing, :evidence_artifact_refs) || Map.get(routing, "evidence_artifact_refs") || [],
      trace_id: attrs.trace_id,
      causation_id: attrs.correlation_id,
      idempotency_key: attrs.idempotency_key
    }
  end

  defp subject_id_from_attrs(attrs) do
    routing = Map.get(attrs, :routing_facts, %{}) || %{}

    Map.get(routing, :subject_id) ||
      Map.get(routing, "subject_id") ||
      case Map.get(attrs, :subject_ref) do
        value when is_binary(value) -> value
        %{} = map -> Map.get(map, :id) || Map.get(map, "id")
        _other -> attrs.resource_ref
      end
  end

  defp execution_id_from_attrs(attrs) do
    routing = Map.get(attrs, :routing_facts, %{}) || %{}
    Map.get(routing, :execution_id) || Map.get(routing, "execution_id") || attrs.command_id
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

  defp citadel_run_intent(%WorkflowExecutionLifecycleInput{} = input) do
    routing = routing_facts(input)

    RunIntent.new(%{
      intent_id: Map.get(routing, :intent_id, input.command_id),
      program_id: Map.get(routing, :program_id, input.project_ref || "program:default"),
      work_id: required_routing!(input, routing, :subject_id),
      capability: required_routing!(input, routing, :capability),
      runtime_class: routing_atom(routing, :runtime_class, :session),
      placement: %{
        target_id: Map.get(routing, :target_id, "workspace_runtime"),
        service_id: Map.get(routing, :service_id, "workspace_runtime"),
        boundary_class: Map.get(routing, :boundary_class, "workspace_session"),
        routing_tags: List.wrap(Map.get(routing, :routing_tags, []))
      },
      grant_profile: %{"allowed_tools" => List.wrap(Map.get(routing, :allowed_tools, []))},
      input: Map.get(routing, :execution_intent, %{}),
      metadata: %{
        "tenant_id" => input.tenant_ref,
        "installation_id" => installation_id(input, routing),
        "policy_version" => Map.get(routing, :policy_version, "workflow-runtime-policy-v1"),
        "workspace_root" => Map.get(routing, :workspace_root),
        "environment" => input.environment_ref || "default"
      }
    })
  end

  defp citadel_compile_attrs(%WorkflowExecutionLifecycleInput{} = input) do
    routing = routing_facts(input)
    installation_revision = required_revision!(input, routing)
    expected_revision = Map.get(routing, :expected_installation_revision)

    if is_integer(expected_revision) and expected_revision != installation_revision do
      {:error,
       {:stale_installation_revision,
        %{expected_installation_revision: expected_revision, installation_revision: installation_revision}}}
    else
      capability = required_routing!(input, routing, :capability)
      subject_id = required_routing!(input, routing, :subject_id)
      execution_id = Map.get(routing, :execution_id, input.command_id)

      {:ok,
       %{
         tenant_id: input.tenant_ref,
         installation_id: installation_id(input, routing),
         installation_revision: installation_revision,
         actor_ref: Map.get(routing, :actor_ref, input.principal_ref),
         subject_id: subject_id,
         execution_id: execution_id,
         request_trace_id: input.trace_id,
         substrate_trace_id: Map.get(routing, :substrate_trace_id, input.trace_id),
         idempotency_key: input.idempotency_key,
         submission_dedupe_key: input.lower_idempotency_key,
         target_id: Map.get(routing, :target_id, "workspace_runtime"),
         service_id: Map.get(routing, :service_id, "workspace_runtime"),
         boundary_class: Map.get(routing, :boundary_class, "workspace_session"),
         scope_kind: Map.get(routing, :scope_kind, "work_object"),
         target_kind: Map.get(routing, :target_kind, "runtime_target"),
         execution_intent_family: Map.get(routing, :execution_intent_family, "process"),
         execution_intent:
           Map.get(routing, :execution_intent, %{
             "command" => capability,
             "subject_id" => subject_id,
             "trace_id" => input.trace_id
           }),
         allowed_operations: List.wrap(Map.get(routing, :allowed_operations, [capability])),
         downstream_scope: Map.get(routing, :downstream_scope, "subject:#{subject_id}"),
         workspace_mutability: Map.get(routing, :workspace_mutability, "read_write"),
         risk_hints: List.wrap(Map.get(routing, :risk_hints, [])),
         policy_refs: List.wrap(Map.get(routing, :policy_refs, [input.permission_decision_ref]))
       }}
    end
  rescue
    error in ArgumentError -> {:error, Exception.message(error)}
  end

  defp compile_citadel_submission(%RunIntent{} = run_intent, attrs, policy_packs) do
    bridge = Application.get_env(:mezzanine_workflow_runtime, :citadel_bridge, Mezzanine.CitadelBridge)

    case bridge.compile_submission(run_intent, attrs, policy_packs, []) do
      {:ok, %{rejection_classification: nil} = compiled} -> {:ok, compiled}
      {:ok, %{rejection_classification: rejection} = compiled} -> {:error, {:citadel_rejected, rejection, compiled}}
      {:error, reason} -> {:error, {:citadel_rejected, reason}}
    end
  end

  defp authorized_lower_invocation(%WorkflowExecutionLifecycleInput{} = input, authority) do
    authority = normalize_authority(authority)
    invocation_request = Map.get(authority, :invocation_request)

    if is_nil(invocation_request) do
      {:error, :missing_citadel_authority}
    else
      routing = routing_facts(input)

      invocation_attrs = %{
        tenant_id: input.tenant_ref,
        installation_id: installation_id(input, routing),
        subject_id: required_routing!(input, routing, :subject_id),
        execution_id: Map.get(routing, :execution_id, input.command_id),
        trace_id: input.trace_id,
        idempotency_key: input.idempotency_key,
        submission_dedupe_key: input.lower_idempotency_key,
        expected_installation_revision: Map.get(routing, :installation_revision),
        invocation_request: invocation_request
      }

      build_authorized_invocation(invocation_attrs)
    end
  end

  defp invoke_authorized_lower(invocation) do
    bridge = Application.get_env(:mezzanine_workflow_runtime, :integration_bridge, Mezzanine.IntegrationBridge)
    bridge.invoke_run_intent(invocation, [])
  end

  defp build_authorized_invocation(attrs) do
    module = Module.concat([Mezzanine, IntegrationBridge, AuthorizedInvocation])

    if Code.ensure_loaded?(module) and function_exported?(module, :new, 1) do
      module.new(attrs)
    else
      {:ok, Map.put(attrs, :authorized_invocation_boundary, module)}
    end
  end

  defp normalize_authority(nil), do: %{}
  defp normalize_authority(authority) when is_map(authority), do: normalize(authority)

  defp policy_packs(%WorkflowExecutionLifecycleInput{} = input) do
    routing = routing_facts(input)

    [
      %{
        pack_id: Map.get(routing, :policy_pack_id, "workflow-runtime-default"),
        policy_version: Map.get(routing, :policy_version, "workflow-runtime-policy-v1"),
        policy_epoch: Map.get(routing, :policy_epoch, 0),
        priority: 0,
        selector: %{
          tenant_ids: [],
          scope_kinds: [],
          environments: [],
          default?: true,
          extensions: %{}
        },
        profiles: %{
          trust_profile: "baseline",
          approval_profile: "standard",
          egress_profile: "restricted",
          workspace_profile: "workspace",
          resource_profile: "standard",
          boundary_class: Map.get(routing, :boundary_class, "workspace_session"),
          extensions: %{}
        },
        rejection_policy: %{
          denial_audit_reason_codes: ["policy_denied", "approval_missing"],
          derived_state_reason_codes: ["planning_failed"],
          runtime_change_reason_codes: ["scope_unavailable", "service_hidden", "boundary_stale"],
          governance_change_reason_codes: ["approval_missing"],
          extensions: %{}
        },
        extensions: %{}
      }
    ]
  end

  defp routing_facts(%WorkflowExecutionLifecycleInput{routing_facts: routing}) do
    routing
    |> normalize()
    |> Map.new(fn
      {key, value} when is_binary(key) -> {String.to_atom(key), value}
      pair -> pair
    end)
  end

  defp required_routing!(input, routing, key) do
    value = Map.get(routing, key) || fallback_routing(input, key)

    if present?(value) do
      value
    else
      raise ArgumentError, "missing required Citadel routing fact #{inspect(key)}"
    end
  end

  defp required_revision!(_input, routing) do
    case Map.get(routing, :installation_revision) do
      value when is_integer(value) and value >= 0 -> value
      nil -> raise ArgumentError, "missing required Citadel routing fact :installation_revision"
      value -> raise ArgumentError, "invalid Citadel installation_revision: #{inspect(value)}"
    end
  end

  defp fallback_routing(%WorkflowExecutionLifecycleInput{} = input, :subject_id) do
    case input.subject_ref do
      value when is_binary(value) -> value
      %{} = map -> Map.get(map, "id") || Map.get(map, :id)
      _other -> nil
    end
  end

  defp fallback_routing(_input, _key), do: nil

  defp installation_id(%WorkflowExecutionLifecycleInput{} = input, routing) do
    Map.get(routing, :installation_id) || parse_installation_ref(input.installation_ref)
  end

  defp parse_installation_ref("installation://" <> rest), do: rest |> String.split("@") |> hd()
  defp parse_installation_ref(value), do: value

  defp routing_atom(routing, key, default) do
    case Map.get(routing, key, default) do
      value when is_atom(value) -> value
      value when is_binary(value) -> String.to_atom(value)
      _other -> default
    end
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
