defmodule Mezzanine.WorkflowRuntime.ExecutionLifecycleWorkflow do
  @moduledoc """
  Phase 4 execution lifecycle workflow contract.

  The workflow is deterministic and ref-oriented. It records compact activity
  refs, lower refs, routing facts, and terminal policy records while lower
  domain truth stays in the owning repositories.
  """

  alias Mezzanine.GovernedRuntimeConfig
  alias Mezzanine.Intent.RunIntent
  alias Mezzanine.WorkflowExecutionLifecycleInput
  alias Mezzanine.WorkflowReceiptSignal
  alias Mezzanine.WorkflowSignalReceipt
  alias Mezzanine.WorkflowTerminalReceiptPolicy

  @release_manifest_ref "phase4-v6-milestone27-execution-lifecycle-workflow"
  @workflow_contract "Mezzanine.WorkflowExecutionLifecycleInput.v1"
  @signal_contract "Mezzanine.WorkflowReceiptSignal.v1"
  @terminal_policy_contract "Mezzanine.WorkflowTerminalReceiptPolicy.v1"
  @authorized_invocation_module :"Elixir.Mezzanine.IntegrationBridge.AuthorizedInvocation"
  @receipt_reducer_module :"Elixir.Mezzanine.Projections.ReceiptReducer"
  @operator_signals [
    "operator.cancel",
    "operator.pause",
    "operator.resume",
    "operator.retry",
    "operator.replan",
    "operator.rework"
  ]
  @normalizable_keys [
    :actor_ref,
    :allowed_operations,
    :allowed_tools,
    :authority_packet_ref,
    :authority_decision,
    :authority_decision_hash,
    :authority_ref,
    :boundary_class,
    :capability,
    :capability_id,
    :capability_negotiation_ref,
    :capability_negotiation_refs,
    :capability_negotiations,
    :citadel_authority,
    :command_id,
    :command_receipt_ref,
    :connector_manifest_ref,
    :connector_manifest_refs,
    :connector_manifests,
    :correlation_id,
    :cwd,
    :decision_hash,
    :denial_refs,
    :dispatch_state,
    :downstream_scope,
    :acceptance,
    :aitrace,
    :artifact_refs,
    :attestation_requirement_ref,
    :credential,
    :evidence_artifact_refs,
    :evidence_ref,
    :execution_id,
    :expected_installation_revision,
    :failure,
    :github_pr_evidence,
    :governed_lower_envelope,
    :id,
    :installation_id,
    :installation_revision,
    :intent_id,
    :incident_bundles,
    :last_receipt_ref,
    :lower_attempt_ref,
    :lower_event_ref,
    :lower_envelope,
    :lower_request_ref,
    :lower_dispatch_opts,
    :lower_receipt_ref,
    :lower_runtime_kind,
    :lower_run_ref,
    :lower_submission_ref,
    :max_turns,
    :package_refs,
    :permission_decision_ref,
    :policy_bundle_refs,
    :policy_epoch,
    :policy_pack_id,
    :policy_refs,
    :policy_version,
    :prompt_provenance,
    :program_id,
    :provider_object_refs,
    :provider_account,
    :raw_history_event,
    :raw_temporalex_result,
    :receipt_state,
    :release_manifest_ref,
    :required_evidence,
    :resource_ref,
    :resource_scope_refs,
    :review_ref,
    :review_required,
    :risk_hints,
    :runtime_events,
    :routing_facts,
    :routing_tags,
    :runtime_class,
    :runtime_profile,
    :runtime_profile_kind,
    :runtime_profile_ref,
    :rate_limit,
    :retry,
    :retry_receipts,
    :sandbox_profile_ref,
    :script_refs,
    :semantic_failure,
    :scope_kind,
    :seen_signal_keys,
    :service_id,
    :signal_id,
    :signal_state,
    :source_publish_ref,
    :source_active?,
    :source_state,
    :source_terminal?,
    :source_terminal,
    :source_visible?,
    :stall_timeout_ms,
    :status,
    :subject_id,
    :subject_ref,
    :substrate_trace_id,
    :target_id,
    :target_kind,
    :task_token,
    :temporalex_struct,
    :tenant_ref,
    :terminal_event_ref,
    :terminal_state,
    :trace_id,
    :turn_count,
    :citadel_bridge,
    :integration_bridge,
    :receipt_reducer,
    :runtime_modules,
    :workflow_runtime_impl,
    :workflow_runtime_modules,
    :workflow_id,
    :workflow_run_id,
    :workflow_state,
    :workspace_cleanup_policy,
    :workspace_mutability,
    :workspace_ref,
    :workspace_root,
    :workpad_refs,
    :assigned_to_current_worker?,
    :token_dedupe,
    :token_totals,
    :acceptable_attestation,
    :action_id,
    :attempt_ref,
    :cedar_schema_hash,
    :cedar_schema_ref,
    :connector_manifest_hash,
    :connector_manifest_state,
    :declared_actions,
    :evidence_profile_ref,
    :filesystem_policy_ref,
    :idempotency_class,
    :input_hash,
    :input_ref,
    :network_policy_ref,
    :placement_ref,
    :policy_bundle_hash,
    :policy_bundle_ref,
    :policy_profile_ref,
    :redaction_profile_ref,
    :run_ref,
    :sandbox_level,
    :script_api_version,
    :script_hash,
    :script_ref,
    :side_effect_class,
    :target_ref,
    :workflow_ref
  ]
  @key_lookup Map.new(@normalizable_keys, &{Atom.to_string(&1), &1})
  @routing_atom_lookup %{"session" => :session, "workflow" => :workflow}

  @activity_sequence [
    :compile_citadel_authority,
    :submit_jido_lower_run,
    :await_receipt_signal,
    :persist_terminal_receipt,
    :cleanup_workspace,
    :publish_source,
    :update_runtime_projection,
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
        update_runtime_projection: :mezzanine,
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
    attrs = normalize(attrs)

    with {:ok, input} <- new_input(attrs),
         {:ok, authority} <- compile_citadel_authority_activity(attrs),
         {:ok, lower} <-
           submit_jido_lower_run_activity(
             attrs
             |> Map.merge(Map.from_struct(input))
             |> Map.put(:citadel_authority, authority)
           ) do
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
    attrs = normalize(attrs)

    with {:ok, input} <- new_input(attrs),
         {:ok, run_intent} <- citadel_run_intent(input),
         {:ok, compile_attrs} <- citadel_compile_attrs(input),
         {:ok, compiled} <-
           compile_citadel_submission(run_intent, compile_attrs, policy_packs(input), attrs) do
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
         {:ok, invocation} <-
           authorized_lower_invocation(input, Map.get(attrs, :citadel_authority)),
         {:ok, dispatched} <- invoke_authorized_lower(invocation, attrs) do
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
    reduce_runtime_projection_activity(
      attrs,
      :persist_terminal_receipt,
      "persist-terminal-receipt",
      "terminal-receipt"
    )
  end

  @doc "Merge terminal side-effect refs into the reducer-owned runtime projection."
  @spec update_runtime_projection_activity(map() | struct()) :: {:ok, map()} | {:error, term()}
  def update_runtime_projection_activity(attrs) do
    reduce_runtime_projection_activity(
      attrs,
      :update_runtime_projection,
      "update-runtime-projection",
      "runtime-projection"
    )
  end

  defp reduce_runtime_projection_activity(attrs, activity, activity_slug, result_scheme) do
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
           activity: activity,
           activity_call_ref: "activity://#{attrs.workflow_id}/#{activity_slug}",
           owner_repo: :mezzanine,
           terminal_state: attrs.terminal_state,
           terminal_event_ref: attrs.terminal_event_ref,
           lower_receipt_ref: attrs.lower_receipt_ref,
           lower_receipt: lower_receipt,
           projection_result: projection_result,
           trace_id: attrs.trace_id,
           release_manifest_ref: attrs.release_manifest_ref,
           result_ref: "#{result_scheme}://#{attrs.workflow_id}/#{attrs.lower_receipt_ref}"
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

        base_result = %{
          activity: :publish_source,
          activity_call_ref: "activity://#{attrs.workflow_id}/publish-source",
          owner_repo: :jido_integration,
          source_publish_ref: source_publish_ref,
          resource_ref: attrs.resource_ref,
          trace_id: attrs.trace_id,
          release_manifest_ref: attrs.release_manifest_ref,
          result_ref: source_publish_ref
        }

        maybe_publish_source(base_result, attrs, source_publish_ref)

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

        result = %{
          activity: :materialize_evidence,
          activity_call_ref: "activity://#{attrs.workflow_id}/materialize-evidence",
          owner_repo: :mezzanine,
          evidence_ref: evidence_ref,
          lower_receipt_ref: attrs.lower_receipt_ref,
          trace_id: attrs.trace_id,
          release_manifest_ref: attrs.release_manifest_ref,
          result_ref: evidence_ref
        }

        {:ok, maybe_attach_github_pr_evidence(result, attrs)}

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
         request <- receipt_signal_request(signal, normalized),
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

    request =
      %{
        workflow_id: attrs.workflow_id,
        query_name: "operator_state.v1",
        tenant_ref: attrs.tenant_ref,
        resource_ref: attrs.resource_ref,
        trace_id: attrs.trace_id,
        release_manifest_ref: Map.get(attrs, :release_manifest_ref, @release_manifest_ref)
      }
      |> maybe_put_runtime_module(attrs, :workflow_runtime_impl)

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

  @doc "Maps source refresh drift into replay-safe workflow control decisions."
  @spec source_reconciliation_decision(map() | keyword()) ::
          {:cancel | :finalize | :continue, map()}
  def source_reconciliation_decision(attrs) do
    attrs = normalize(attrs)

    cond do
      Map.get(attrs, :source_visible?) == false ->
        {:cancel,
         source_reconciliation_fields(
           :source_missing,
           :cancel_lower_and_quarantine,
           "quarantine_subject",
           false
         )}

      Map.get(attrs, :source_active?) == false ->
        {:cancel,
         source_reconciliation_fields(
           :non_active_source,
           :cancel_lower_and_block,
           "block_subject",
           false
         )}

      Map.get(attrs, :assigned_to_current_worker?) == false ->
        {:cancel,
         source_reconciliation_fields(
           :source_reassigned,
           :cancel_lower_and_block,
           "block_subject",
           false
         )}

      source_reconciliation_terminal?(attrs) ->
        {:finalize,
         source_reconciliation_fields(
           :terminal_source,
           :terminal_cleanup,
           "complete_subject",
           true
         )}

      true ->
        {:continue, %{reason: :active_source, safe_action: :continue_workflow}}
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

  defp source_reconciliation_fields(reason, safe_action, projection_mutation, cleanup_required?) do
    %{
      reason: reason,
      safe_action: safe_action,
      workflow_signal: "operator.cancel",
      projection_mutation: projection_mutation,
      cleanup_required?: cleanup_required?
    }
  end

  defp source_reconciliation_terminal?(attrs) do
    Map.get(attrs, :source_terminal?) == true or
      Map.get(attrs, :source_terminal) == true or
      Map.get(attrs, :source_state) == "terminal"
  end

  defp max_turns_reached?(attrs) do
    case {Map.get(attrs, :turn_count), Map.get(attrs, :max_turns)} do
      {turn_count, max_turns} when is_integer(turn_count) and is_integer(max_turns) ->
        turn_count >= max_turns

      _other ->
        false
    end
  end

  defp receipt_signal_request(%WorkflowReceiptSignal{} = signal, attrs) do
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
    |> maybe_put_runtime_module(attrs, :workflow_runtime_impl)
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
      GovernedRuntimeConfig.module(
        attrs,
        :mezzanine_workflow_runtime,
        :receipt_reducer,
        receipt_reducer_module(),
        governed_default?: true
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
        required_evidence: get_in(attrs, [:routing_facts, :required_evidence]) || [],
        review_required?: get_in(attrs, [:routing_facts, :review_required]),
        actor_ref:
          Map.get(attrs, :actor_ref) ||
            get_in(attrs, [:routing_facts, :actor_ref]) ||
            %{"kind" => "system", "ref" => "execution_lifecycle_workflow"}
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
      provider_object_refs: payload_value(attrs, routing, :provider_object_refs) || [],
      evidence_artifact_refs: payload_value(attrs, routing, :evidence_artifact_refs) || [],
      artifact_refs: payload_value(attrs, routing, :artifact_refs) || [],
      token_totals: payload_value(attrs, routing, :token_totals),
      token_dedupe: payload_value(attrs, routing, :token_dedupe),
      rate_limit: payload_value(attrs, routing, :rate_limit),
      retry: payload_value(attrs, routing, :retry),
      retry_receipts: payload_value(attrs, routing, :retry_receipts) || [],
      runtime_events: payload_value(attrs, routing, :runtime_events) || [],
      aitrace: payload_value(attrs, routing, :aitrace),
      prompt_provenance: payload_value(attrs, routing, :prompt_provenance),
      semantic_failure: payload_value(attrs, routing, :semantic_failure),
      provider_account: payload_value(attrs, routing, :provider_account),
      credential: payload_value(attrs, routing, :credential),
      runtime_profile: runtime_profile_payload(attrs, routing),
      governed_lower_envelope: lower_envelope_payload(attrs, routing),
      authority_decision: authority_decision_payload(attrs, routing),
      connector_manifests: connector_manifest_payload(attrs, routing),
      capability_negotiations: capability_negotiation_payload(attrs, routing),
      incident_bundles: payload_value(attrs, routing, :incident_bundles) || [],
      acceptance: payload_value(attrs, routing, :acceptance),
      github_pr_evidence: payload_value(attrs, routing, :github_pr_evidence),
      source_publication: payload_value(attrs, routing, :source_publication),
      workpad_refs: payload_value(attrs, routing, :workpad_refs) || [],
      trace_id: attrs.trace_id,
      causation_id: attrs.correlation_id,
      idempotency_key: attrs.idempotency_key
    }
  end

  defp runtime_profile_payload(attrs, routing) do
    payload_value(attrs, routing, :runtime_profile) ||
      %{
        runtime_profile_ref: payload_value(attrs, routing, :runtime_profile_ref),
        runtime_profile_kind: payload_value(attrs, routing, :runtime_profile_kind)
      }
  end

  defp lower_envelope_payload(attrs, routing) do
    payload_value(attrs, routing, :governed_lower_envelope) ||
      payload_value(attrs, routing, :lower_envelope) ||
      %{
        lower_request_ref: payload_value(attrs, routing, :lower_request_ref),
        lower_runtime_kind: payload_value(attrs, routing, :lower_runtime_kind),
        capability_id:
          payload_value(attrs, routing, :capability_id) ||
            payload_value(attrs, routing, :capability),
        resource_scope_refs: payload_value(attrs, routing, :resource_scope_refs),
        policy_bundle_refs: payload_value(attrs, routing, :policy_bundle_refs),
        script_refs: payload_value(attrs, routing, :script_refs),
        package_refs: payload_value(attrs, routing, :package_refs),
        sandbox_profile_ref: payload_value(attrs, routing, :sandbox_profile_ref),
        attestation_requirement_ref: payload_value(attrs, routing, :attestation_requirement_ref),
        denial_refs: payload_value(attrs, routing, :denial_refs)
      }
  end

  defp authority_decision_payload(attrs, routing) do
    payload_value(attrs, routing, :authority_decision) ||
      %{
        authority_ref:
          payload_value(attrs, routing, :authority_ref) ||
            Map.get(attrs, :permission_decision_ref),
        authority_decision_hash:
          payload_value(attrs, routing, :authority_decision_hash) ||
            Map.get(attrs, :decision_hash)
      }
  end

  defp connector_manifest_payload(attrs, routing) do
    case payload_value(attrs, routing, :connector_manifests) do
      nil ->
        attrs
        |> payload_value(routing, :connector_manifest_refs)
        |> List.wrap()
        |> Enum.map(&%{connector_manifest_ref: &1})

      manifests ->
        manifests
    end
  end

  defp capability_negotiation_payload(attrs, routing) do
    case payload_value(attrs, routing, :capability_negotiations) do
      nil ->
        attrs
        |> payload_value(routing, :capability_negotiation_refs)
        |> List.wrap()
        |> Enum.map(&%{capability_negotiation_ref: &1})

      negotiations ->
        negotiations
    end
  end

  defp payload_value(attrs, routing, key) do
    string_key = Atom.to_string(key)

    Map.get(routing, key) ||
      Map.get(routing, string_key) ||
      Map.get(attrs, key) ||
      Map.get(attrs, string_key)
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
        %{
          expected_installation_revision: expected_revision,
          installation_revision: installation_revision
        }}}
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

  defp compile_citadel_submission(%RunIntent{} = run_intent, attrs, policy_packs, runtime_attrs) do
    bridge =
      GovernedRuntimeConfig.module(
        runtime_attrs,
        :mezzanine_workflow_runtime,
        :citadel_bridge,
        Mezzanine.CitadelBridge,
        governed_default?: true
      )

    case bridge.compile_submission(run_intent, attrs, policy_packs, []) do
      {:ok, %{rejection_classification: nil} = compiled} ->
        {:ok, compiled}

      {:ok, %{rejection_classification: rejection} = compiled} ->
        {:error, {:citadel_rejected, rejection, compiled}}

      {:error, reason} ->
        {:error, {:citadel_rejected, reason}}
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

  defp invoke_authorized_lower(invocation, attrs) do
    bridge =
      GovernedRuntimeConfig.module(
        attrs,
        :mezzanine_workflow_runtime,
        :integration_bridge,
        Mezzanine.IntegrationBridge,
        governed_default?: true
      )

    bridge.invoke_run_intent(invocation, lower_dispatch_opts(attrs))
  end

  defp lower_dispatch_opts(attrs) do
    routing = Map.get(attrs, :routing_facts, %{}) || %{}

    envelope_opts =
      [
        :acceptable_attestation,
        :action_id,
        :attempt_ref,
        :attestation_requirement_ref,
        :capability_id,
        :capability_negotiation_ref,
        :cedar_schema_hash,
        :cedar_schema_ref,
        :connector_manifest_hash,
        :connector_manifest_ref,
        :connector_manifest_state,
        :connector_ref,
        :declared_actions,
        :evidence_profile_ref,
        :filesystem_policy_ref,
        :idempotency_class,
        :input_hash,
        :input_ref,
        :lower_request_ref,
        :lower_runtime_kind,
        :network_policy_ref,
        :package_refs,
        :placement_ref,
        :policy_bundle_hash,
        :policy_bundle_ref,
        :policy_profile_ref,
        :redaction_profile_ref,
        :resource_scope_refs,
        :run_ref,
        :runtime_class,
        :runtime_profile_kind,
        :runtime_profile_ref,
        :sandbox_level,
        :sandbox_profile_ref,
        :script_api_version,
        :script_hash,
        :script_ref,
        :side_effect_class,
        :target_ref,
        :workflow_ref,
        :workspace_root,
        :cwd,
        :workspace_ref
      ]
      |> Enum.reduce([], fn key, opts ->
        case payload_value(attrs, routing, key) do
          nil -> opts
          value -> Keyword.put(opts, key, value)
        end
      end)

    envelope_opts
    |> Keyword.put_new(:capability_id, payload_value(attrs, routing, :capability))
    |> Keyword.put_new(:workspace_root, payload_value(attrs, routing, :workspace_root))
    |> Keyword.put_new(:cwd, routing_cwd(routing))
    |> Keyword.merge(normalize_keyword_opts(Map.get(attrs, :lower_dispatch_opts, [])))
    |> Enum.reject(fn {_key, value} -> is_nil(value) end)
  end

  defp routing_cwd(routing) do
    intent = payload_value(%{}, routing, :execution_intent) || %{}

    case payload_value(intent, %{}, :cwd) || payload_value(intent, %{}, :workspace_root) do
      value when is_binary(value) and value != "" -> value
      _other -> payload_value(%{}, routing, :workspace_root)
    end
  end

  defp normalize_keyword_opts(opts) when is_list(opts) do
    Enum.flat_map(opts, &normalize_keyword_pair/1)
  end

  defp normalize_keyword_opts(opts) when is_map(opts) do
    Enum.flat_map(opts, &normalize_keyword_pair/1)
  end

  defp normalize_keyword_opts(_opts), do: []

  defp normalize_keyword_pair({key, value}) when is_atom(key), do: [{key, value}]

  defp normalize_keyword_pair({key, value}) when is_binary(key) do
    case Map.fetch(@key_lookup, key) do
      {:ok, normalized_key} -> [{normalized_key, value}]
      :error -> []
    end
  end

  defp normalize_keyword_pair(_pair), do: []

  defp maybe_publish_source(base_result, attrs, source_publish_ref) do
    case source_publication_request(attrs, source_publish_ref) do
      nil -> {:ok, base_result}
      request -> dispatch_source_publication(base_result, attrs, request, source_publish_ref)
    end
  end

  defp dispatch_source_publication(base_result, attrs, request, source_publish_ref) do
    with {:ok, input} <- new_input(attrs),
         {:ok, invocation} <-
           authorized_lower_invocation(input, Map.get(attrs, :citadel_authority)),
         {:ok, publication} <- invoke_source_publication(invocation, request, attrs) do
      {:ok, source_publication_result(base_result, publication, source_publish_ref)}
    end
  end

  defp source_publication_result(base_result, publication, source_publish_ref) do
    base_result
    |> Map.put(:source_publication_receipt, Map.get(publication, :source_publication_receipt))
    |> Map.put(:source_publication_result, publication)
    |> Map.put(:result_ref, source_publication_receipt_ref(publication) || source_publish_ref)
  end

  defp source_publication_receipt_ref(publication) do
    get_in(publication, [:source_publication_receipt, :source_publication_receipt_ref])
  end

  defp maybe_attach_github_pr_evidence(result, attrs) do
    case Map.get(attrs, :github_pr_evidence) || Map.get(attrs, "github_pr_evidence") do
      %{} = evidence ->
        result
        |> Map.put(:evidence_kind, Map.get(evidence, :evidence_kind, "github_pr"))
        |> Map.put(:content_ref, Map.get(evidence, :content_ref))
        |> Map.put(:evidence_metadata, Map.get(evidence, :metadata, %{}))
        |> Map.put(:github_pr_evidence, evidence)
        |> Map.put(:result_ref, Map.get(evidence, :evidence_ref, result.result_ref))

      _other ->
        result
    end
  end

  defp invoke_source_publication(invocation, request, attrs) do
    bridge =
      GovernedRuntimeConfig.module(
        attrs,
        :mezzanine_workflow_runtime,
        :integration_bridge,
        Mezzanine.IntegrationBridge,
        governed_default?: true
      )

    bridge.publish_linear_source(invocation, request, [])
  end

  defp source_publication_request(attrs, source_publish_ref) do
    request = Map.get(attrs, :source_publication_request) || Map.get(attrs, :source_publication)

    case request do
      nil ->
        nil

      request when is_map(request) or is_list(request) ->
        request
        |> normalize()
        |> Map.put_new(:source_publish_ref, source_publish_ref)
        |> Map.put_new(:source_ref, Map.get(attrs, :resource_ref))
        |> Map.put_new(:trace_id, Map.get(attrs, :trace_id))
    end
  end

  defp maybe_put_runtime_module(request, attrs, key) do
    case runtime_module(attrs, key) do
      module when is_atom(module) -> Map.put(request, key, module)
      _other -> request
    end
  end

  defp runtime_module(attrs, key) do
    Map.get(attrs, key) ||
      get_in(attrs, [:runtime_modules, key]) ||
      get_in(attrs, [:workflow_runtime_modules, key])
  end

  defp build_authorized_invocation(attrs) do
    module = authorized_invocation_module()

    if Code.ensure_loaded?(module) and function_exported?(module, :new, 1) do
      :erlang.apply(module, :new, [attrs])
    else
      {:ok, Map.put(attrs, :authorized_invocation_boundary, module)}
    end
  end

  defp authorized_invocation_module, do: @authorized_invocation_module

  defp receipt_reducer_module, do: @receipt_reducer_module

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
      {key, value} when is_binary(key) -> {Map.get(@key_lookup, key, key), value}
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
      value when is_binary(value) -> Map.get(@routing_atom_lookup, value, default)
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
  defp normalize_key(key) when is_binary(key), do: Map.get(@key_lookup, key, key)

  defp missing_required(attrs, required), do: Enum.reject(required, &present?(Map.get(attrs, &1)))

  defp present?(value) when is_binary(value), do: String.trim(value) != ""
  defp present?(value), do: not is_nil(value)
end
