defmodule Mezzanine.M1M2Runtime.WorkflowLowerGateway do
  @moduledoc """
  Explicit lower-gateway implementation for workflow-backed dispatch claims.

  The execution ledger stays fail-closed unless a governed caller selects this
  module as `:lower_gateway_impl`. This adapter converts the execution claim
  into the workflow lifecycle input, lets Citadel compile authority, and then
  submits through the Mezzanine IntegrationBridge into Jido Integration.
  """

  @behaviour Mezzanine.LowerGateway

  alias Mezzanine.WorkflowRuntime.ExecutionLifecycleWorkflow

  @default_release_manifest_ref "phase4-v6-workflow-lower-gateway"
  @contracts_module :"Elixir.Jido.Integration.V2.Contracts"
  @denial_module :"Elixir.Jido.Integration.V2.GovernedLowerDenial"
  @envelope_module :"Elixir.Jido.Integration.V2.GovernedLowerEnvelope"
  @lower_facts_module :"Elixir.Jido.Integration.V2.LowerFacts"
  @receipt_module :"Elixir.Jido.Integration.V2.GovernedLowerReceipt"
  @submission_acceptance_module :"Elixir.Jido.Integration.V2.SubmissionAcceptance"
  @tenant_scope_module :"Elixir.Jido.Integration.V2.TenantScope"
  @dispatch_option_keys [
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
  @lifecycle_keys [
    :allowed_operations,
    :allowed_tools,
    :authority_decision_ref,
    :authority_packet_ref,
    :authority_ref,
    :boundary_class,
    :capability,
    :command_id,
    :command_receipt_ref,
    :downstream_scope,
    :environment_ref,
    :execution_intent,
    :execution_intent_family,
    :expected_installation_revision,
    :installation_revision,
    :lower_submission_ref,
    :permission_decision_ref,
    :policy_epoch,
    :policy_pack_id,
    :policy_refs,
    :policy_version,
    :project_ref,
    :requested_capability_ids,
    :resource_ref,
    :review_required,
    :risk_hints,
    :scope_kind,
    :substrate_trace_id,
    :target_id,
    :target_kind,
    :terminal_policy,
    :workflow_id,
    :workflow_input_ref,
    :workflow_run_id,
    :workflow_type,
    :workflow_version,
    :workspace_mutability,
    :workspace_root
  ]
  @normalizable_keys [
                       :actor_ref,
                       :binding_snapshot,
                       :causation_id,
                       :compiled_pack_revision,
                       :correlation_id,
                       :dispatch_envelope,
                       :execution_id,
                       :installation_id,
                       :lower_dispatch_opts,
                       :principal_ref,
                       :release_manifest_ref,
                       :runtime_modules,
                       :subject_id,
                       :submission_dedupe_key,
                       :system_actor_ref,
                       :tenant_id,
                       :trace_id
                     ] ++
                       @dispatch_option_keys ++ @lifecycle_keys
  @key_lookup Map.new(@normalizable_keys, &{Atom.to_string(&1), &1})

  @impl true
  def dispatch(claim) when is_map(claim) do
    claim = normalize(claim)

    with {:ok, attrs} <- lifecycle_attrs(claim),
         {:ok, authority} <- ExecutionLifecycleWorkflow.compile_citadel_authority_activity(attrs),
         {:ok, lower} <-
           attrs
           |> Map.put(:citadel_authority, authority)
           |> ExecutionLifecycleWorkflow.submit_jido_lower_run_activity() do
      {:accepted, accepted_dispatch(attrs, authority, lower)}
    else
      {:error, {:citadel_rejected, reason}} ->
        {:rejected, %{"reason" => inspect(reason), "owner_repo" => "citadel"}}

      {:error, {:citadel_rejected, reason, compiled}} ->
        {:rejected,
         %{
           "reason" => inspect(reason),
           "owner_repo" => "citadel",
           "compiled_submission_ref" => map_value(compiled, :compiled_submission_ref)
         }}

      {:error, reason} ->
        classify_dispatch_error(reason)
    end
  end

  @impl true
  def lookup_submission(submission_dedupe_key, tenant_id)
      when is_binary(submission_dedupe_key) and is_binary(tenant_id) do
    with {:ok, scope} <- tenant_scope(tenant_id),
         {:ok, lower_facts} <- loaded_module(@lower_facts_module, :fetch_submission_receipt, 2) do
      case lower_facts.fetch_submission_receipt(scope, submission_dedupe_key) do
        {:ok, acceptance} ->
          {:accepted,
           %{
             submission_ref: submission_acceptance_ref(acceptance),
             lower_receipt: submission_acceptance_ref(acceptance)
           }}

        {:error, :not_found} ->
          :never_seen

        {:error, reason} ->
          {:error, reason}
      end
    end
  end

  @impl true
  def fetch_execution_outcome(execution_lookup, tenant_id)
      when is_map(execution_lookup) and is_binary(tenant_id) do
    with {:ok, scope} <- tenant_scope(tenant_id, execution_lookup),
         {:ok, lower_facts} <- loaded_module(@lower_facts_module, :fetch_execution_outcome, 2) do
      case lower_facts.fetch_execution_outcome(scope, execution_lookup) do
        :pending -> :pending
        {:ok, outcome} -> {:ok, outcome}
        {:error, reason} -> {:error, reason}
      end
    end
  end

  @impl true
  def request_cancel(_submission_ref, _tenant_id, _reason), do: {:error, :cancel_not_supported}

  defp lifecycle_attrs(claim) do
    binding = normalize(Map.get(claim, :binding_snapshot, %{}))
    dispatch = normalize(Map.get(claim, :dispatch_envelope, %{}))

    attrs = %{
      tenant_ref: required_value(claim, :tenant_id),
      installation_ref: required_value(claim, :installation_id),
      workspace_ref: first_value([binding[:workspace_ref], dispatch[:workspace_ref]]),
      project_ref: first_value([dispatch[:project_ref], binding[:project_ref]]),
      environment_ref: first_value([dispatch[:environment_ref], binding[:environment_ref]]),
      principal_ref:
        first_value([
          dispatch[:principal_ref],
          dispatch[:actor_ref],
          claim[:principal_ref],
          claim[:actor_ref]
        ]),
      system_actor_ref: Map.get(claim, :system_actor_ref, "system://mezzanine/lower-gateway"),
      resource_ref:
        first_value([
          dispatch[:resource_ref],
          binding[:resource_ref],
          "work-object://#{required_value(claim, :subject_id)}"
        ]),
      subject_ref: required_value(claim, :subject_id),
      workflow_id: first_value([dispatch[:workflow_id], "workflow://#{claim.execution_id}"]),
      workflow_run_id: dispatch[:workflow_run_id],
      workflow_type: first_value([dispatch[:workflow_type], "execution_attempt"]),
      workflow_version: first_value([dispatch[:workflow_version], "execution-attempt.v1"]),
      command_id: first_value([dispatch[:command_id], "execution:#{claim.execution_id}"]),
      command_receipt_ref:
        first_value([
          dispatch[:command_receipt_ref],
          "command-receipt://mezzanine/execution/#{claim.execution_id}"
        ]),
      workflow_input_ref:
        first_value([dispatch[:workflow_input_ref], "workflow-input://#{claim.execution_id}"]),
      lower_submission_ref:
        first_value([
          dispatch[:lower_submission_ref],
          "lower-submission://#{claim.submission_dedupe_key}"
        ]),
      lower_idempotency_key: required_value(claim, :submission_dedupe_key),
      activity_call_ref: "activity-call://#{claim.execution_id}/submit-lower",
      authority_packet_ref: first_value([dispatch[:authority_packet_ref], claim[:authority_ref]]),
      permission_decision_ref:
        first_value([dispatch[:permission_decision_ref], dispatch[:authority_decision_ref]]),
      idempotency_key: required_value(claim, :submission_dedupe_key),
      trace_id: required_value(claim, :trace_id),
      correlation_id:
        first_value([claim[:correlation_id], claim[:causation_id], dispatch[:correlation_id]]),
      release_manifest_ref: Map.get(claim, :release_manifest_ref, @default_release_manifest_ref),
      retry_policy: retry_policy(binding, dispatch),
      terminal_policy: Map.get(dispatch, :terminal_policy, "quarantine_late_receipts"),
      routing_facts: routing_facts(claim, binding, dispatch),
      runtime_modules: Map.get(claim, :runtime_modules, %{}),
      lower_dispatch_opts: lower_dispatch_opts(claim, binding, dispatch)
    }

    case required_missing(attrs) do
      [] -> {:ok, attrs}
      missing -> {:error, {:missing_required_lower_gateway_fields, missing}}
    end
  end

  defp accepted_dispatch(attrs, authority, lower) do
    lower = normalize(lower)
    provider_submission = normalize(Map.get(lower, :provider_submission, %{}))
    lower_receipt = lower_receipt(provider_submission)
    lower_envelope = lower_envelope(provider_submission)

    %{
      owner_repo: :jido_integration,
      execution_plane_owner_repo: Map.get(lower, :execution_plane_owner_repo),
      authority: compact_authority(authority),
      activity_call_ref: Map.get(lower, :activity_call_ref),
      submission_ref: submission_ref(attrs, lower, provider_submission, lower_envelope),
      lower_receipt: lower_receipt,
      governed_lower_envelope: lower_envelope,
      provider_submission: dump_json_safe(provider_submission),
      trace_id: attrs.trace_id,
      routing_facts: attrs.routing_facts
    }
  end

  defp classify_dispatch_error(%{__struct__: @denial_module} = denial) do
    {:rejected, %{"denial" => dump_jido_contract(denial)}}
  end

  defp classify_dispatch_error(reason), do: {:error, reason}

  defp compact_authority(authority) do
    authority = normalize(authority)

    %{
      owner_repo: Map.get(authority, :owner_repo),
      activity_call_ref: Map.get(authority, :activity_call_ref),
      authority_packet_ref: Map.get(authority, :authority_packet_ref),
      permission_decision_ref: Map.get(authority, :permission_decision_ref),
      compiled_submission_ref: Map.get(authority, :compiled_submission_ref),
      citadel_decision_hash: Map.get(authority, :citadel_decision_hash),
      result_ref: Map.get(authority, :result_ref)
    }
  end

  defp submission_ref(attrs, lower, provider_submission, lower_envelope) do
    %{
      "lower_submission_ref" => Map.get(lower, :lower_submission_ref),
      "idempotency_key" => Map.get(lower, :idempotency_key),
      "lower_idempotency_key" => Map.get(lower, :lower_idempotency_key),
      "lower_request_ref" => map_value(lower_envelope, "lower_request_ref"),
      "run_id" => run_id(provider_submission),
      "attempt_id" => attempt_id(provider_submission),
      "ji_submission_key" => attrs.lower_idempotency_key
    }
    |> compact_map()
  end

  defp lower_receipt(%{governed_lower_receipt: receipt}), do: dump_jido_contract(receipt)
  defp lower_receipt(%{"governed_lower_receipt" => receipt}), do: dump_jido_contract(receipt)

  defp lower_receipt(provider_submission) do
    %{
      "receipt_id" => map_value(provider_submission, :receipt_id),
      "run_id" => run_id(provider_submission),
      "attempt_id" => attempt_id(provider_submission),
      "state" => "accepted",
      "terminal?" => false
    }
    |> compact_map()
  end

  defp lower_envelope(%{governed_lower_envelope: envelope}), do: dump_jido_contract(envelope)
  defp lower_envelope(%{"governed_lower_envelope" => envelope}), do: dump_jido_contract(envelope)
  defp lower_envelope(_provider_submission), do: %{}

  defp run_id(provider_submission) do
    first_value([
      map_value(provider_submission, :run_id),
      provider_submission |> map_value(:run) |> map_value(:run_id)
    ])
  end

  defp attempt_id(provider_submission) do
    first_value([
      map_value(provider_submission, :attempt_id),
      provider_submission |> map_value(:attempt) |> map_value(:attempt_id)
    ])
  end

  defp lower_dispatch_opts(claim, binding, dispatch) do
    envelope_opts =
      @dispatch_option_keys
      |> Enum.reduce([], fn key, opts ->
        case first_value([claim[key], dispatch[key], binding[key]]) do
          nil -> opts
          value -> Keyword.put(opts, key, value)
        end
      end)

    envelope_opts
    |> Keyword.put_new(:capability_id, capability(binding, dispatch))
    |> Keyword.put_new(:workspace_root, binding[:workspace_root] || dispatch[:workspace_root])
    |> Keyword.put_new(:cwd, execution_intent_cwd(binding, dispatch))
    |> Keyword.merge(normalize_keyword(Map.get(claim, :lower_dispatch_opts, [])))
    |> Enum.reject(fn {_key, value} -> is_nil(value) end)
  end

  defp routing_facts(claim, binding, dispatch) do
    capability = capability(binding, dispatch)

    %{
      review_required: truthy?(dispatch[:review_required]),
      installation_id: claim.installation_id,
      installation_revision: dispatch[:installation_revision],
      expected_installation_revision: dispatch[:expected_installation_revision],
      actor_ref:
        first_value([dispatch[:actor_ref], dispatch[:principal_ref], claim[:actor_ref], "system"]),
      subject_id: claim.subject_id,
      execution_id: claim.execution_id,
      capability: capability,
      allowed_operations: allowed_operations(binding, dispatch, capability),
      allowed_tools: List.wrap(dispatch[:allowed_tools]),
      substrate_trace_id: first_value([dispatch[:substrate_trace_id], claim.trace_id]),
      target_id: first_value([dispatch[:target_id], "workspace_runtime"]),
      service_id: first_value([dispatch[:service_id], "workspace_runtime"]),
      boundary_class: first_value([dispatch[:boundary_class], "workspace_session"]),
      target_kind: first_value([dispatch[:target_kind], "runtime_target"]),
      policy_refs: List.wrap(dispatch[:policy_refs]),
      policy_version: first_value([dispatch[:policy_version], "workflow-runtime-policy-v1"]),
      policy_epoch: dispatch[:policy_epoch] || 0,
      workspace_mutability: first_value([dispatch[:workspace_mutability], "read_write"]),
      downstream_scope: first_value([dispatch[:downstream_scope], "subject:#{claim.subject_id}"]),
      runtime_profile_ref: binding[:runtime_profile_ref] || dispatch[:runtime_profile_ref],
      runtime_profile_kind: binding[:runtime_profile_kind] || dispatch[:runtime_profile_kind],
      lower_runtime_kind: binding[:lower_runtime_kind] || dispatch[:lower_runtime_kind],
      resource_scope_refs:
        List.wrap(binding[:resource_scope_refs] || dispatch[:resource_scope_refs]),
      workspace_root: binding[:workspace_root] || dispatch[:workspace_root],
      execution_intent: execution_intent(claim, binding, dispatch, capability)
    }
    |> compact_map()
  end

  defp execution_intent(claim, binding, dispatch, capability) do
    case dispatch[:execution_intent] || binding[:execution_intent] do
      %{} = intent ->
        intent

      _other ->
        %{
          "command" => capability,
          "subject_id" => claim.subject_id,
          "execution_id" => claim.execution_id,
          "trace_id" => claim.trace_id
        }
    end
  end

  defp execution_intent_cwd(binding, dispatch) do
    intent = dispatch[:execution_intent] || binding[:execution_intent] || %{}

    case map_value(intent, :cwd) || map_value(intent, :workspace_root) do
      value when is_binary(value) and value != "" -> value
      _other -> binding[:workspace_root] || dispatch[:workspace_root]
    end
  end

  defp allowed_operations(binding, dispatch, capability) do
    [
      dispatch[:allowed_operations],
      dispatch[:requested_capability_ids],
      binding[:requested_capability_ids],
      capability
    ]
    |> Enum.flat_map(&List.wrap/1)
    |> Enum.reject(&is_nil/1)
    |> Enum.uniq()
  end

  defp capability(binding, dispatch) do
    first_value([
      dispatch[:capability],
      binding[:capability],
      List.first(List.wrap(dispatch[:requested_capability_ids])),
      List.first(List.wrap(binding[:requested_capability_ids])),
      "codex.session.turn"
    ])
  end

  defp retry_policy(binding, dispatch) do
    case dispatch[:retry_policy] || binding[:retry_policy] do
      %{} = retry_policy -> retry_policy
      _other -> %{max_attempts: 3}
    end
  end

  defp required_missing(attrs) do
    [
      :tenant_ref,
      :installation_ref,
      :principal_ref,
      :resource_ref,
      :subject_ref,
      :workflow_id,
      :workflow_type,
      :workflow_version,
      :command_id,
      :command_receipt_ref,
      :workflow_input_ref,
      :lower_submission_ref,
      :lower_idempotency_key,
      :activity_call_ref,
      :authority_packet_ref,
      :permission_decision_ref,
      :idempotency_key,
      :trace_id,
      :correlation_id,
      :release_manifest_ref,
      :retry_policy,
      :terminal_policy,
      :routing_facts
    ]
    |> Enum.reject(&present?(Map.get(attrs, &1)))
  end

  defp submission_acceptance_ref(acceptance) do
    case loaded_module(@submission_acceptance_module, :dump, 1) do
      {:ok, acceptance_module} ->
        acceptance
        |> acceptance_module.dump()
        |> dump_json_safe()

      {:error, _reason} ->
        dump_json_safe(acceptance)
    end
  end

  defp tenant_scope(tenant_id, attrs \\ %{}) do
    with {:ok, tenant_scope_module} <- loaded_module(@tenant_scope_module, :new, 1) do
      tenant_scope_module.new(%{
        tenant_id: tenant_id,
        installation_id: map_value(attrs, :installation_id),
        actor_ref: map_value(attrs, :actor_ref),
        trace_id: map_value(attrs, :trace_id),
        authorized_at: DateTime.utc_now()
      })
    end
  end

  defp dump_jido_contract(%{__struct__: module} = struct)
       when module in [@receipt_module, @envelope_module, @denial_module] do
    if function_exported?(module, :to_map, 1) do
      module.to_map(struct)
    else
      struct |> Map.from_struct() |> dump_jido_contract()
    end
  end

  defp dump_jido_contract(%_{} = struct), do: struct |> Map.from_struct() |> dump_jido_contract()
  defp dump_jido_contract(map) when is_map(map), do: dump_json_safe(map)
  defp dump_jido_contract(value), do: value

  defp dump_json_safe(value) do
    if Code.ensure_loaded?(@contracts_module) and
         function_exported?(@contracts_module, :dump_json_safe!, 1) do
      @contracts_module.dump_json_safe!(value)
    else
      fallback_json_safe(value)
    end
  end

  defp fallback_json_safe(%DateTime{} = value), do: DateTime.to_iso8601(value)
  defp fallback_json_safe(%NaiveDateTime{} = value), do: NaiveDateTime.to_iso8601(value)
  defp fallback_json_safe(%Date{} = value), do: Date.to_iso8601(value)
  defp fallback_json_safe(%Time{} = value), do: Time.to_iso8601(value)
  defp fallback_json_safe(%_{} = value), do: value |> Map.from_struct() |> fallback_json_safe()

  defp fallback_json_safe(map) when is_map(map) do
    Map.new(map, fn {key, value} -> {to_string(key), fallback_json_safe(value)} end)
  end

  defp fallback_json_safe(values) when is_list(values),
    do: Enum.map(values, &fallback_json_safe/1)

  defp fallback_json_safe(value) when is_atom(value), do: Atom.to_string(value)
  defp fallback_json_safe(value), do: value

  defp loaded_module(module, function, arity) do
    if Code.ensure_loaded?(module) and function_exported?(module, function, arity) do
      {:ok, module}
    else
      {:error, {:module_unavailable, module, function, arity}}
    end
  end

  defp normalize(nil), do: %{}
  defp normalize(attrs) when is_list(attrs), do: attrs |> Map.new() |> normalize()
  defp normalize(%_{} = attrs), do: attrs |> Map.from_struct() |> normalize()

  defp normalize(attrs) when is_map(attrs) do
    Map.new(attrs, fn {key, value} -> {normalize_key(key), value} end)
  end

  defp normalize_key(key) when is_atom(key), do: key
  defp normalize_key(key) when is_binary(key), do: Map.get(@key_lookup, key, key)
  defp normalize_key(key), do: key

  defp normalize_keyword(opts) when is_list(opts) do
    Enum.flat_map(opts, &normalize_keyword_pair/1)
  end

  defp normalize_keyword(opts) when is_map(opts) do
    Enum.flat_map(opts, &normalize_keyword_pair/1)
  end

  defp normalize_keyword(_opts), do: []

  defp normalize_keyword_pair({key, value}) when is_atom(key), do: [{key, value}]

  defp normalize_keyword_pair({key, value}) when is_binary(key) do
    case Map.fetch(@key_lookup, key) do
      {:ok, normalized_key} -> [{normalized_key, value}]
      :error -> []
    end
  end

  defp normalize_keyword_pair(_pair), do: []

  defp required_value(map, key) do
    case map_value(map, key) do
      value when is_binary(value) and value != "" -> value
      value when is_integer(value) -> Integer.to_string(value)
      value -> value
    end
  end

  defp map_value(nil, _key), do: nil
  defp map_value(%_{} = struct, key), do: struct |> Map.from_struct() |> map_value(key)

  defp map_value(map, key) when is_map(map) do
    Map.get(map, key) || Map.get(map, Atom.to_string(key))
  end

  defp map_value(_value, _key), do: nil

  defp first_value(values), do: Enum.find_value(values, &present_value/1)

  defp present_value(value) when value in [nil, "", []], do: nil
  defp present_value(%{} = value) when map_size(value) == 0, do: nil
  defp present_value(value), do: value

  defp present?(value), do: not is_nil(present_value(value))

  defp truthy?(value) when value in [true, "true", true, 1], do: true
  defp truthy?(_value), do: false

  defp compact_map(map) do
    map
    |> Enum.reject(fn {_key, value} -> is_nil(present_value(value)) end)
    |> Map.new()
  end
end
