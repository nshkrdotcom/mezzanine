defmodule Mezzanine.IntegrationBridge.AuthorizedInvocation do
  @moduledoc """
  Post-Citadel lower invocation envelope.

  This struct is the physical boundary between Mezzanine governance and Jido
  Integration provider effects. Dispatchers must receive this struct, not a
  generic map, `RunIntent`, or `EffectIntent`.
  """

  alias Jido.Integration.V2.GovernedLowerDenial
  alias Jido.Integration.V2.GovernedLowerEnvelope
  alias Jido.Integration.V2.GovernedLowerReceipt

  @typedoc "Raw `Citadel.InvocationRequest.V2` struct or dumped map."
  @type invocation_request :: map() | struct()

  @type t :: %__MODULE__{
          tenant_id: String.t(),
          installation_id: String.t(),
          subject_id: String.t(),
          execution_id: String.t(),
          trace_id: String.t(),
          idempotency_key: String.t(),
          submission_dedupe_key: String.t(),
          invocation_request: invocation_request(),
          action_ref: String.t() | nil
        }

  @required_fields [
    :tenant_id,
    :installation_id,
    :subject_id,
    :execution_id,
    :trace_id,
    :idempotency_key,
    :submission_dedupe_key,
    :invocation_request
  ]
  @optional_fields [:action_ref]
  defstruct @required_fields ++ @optional_fields

  @invocation_request_module :"Elixir.Citadel.InvocationRequest.V2"
  @sandbox_rank %{strict: 0, standard: 1, none: 2}
  @known_atomish_values %{
    "active" => :active,
    "stale" => :stale,
    "refresh_required" => :refresh_required,
    "invalid" => :invalid,
    "quarantined" => :quarantined,
    "strict" => :strict,
    "standard" => :standard,
    "none" => :none
  }
  @dialyzer :no_match

  @spec new(map() | keyword() | t()) :: {:ok, t()} | {:error, Exception.t()}
  def new(attrs) do
    {:ok, new!(attrs)}
  rescue
    error in [ArgumentError, KeyError] -> {:error, error}
  end

  @spec new!(map() | keyword() | t()) :: t()
  def new!(%__MODULE__{} = invocation), do: validate!(invocation)

  def new!(attrs) when is_list(attrs) do
    attrs
    |> Enum.into(%{})
    |> new!()
  end

  def new!(attrs) when is_map(attrs) do
    invocation =
      %__MODULE__{
        tenant_id: required_string!(attrs, :tenant_id),
        installation_id: required_string!(attrs, :installation_id),
        subject_id: required_string!(attrs, :subject_id),
        execution_id: required_string!(attrs, :execution_id),
        trace_id: required_string!(attrs, :trace_id),
        idempotency_key: required_string!(attrs, :idempotency_key),
        submission_dedupe_key: required_string!(attrs, :submission_dedupe_key),
        invocation_request: required!(attrs, :invocation_request),
        action_ref: optional_string!(attrs, :action_ref)
      }

    :ok = validate_expected_installation_revision!(attrs, invocation.invocation_request)

    validate!(invocation)
  end

  @spec default_capability!(t()) :: String.t()
  def default_capability!(%__MODULE__{} = invocation) do
    invocation.invocation_request
    |> normalized_request!()
    |> required!(:allowed_operations)
    |> case do
      [capability | _rest] when is_binary(capability) and capability != "" ->
        capability

      other ->
        raise ArgumentError,
              "AuthorizedInvocation.invocation_request.allowed_operations must include at least one capability, got: #{inspect(other)}"
    end
  end

  @spec authorize_capability!(t(), String.t()) :: :ok
  def authorize_capability!(%__MODULE__{} = invocation, capability_id)
      when is_binary(capability_id) and capability_id != "" do
    allowed_operations =
      invocation.invocation_request
      |> normalized_request!()
      |> required!(:allowed_operations)

    if capability_id in allowed_operations do
      :ok
    else
      raise ArgumentError,
            "AuthorizedInvocation capability #{inspect(capability_id)} is not present in Citadel authority allowed_operations"
    end
  end

  def authorize_capability!(%__MODULE__{}, capability_id) do
    raise ArgumentError,
          "AuthorizedInvocation capability must be a non-empty string, got: #{inspect(capability_id)}"
  end

  @spec invoke_input(t(), String.t()) :: map()
  def invoke_input(%__MODULE__{} = invocation, capability_id) do
    request = normalized_request!(invocation.invocation_request)
    authority_packet = required!(request, :authority_packet)
    execution_governance = required!(request, :execution_governance)

    %{
      tenant_id: invocation.tenant_id,
      installation_id: invocation.installation_id,
      subject_id: invocation.subject_id,
      execution_id: invocation.execution_id,
      trace_id: invocation.trace_id,
      idempotency_key: invocation.idempotency_key,
      submission_dedupe_key: invocation.submission_dedupe_key,
      capability_id: capability_id,
      invocation_request: invocation.invocation_request,
      authority: %{
        authority_packet_ref: authority_packet_ref(authority_packet),
        permission_decision_ref: required_string!(authority_packet, :decision_id),
        policy_version: required_string!(authority_packet, :policy_version),
        allowed_operations: required!(request, :allowed_operations),
        execution_governance_id: required_string!(execution_governance, :execution_governance_id),
        for_action_ref: invocation.action_ref || authority_for_action_ref(authority_packet)
      }
    }
    |> Map.merge(
      codex_turn_input_attrs(request, authority_packet, execution_governance, capability_id)
    )
  end

  @spec governed_lower_envelope(t(), String.t(), keyword()) ::
          {:ok, GovernedLowerEnvelope.t()} | {:error, GovernedLowerDenial.t() | Exception.t()}
  def governed_lower_envelope(%__MODULE__{} = invocation, capability_id, opts \\ [])
      when is_binary(capability_id) and is_list(opts) do
    request = normalized_request!(invocation.invocation_request)
    authority_packet = required!(request, :authority_packet)
    execution_governance = required!(request, :execution_governance)
    execution_envelope = execution_envelope(request)

    attrs =
      lower_envelope_attrs(
        invocation,
        capability_id,
        request,
        authority_packet,
        execution_governance,
        execution_envelope,
        opts
      )

    with :ok <- validate_authorized_capability(attrs),
         :ok <- validate_connector_ref(attrs),
         :ok <- validate_connector_manifest(attrs),
         :ok <- validate_resource_scope_refs(attrs),
         :ok <- validate_sandbox_posture(attrs, execution_governance, opts),
         :ok <- validate_attestation_posture(attrs, execution_governance, opts) do
      GovernedLowerEnvelope.new(attrs)
    else
      {:error, %GovernedLowerDenial{} = denial} ->
        {:error, denial}
    end
  rescue
    error in [ArgumentError, KeyError] -> {:error, error}
  end

  @spec governed_lower_envelope!(t(), String.t(), keyword()) :: GovernedLowerEnvelope.t()
  def governed_lower_envelope!(%__MODULE__{} = invocation, capability_id, opts \\ []) do
    case governed_lower_envelope(invocation, capability_id, opts) do
      {:ok, %GovernedLowerEnvelope{} = envelope} ->
        envelope

      {:error, %GovernedLowerDenial{} = denial} ->
        raise ArgumentError, GovernedLowerDenial.to_map(denial) |> inspect()

      {:error, %ArgumentError{} = error} ->
        raise error
    end
  end

  @spec governed_lower_receipt(GovernedLowerEnvelope.t(), :succeeded | :failed, map()) ::
          {:ok, GovernedLowerReceipt.t()} | {:error, Exception.t()}
  def governed_lower_receipt(%GovernedLowerEnvelope{} = envelope, status, dispatch_result)
      when status in [:succeeded, :failed] and is_map(dispatch_result) do
    GovernedLowerReceipt.new(%{
      lower_receipt_ref:
        "lower-receipt://#{URI.encode_www_form(envelope.lower_request_ref)}/#{status}",
      lower_request_ref: envelope.lower_request_ref,
      lower_runtime_kind: envelope.lower_runtime_kind,
      runtime_profile_ref: envelope.runtime_profile_ref,
      runtime_profile_kind: envelope.runtime_profile_kind,
      status: status,
      tenant_ref: envelope.tenant_ref,
      subject_ref: envelope.subject_ref,
      run_ref: envelope.run_ref,
      workflow_ref: envelope.workflow_ref,
      attempt_ref: envelope.attempt_ref,
      trace_id: envelope.trace_id,
      idempotency_key: envelope.idempotency_key,
      authority_ref: envelope.authority_ref,
      authority_decision_hash: envelope.authority_decision_hash,
      allowed_operations: envelope.allowed_operations,
      capability_id: envelope.capability_id,
      action_id: envelope.action_id,
      connector_ref: envelope.connector_ref,
      connector_manifest_ref: envelope.connector_manifest_ref,
      connector_manifest_hash: envelope.connector_manifest_hash,
      connector_manifest_state: envelope.connector_manifest_state,
      capability_negotiation_ref: envelope.capability_negotiation_ref,
      policy_profile_ref: envelope.policy_profile_ref,
      policy_bundle_ref: envelope.policy_bundle_ref,
      policy_bundle_hash: envelope.policy_bundle_hash,
      cedar_schema_ref: envelope.cedar_schema_ref,
      cedar_schema_hash: envelope.cedar_schema_hash,
      script_ref: envelope.script_ref,
      script_hash: envelope.script_hash,
      script_api_version: envelope.script_api_version,
      declared_actions: envelope.declared_actions,
      package_refs: envelope.package_refs,
      resource_scope_refs: envelope.resource_scope_refs,
      workspace_ref: envelope.workspace_ref,
      target_ref: envelope.target_ref,
      placement_ref: envelope.placement_ref,
      sandbox_profile_ref: envelope.sandbox_profile_ref,
      sandbox_level: envelope.sandbox_level,
      network_policy_ref: envelope.network_policy_ref,
      filesystem_policy_ref: envelope.filesystem_policy_ref,
      acceptable_attestation: envelope.acceptable_attestation,
      attestation_requirement_ref: envelope.attestation_requirement_ref,
      evidence_profile_ref: envelope.evidence_profile_ref,
      redaction_profile_ref: envelope.redaction_profile_ref,
      input_ref: envelope.input_ref,
      input_hash: envelope.input_hash,
      artifact_refs: artifact_refs(dispatch_result),
      event_refs: event_refs(dispatch_result),
      observed_at: DateTime.utc_now() |> DateTime.to_iso8601(),
      extensions:
        Map.merge(envelope.extensions || %{}, %{
          "mezzanine" =>
            %{
              "dispatch_status" => Atom.to_string(status),
              "jido_run_ref" => lower_run_ref(dispatch_result),
              "jido_attempt_ref" => lower_attempt_ref(dispatch_result)
            }
            |> Map.merge(nested_receipt_refs(dispatch_result))
        })
    })
  end

  @spec governed_lower_receipt!(GovernedLowerEnvelope.t(), :succeeded | :failed, map()) ::
          GovernedLowerReceipt.t()
  def governed_lower_receipt!(%GovernedLowerEnvelope{} = envelope, status, dispatch_result) do
    case governed_lower_receipt(envelope, status, dispatch_result) do
      {:ok, %GovernedLowerReceipt{} = receipt} -> receipt
      {:error, %ArgumentError{} = error} -> raise error
    end
  end

  @spec governed_lower_denial(GovernedLowerEnvelope.t(), atom(), String.t()) ::
          GovernedLowerDenial.t()
  def governed_lower_denial(%GovernedLowerEnvelope{} = envelope, denial_class, reason)
      when is_atom(denial_class) and is_binary(reason) do
    GovernedLowerDenial.new!(%{
      lower_denial_ref:
        "lower-denial://#{URI.encode_www_form(envelope.lower_request_ref)}/#{denial_class}",
      lower_request_ref: envelope.lower_request_ref,
      lower_runtime_kind: envelope.lower_runtime_kind,
      denial_class: denial_class,
      reason: reason,
      tenant_ref: envelope.tenant_ref,
      subject_ref: envelope.subject_ref,
      run_ref: envelope.run_ref,
      workflow_ref: envelope.workflow_ref,
      attempt_ref: envelope.attempt_ref,
      trace_id: envelope.trace_id,
      authority_ref: envelope.authority_ref,
      authority_decision_hash: envelope.authority_decision_hash,
      capability_id: envelope.capability_id,
      action_id: envelope.action_id,
      connector_manifest_ref: envelope.connector_manifest_ref,
      connector_manifest_hash: envelope.connector_manifest_hash,
      capability_negotiation_ref: envelope.capability_negotiation_ref,
      policy_bundle_ref: envelope.policy_bundle_ref,
      cedar_schema_ref: envelope.cedar_schema_ref,
      script_ref: envelope.script_ref,
      resource_scope_refs: envelope.resource_scope_refs,
      sandbox_profile_ref: envelope.sandbox_profile_ref,
      extensions: %{"mezzanine" => %{"stage" => "authorized_invocation"}}
    })
  end

  defp lower_envelope_attrs(
         %__MODULE__{} = invocation,
         capability_id,
         request,
         authority_packet,
         execution_governance,
         execution_envelope,
         opts
       ) do
    context =
      lower_envelope_context(
        invocation,
        capability_id,
        request,
        authority_packet,
        execution_governance,
        execution_envelope,
        opts
      )

    %{
      lower_request_ref: context.lower_request_ref,
      lower_runtime_kind: context.lower_runtime_kind,
      capability_id: capability_id,
      action_id: Keyword.get(opts, :action_id, capability_id),
      tenant_ref: invocation.tenant_id,
      subject_ref: invocation.subject_id,
      trace_id: invocation.trace_id,
      idempotency_key: invocation.idempotency_key
    }
    |> Map.merge(runtime_profile_attrs(context))
    |> Map.merge(authority_attrs(context))
    |> Map.merge(connector_attrs(context))
    |> Map.merge(policy_script_attrs(context))
    |> Map.merge(scope_attrs(context))
    |> Map.merge(sandbox_attrs(context))
    |> Map.merge(evidence_attrs(context))
    |> Map.merge(extension_attrs(context))
  end

  defp codex_turn_input_attrs(request, authority_packet, execution_governance, capability_id) do
    execution_intent = execution_intent(request)

    if execution_intent == %{} do
      %{}
    else
      dynamic_tool_manifest = optional_map(execution_intent, "dynamic_tool_manifest")
      memory_context = execution_intent |> optional_map("memory_context") |> public_ref_map()

      provider_metadata =
        execution_intent
        |> optional_map("provider_metadata")
        |> maybe_put_non_empty("dynamic_tool_manifest", dynamic_tool_manifest)
        |> maybe_put_non_empty("memory_context", memory_context)

      %{}
      |> maybe_put_non_empty(:prompt, string_value(execution_intent, "prompt"))
      |> maybe_put_non_empty(:cwd, codex_cwd(execution_intent, execution_governance))
      |> maybe_put_non_empty(:workspace, optional_map(execution_intent, "workspace"))
      |> maybe_put_non_empty(:host_tools, optional_list(execution_intent, "host_tools"))
      |> maybe_put_non_empty(:continuation, optional_map(execution_intent, "continuation"))
      |> maybe_put_non_empty(:provider_metadata, provider_metadata)
      |> maybe_put_non_empty(:dynamic_tool_manifest, dynamic_tool_manifest)
      |> maybe_put_non_empty(
        :authority_metadata,
        authority_metadata(request, authority_packet, execution_governance, capability_id)
      )
    end
  end

  defp execution_intent(request) do
    request
    |> optional(:extensions)
    |> citadel_extension()
    |> optional("execution_intent")
    |> case do
      %{} = intent -> intent
      _other -> %{}
    end
  end

  defp codex_cwd(execution_intent, execution_governance) do
    string_value(execution_intent, "cwd") ||
      string_value(execution_intent, "workspace_root") ||
      execution_governance
      |> optional(:sandbox)
      |> string_value("file_scope_hint")
  end

  defp authority_metadata(request, authority_packet, execution_governance, capability_id) do
    governance_operations = optional(execution_governance, :operations) || %{}
    governance_authority_ref = optional(execution_governance, :authority_ref) || %{}

    %{
      "authority_ref" => authority_packet_ref(authority_packet),
      "authority_decision_hash" =>
        authority_decision_hash(%{
          authority_packet: authority_packet,
          governance_authority_ref: governance_authority_ref
        }),
      "permission_decision_ref" => required_string!(authority_packet, :decision_id),
      "policy_version" => required_string!(authority_packet, :policy_version),
      "execution_governance_id" =>
        required_string!(execution_governance, :execution_governance_id),
      "capability_id" => capability_id,
      "allowed_operations" =>
        string_list_value(
          governance_operations,
          "allowed_operations",
          required!(request, :allowed_operations)
        )
    }
  end

  defp optional_map(%{} = map, key) do
    case optional(map, key) do
      %{} = value -> value
      _other -> %{}
    end
  end

  defp optional_map(_map, _key), do: %{}

  defp optional_list(%{} = map, key) do
    case optional(map, key) do
      value when is_list(value) -> value
      _other -> []
    end
  end

  defp optional_list(_map, _key), do: []

  defp public_ref_map(%{} = map) do
    map
    |> Enum.reject(fn {key, _value} -> private_payload_key?(key) end)
    |> Map.new(fn {key, value} -> {to_string(key), public_ref_value(value)} end)
  end

  defp public_ref_map(_value), do: %{}

  defp public_ref_value(%{} = map), do: public_ref_map(map)
  defp public_ref_value(values) when is_list(values), do: Enum.map(values, &public_ref_value/1)
  defp public_ref_value(value), do: value

  defp private_payload_key?(key) when is_atom(key), do: private_payload_key?(Atom.to_string(key))

  defp private_payload_key?(key) when is_binary(key) do
    key in [
      "api_key",
      "authorization",
      "body",
      "provider_payload",
      "raw_body",
      "raw_content",
      "raw_payload",
      "raw_prompt",
      "raw_provider_payload",
      "secret",
      "token"
    ]
  end

  defp private_payload_key?(_key), do: false

  defp maybe_put_non_empty(map, _key, nil), do: map
  defp maybe_put_non_empty(map, _key, []), do: map
  defp maybe_put_non_empty(map, _key, value) when value == %{}, do: map
  defp maybe_put_non_empty(map, key, value), do: Map.put(map, key, value)

  defp lower_envelope_context(
         invocation,
         capability_id,
         request,
         authority_packet,
         execution_governance,
         execution_envelope,
         opts
       ) do
    governance_sandbox = optional(execution_governance, :sandbox) || %{}

    context = %{
      invocation: invocation,
      capability_id: capability_id,
      request: request,
      authority_packet: authority_packet,
      execution_governance: execution_governance,
      execution_envelope: execution_envelope,
      opts: opts,
      governance_sandbox: governance_sandbox,
      governance_workspace: optional(execution_governance, :workspace) || %{},
      governance_placement: optional(execution_governance, :placement) || %{},
      governance_operations: optional(execution_governance, :operations) || %{},
      governance_authority_ref: optional(execution_governance, :authority_ref) || %{},
      request_extensions: optional(request, :extensions) || %{},
      tre_policy:
        tre_policy(authority_packet, execution_governance, optional(request, :extensions) || %{})
    }

    Map.merge(context, lower_envelope_defaults(context))
  end

  defp lower_envelope_defaults(context) do
    opts = context.opts
    invocation = context.invocation
    capability_id = context.capability_id
    execution_governance = context.execution_governance
    execution_envelope = context.execution_envelope
    governance_sandbox = context.governance_sandbox

    %{
      lower_runtime_kind:
        Keyword.get(opts, :lower_runtime_kind) ||
          normalize_atomish(string_value(execution_envelope, "lower_runtime_kind")) ||
          :direct_connector,
      connector_ref:
        Keyword.get(opts, :connector_ref) ||
          string_value(execution_envelope, "connector_ref") ||
          string_value(execution_governance, "connector_ref"),
      lower_request_ref:
        Keyword.get(opts, :lower_request_ref, lower_request_ref(invocation, capability_id)),
      resource_scope_refs:
        Keyword.get(
          opts,
          :resource_scope_refs,
          resource_scope_refs(invocation, execution_governance)
        ),
      sandbox_level: Keyword.get(opts, :sandbox_level, sandbox_level(governance_sandbox)),
      acceptable_attestation:
        Keyword.get(
          opts,
          :acceptable_attestation,
          list_value(governance_sandbox, "acceptable_attestation", [])
        )
    }
  end

  defp runtime_profile_attrs(context) do
    opts = context.opts
    execution_envelope = context.execution_envelope
    invocation = context.invocation

    %{
      runtime_profile_ref:
        Keyword.get(
          opts,
          :runtime_profile_ref,
          default_runtime_profile_ref(execution_envelope, invocation)
        ),
      runtime_profile_kind:
        Keyword.get(opts, :runtime_profile_kind, default_runtime_profile_kind(execution_envelope)),
      run_ref: Keyword.get(opts, :run_ref, invocation.execution_id),
      workflow_ref:
        Keyword.get(opts, :workflow_ref, string_value(execution_envelope, "workflow_id")),
      attempt_ref:
        Keyword.get(opts, :attempt_ref, string_value(execution_envelope, "attempt_ref"))
    }
  end

  defp default_runtime_profile_ref(execution_envelope, invocation) do
    string_value(execution_envelope, "runtime_profile_ref") ||
      "runtime-profile://local/#{invocation.installation_id}"
  end

  defp default_runtime_profile_kind(execution_envelope) do
    string_value(execution_envelope, "runtime_profile_kind") || :temporal_local
  end

  defp authority_attrs(context) do
    %{
      authority_ref: authority_packet_ref(context.authority_packet),
      authority_decision_hash: authority_decision_hash(context),
      allowed_operations:
        string_list_value(
          context.governance_operations,
          "allowed_operations",
          required!(context.request, :allowed_operations)
        )
    }
  end

  defp authority_decision_hash(context) do
    string_value(context.authority_packet, :decision_hash) ||
      string_value(context.governance_authority_ref, "decision_hash") ||
      authority_packet_ref(context.authority_packet)
  end

  defp connector_attrs(context) do
    opts = context.opts
    capability_id = context.capability_id
    connector_ref = context.connector_ref

    %{
      connector_ref: connector_ref,
      connector_manifest_ref:
        Keyword.get(opts, :connector_manifest_ref, default_connector_manifest_ref(connector_ref)),
      connector_manifest_hash:
        Keyword.get(
          opts,
          :connector_manifest_hash,
          default_connector_manifest_hash(connector_ref, capability_id)
        ),
      connector_manifest_state: Keyword.get(opts, :connector_manifest_state, :active),
      capability_negotiation_ref:
        Keyword.get(opts, :capability_negotiation_ref, "cap-neg://#{context.lower_request_ref}"),
      side_effect_class:
        Keyword.get(opts, :side_effect_class, infer_side_effect_class(capability_id)),
      idempotency_class:
        Keyword.get(opts, :idempotency_class, infer_idempotency_class(capability_id)),
      runtime_class:
        Keyword.get(opts, :runtime_class, infer_runtime_class(context.lower_runtime_kind))
    }
  end

  defp policy_script_attrs(context) do
    opts = context.opts
    tre_policy = context.tre_policy

    %{
      policy_profile_ref:
        Keyword.get(
          opts,
          :policy_profile_ref,
          string_value(tre_policy, "policy_profile_ref") ||
            policy_profile_ref(context.authority_packet, context.request_extensions)
        ),
      policy_bundle_ref:
        Keyword.get(opts, :policy_bundle_ref, string_value(tre_policy, "policy_bundle_ref")),
      policy_bundle_hash:
        Keyword.get(opts, :policy_bundle_hash, string_value(tre_policy, "policy_bundle_hash")),
      cedar_schema_ref:
        Keyword.get(opts, :cedar_schema_ref, string_value(tre_policy, "cedar_schema_ref")),
      cedar_schema_hash:
        Keyword.get(opts, :cedar_schema_hash, string_value(tre_policy, "cedar_schema_hash")),
      script_ref: Keyword.get(opts, :script_ref),
      script_hash: Keyword.get(opts, :script_hash),
      script_api_version: Keyword.get(opts, :script_api_version),
      declared_actions:
        Keyword.get(opts, :declared_actions, list_value(tre_policy, "allowed_actions", [])),
      package_refs: Keyword.get(opts, :package_refs, [])
    }
  end

  defp scope_attrs(context) do
    opts = context.opts

    %{
      resource_scope_refs: context.resource_scope_refs,
      workspace_ref: Keyword.get(opts, :workspace_ref, default_workspace_ref(context)),
      target_ref: Keyword.get(opts, :target_ref, required_string!(context.request, :target_id)),
      placement_ref: Keyword.get(opts, :placement_ref, default_placement_ref(context))
    }
  end

  defp default_workspace_ref(context) do
    string_value(context.governance_workspace, "logical_workspace_ref") ||
      List.first(context.resource_scope_refs)
  end

  defp default_placement_ref(context) do
    string_value(context.governance_placement, "node_affinity") ||
      required_string!(context.request, :target_id)
  end

  defp sandbox_attrs(context) do
    opts = context.opts
    execution_governance = context.execution_governance
    governance_sandbox = context.governance_sandbox

    %{
      sandbox_profile_ref:
        Keyword.get(
          opts,
          :sandbox_profile_ref,
          "sandbox://#{required_string!(execution_governance, :execution_governance_id)}"
        ),
      sandbox_level: context.sandbox_level,
      network_policy_ref:
        Keyword.get(opts, :network_policy_ref, network_policy_ref(governance_sandbox)),
      filesystem_policy_ref:
        Keyword.get(opts, :filesystem_policy_ref, filesystem_policy_ref(governance_sandbox)),
      acceptable_attestation: context.acceptable_attestation,
      attestation_requirement_ref:
        Keyword.get(
          opts,
          :attestation_requirement_ref,
          List.first(context.acceptable_attestation)
        )
    }
  end

  defp evidence_attrs(context) do
    opts = context.opts
    invocation = context.invocation
    request = context.request

    %{
      evidence_profile_ref:
        Keyword.get(opts, :evidence_profile_ref, "evidence://#{invocation.execution_id}/minimal"),
      redaction_profile_ref:
        Keyword.get(
          opts,
          :redaction_profile_ref,
          "redaction://#{invocation.execution_id}/default"
        ),
      input_ref: Keyword.get(opts, :input_ref, "input://#{invocation.execution_id}"),
      input_hash:
        Keyword.get(
          opts,
          :input_hash,
          "sha256:" <> sha256(inspect(optional(request, :extensions)))
        )
    }
  end

  defp extension_attrs(context) do
    invocation = context.invocation
    request = context.request
    execution_governance = context.execution_governance

    %{
      extensions: %{
        "mezzanine" => %{
          "execution_id" => invocation.execution_id,
          "installation_id" => invocation.installation_id,
          "submission_dedupe_key" => invocation.submission_dedupe_key
        },
        "citadel" => %{
          "invocation_request_id" => required_string!(request, :invocation_request_id),
          "execution_governance_id" =>
            required_string!(execution_governance, :execution_governance_id)
        },
        "workspace" => workspace_extension(context)
      }
    }
  end

  defp workspace_extension(context) do
    opts = context.opts
    execution_intent = execution_intent(context.request)
    workspace_ref = Keyword.get(opts, :workspace_ref, default_workspace_ref(context))
    workspace_root_ref = string_value(context.governance_workspace, "logical_workspace_ref")
    file_scope_ref = string_value(context.governance_sandbox, "file_scope_ref")

    workspace_root =
      Keyword.get(opts, :workspace_root) ||
        string_value(execution_intent, "workspace_root") ||
        string_value(context.governance_sandbox, "file_scope_hint")

    cwd =
      Keyword.get(opts, :cwd) ||
        string_value(execution_intent, "cwd") ||
        workspace_root

    %{
      "workspace_ref" => workspace_ref,
      "workspace_root_ref" => workspace_root_ref || workspace_ref,
      "file_scope_ref" => file_scope_ref || workspace_root_ref || workspace_ref,
      "workspace_root" => workspace_root,
      "cwd" => cwd,
      "path_redacted?" => true,
      "placement_ref" => Keyword.get(opts, :placement_ref, default_placement_ref(context))
    }
    |> Enum.reject(fn {_key, value} -> is_nil(value) end)
    |> Map.new()
  end

  defp validate_resource_scope_refs(attrs) do
    if Enum.any?(attrs.resource_scope_refs, &String.starts_with?(&1, "unresolved://")) do
      {:error,
       lower_denial_from_attrs(
         attrs,
         :resource_scope_unresolvable,
         "resource scope refs must resolve before lower dispatch"
       )}
    else
      :ok
    end
  end

  defp validate_authorized_capability(attrs) do
    if attrs.capability_id in attrs.allowed_operations do
      :ok
    else
      {:error,
       lower_denial_from_attrs(
         attrs,
         :capability_denied,
         "capability must be present in Citadel authority allowed_operations"
       )}
    end
  end

  defp validate_connector_ref(%{connector_ref: connector_ref} = attrs) do
    case connector_ref do
      value when is_binary(value) and value != "" ->
        :ok

      _missing ->
        {:error,
         lower_denial_from_attrs(
           attrs,
           :manifest_missing,
           "connector_ref must be resolved from binding or manifest data before lower dispatch"
         )}
    end
  end

  defp validate_connector_manifest(
         %{
           side_effect_class: side_effect_class,
           idempotency_class: idempotency_class,
           connector_manifest_state: connector_manifest_state
         } = attrs
       )
       when side_effect_class in [:write, "write"] and
              idempotency_class in [:non_idempotent, "non_idempotent"] do
    case normalize_atomish(connector_manifest_state) do
      :active ->
        :ok

      state ->
        {:error,
         lower_denial_from_attrs(
           attrs,
           manifest_denial_class(state),
           "non-idempotent writes require an active connector manifest before lower dispatch"
         )}
    end
  end

  defp validate_connector_manifest(_attrs), do: :ok

  defp manifest_denial_class(nil), do: :manifest_missing
  defp manifest_denial_class(:stale), do: :manifest_stale
  defp manifest_denial_class(:refresh_required), do: :manifest_stale
  defp manifest_denial_class(:invalid), do: :manifest_invalid
  defp manifest_denial_class(:quarantined), do: :manifest_quarantined
  defp manifest_denial_class(_state), do: :manifest_invalid

  defp validate_sandbox_posture(attrs, execution_governance, opts) do
    requested = posture_sandbox_level(Keyword.get(opts, :sandbox_level))

    required =
      execution_governance |> optional(:sandbox) |> sandbox_level() |> posture_sandbox_level()

    cond do
      is_nil(requested) or is_nil(required) ->
        :ok

      Map.fetch!(@sandbox_rank, requested) <= Map.fetch!(@sandbox_rank, required) ->
        :ok

      true ->
        {:error,
         lower_denial_from_attrs(
           attrs,
           :sandbox_downgrade,
           "requested sandbox level is weaker than Citadel execution governance"
         )}
    end
  end

  defp validate_attestation_posture(attrs, execution_governance, opts) do
    requested = Keyword.get(opts, :acceptable_attestation)

    if is_nil(requested) do
      :ok
    else
      required =
        execution_governance
        |> optional(:sandbox)
        |> list_value("acceptable_attestation", [])

      requested = normalize_string_list(requested)

      if required == [] or Enum.any?(requested, &(&1 in required)) do
        :ok
      else
        {:error,
         lower_denial_from_attrs(
           attrs,
           :attestation_unsatisfied,
           "requested attestation does not satisfy Citadel execution governance"
         )}
      end
    end
  end

  defp lower_denial_from_attrs(attrs, denial_class, reason) do
    GovernedLowerDenial.new!(%{
      lower_denial_ref:
        "lower-denial://#{URI.encode_www_form(attrs.lower_request_ref)}/#{denial_class}",
      lower_request_ref: attrs.lower_request_ref,
      lower_runtime_kind: attrs.lower_runtime_kind,
      denial_class: denial_class,
      reason: reason,
      tenant_ref: attrs.tenant_ref,
      subject_ref: attrs.subject_ref,
      run_ref: attrs.run_ref,
      workflow_ref: attrs.workflow_ref,
      attempt_ref: attrs.attempt_ref,
      trace_id: attrs.trace_id,
      authority_ref: attrs.authority_ref,
      authority_decision_hash: attrs.authority_decision_hash,
      capability_id: attrs.capability_id,
      action_id: attrs.action_id,
      connector_manifest_ref: attrs.connector_manifest_ref,
      connector_manifest_hash: attrs.connector_manifest_hash,
      capability_negotiation_ref: attrs.capability_negotiation_ref,
      policy_bundle_ref: attrs.policy_bundle_ref,
      cedar_schema_ref: attrs.cedar_schema_ref,
      script_ref: attrs.script_ref,
      resource_scope_refs: attrs.resource_scope_refs,
      sandbox_profile_ref: attrs.sandbox_profile_ref,
      extensions: %{"mezzanine" => %{"stage" => "authorized_invocation"}}
    })
  end

  defp lower_request_ref(%__MODULE__{} = invocation, capability_id) do
    "lower-request://#{invocation.execution_id}/#{URI.encode_www_form(capability_id)}"
  end

  defp infer_runtime_class(:codex_session), do: :session
  defp infer_runtime_class(:deterministic_fixture), do: :fixture
  defp infer_runtime_class(_lower_runtime_kind), do: :direct

  defp default_connector_manifest_ref(connector_ref)
       when is_binary(connector_ref) and connector_ref != "",
       do: "manifest://#{connector_ref}@local"

  defp default_connector_manifest_ref(_connector_ref), do: nil

  defp default_connector_manifest_hash(connector_ref, capability_id)
       when is_binary(connector_ref) and connector_ref != "",
       do: "sha256:" <> sha256("#{connector_ref}:#{capability_id}")

  defp default_connector_manifest_hash(_connector_ref, _capability_id), do: nil

  defp infer_side_effect_class(capability_id) do
    capability_id
    |> String.split(".")
    |> List.last()
    |> case do
      action when action in ["list", "retrieve", "fetch", "status", "get_self", "get_combined"] ->
        :read

      action when action in ["create", "update", "delete", "upsert", "label", "close"] ->
        :write

      _action ->
        :execute
    end
  end

  defp infer_idempotency_class(capability_id) do
    case infer_side_effect_class(capability_id) do
      :read -> :idempotent
      _other -> :non_idempotent
    end
  end

  defp resource_scope_refs(%__MODULE__{} = invocation, execution_governance) do
    governance_sandbox = optional(execution_governance, :sandbox) || %{}
    governance_workspace = optional(execution_governance, :workspace) || %{}

    [
      string_value(governance_workspace, "logical_workspace_ref"),
      string_value(governance_sandbox, "file_scope_ref"),
      "workspace://work_object/#{invocation.subject_id}"
    ]
    |> Enum.filter(&(is_binary(&1) and &1 != ""))
    |> Enum.uniq()
  end

  defp sandbox_level(%{} = sandbox) do
    sandbox
    |> string_value("level")
    |> normalize_atomish()
  end

  defp sandbox_level(_sandbox), do: nil

  defp posture_sandbox_level(level) when level in [:strict, :standard, :none], do: level

  defp posture_sandbox_level(level) when is_binary(level) do
    case level do
      "strict" -> :strict
      "standard" -> :standard
      "none" -> :none
      _other -> nil
    end
  end

  defp posture_sandbox_level(_level), do: nil

  defp network_policy_ref(sandbox) do
    case string_value(sandbox || %{}, "egress") do
      nil -> nil
      egress -> "network-policy://#{egress}"
    end
  end

  defp filesystem_policy_ref(sandbox) do
    case string_value(sandbox || %{}, "file_scope_ref") do
      nil -> nil
      file_scope_ref -> "filesystem-policy://#{URI.encode_www_form(file_scope_ref)}"
    end
  end

  defp policy_profile_ref(authority_packet, request_extensions) do
    citadel_extensions = citadel_extension(optional(authority_packet, :extensions))

    string_value(citadel_extensions, "policy_pack_id") ||
      get_in(request_extensions, ["citadel", "policy_pack_id"])
  end

  defp tre_policy(authority_packet, execution_governance, request_extensions) do
    [
      authority_packet |> optional(:extensions) |> citadel_extension() |> optional("tre_policy"),
      execution_governance
      |> optional(:extensions)
      |> citadel_extension()
      |> optional("tre_policy"),
      request_extensions |> citadel_extension() |> optional("tre_policy")
    ]
    |> Enum.find_value(%{}, fn
      %{} = policy -> policy
      _other -> nil
    end)
  end

  defp artifact_refs(dispatch_result) do
    [
      map_get(dispatch_result, :artifact_refs, []),
      nested_jido_receipt(dispatch_result) |> map_get(:artifact_refs, []),
      nested_execution_plane_receipt(dispatch_result) |> map_get(:artifact_refs, [])
    ]
    |> List.flatten()
    |> normalize_string_list()
    |> Enum.uniq()
  end

  defp event_refs(dispatch_result) do
    [
      map_get(dispatch_result, :event_refs, []),
      nested_jido_receipt(dispatch_result) |> map_get(:event_refs, []),
      nested_execution_plane_receipt(dispatch_result) |> map_get(:event_refs, [])
    ]
    |> List.flatten()
    |> Enum.reject(&is_nil/1)
    |> Enum.uniq()
  end

  defp nested_receipt_refs(dispatch_result) do
    jido_receipt = nested_jido_receipt(dispatch_result)
    execution_plane_receipt = nested_execution_plane_receipt(dispatch_result)

    %{}
    |> maybe_put_non_empty(
      "jido_governed_lower_receipt_ref",
      map_get(jido_receipt, :lower_receipt_ref)
    )
    |> maybe_put_non_empty("jido_governed_lower_status", map_get(jido_receipt, :status))
    |> maybe_put_non_empty(
      "execution_plane_receipt_ref",
      map_get(execution_plane_receipt, :receipt_ref)
    )
    |> maybe_put_non_empty("execution_plane_status", map_get(execution_plane_receipt, :status))
  end

  defp nested_jido_receipt(dispatch_result) do
    dispatch_result
    |> map_get(:output, %{})
    |> map_get(:governed_lower_receipt, %{})
    |> ensure_map()
  end

  defp nested_execution_plane_receipt(dispatch_result) do
    dispatch_result
    |> map_get(:output, %{})
    |> map_get(:execution_plane_receipt, %{})
    |> ensure_map()
  end

  defp map_get(value, key, default \\ nil)

  defp map_get(%{} = map, key, default) do
    Map.get(map, key, Map.get(map, to_string(key), default))
  end

  defp map_get(_value, _key, default), do: default

  defp ensure_map(%{} = map), do: map
  defp ensure_map(_value), do: %{}

  defp lower_run_ref(dispatch_result) do
    case Map.get(dispatch_result, :run) || Map.get(dispatch_result, "run") do
      %{run_id: run_id} when is_binary(run_id) -> run_id
      %{"run_id" => run_id} when is_binary(run_id) -> run_id
      _other -> nil
    end
  end

  defp lower_attempt_ref(dispatch_result) do
    case Map.get(dispatch_result, :attempt) || Map.get(dispatch_result, "attempt") do
      %{attempt_id: attempt_id} when is_binary(attempt_id) -> attempt_id
      %{"attempt_id" => attempt_id} when is_binary(attempt_id) -> attempt_id
      _other -> nil
    end
  end

  defp list_value(%{} = map, key, default) do
    map
    |> optional(key)
    |> case do
      nil -> default
      value -> normalize_string_list(value)
    end
  end

  defp list_value(_map, _key, default), do: default

  defp string_list_value(%{} = map, key, default) do
    map
    |> optional(key)
    |> case do
      nil -> default
      value -> normalize_string_list(value)
    end
  end

  defp string_list_value(_map, _key, default), do: default

  defp normalize_string_list(values) when is_list(values) do
    values
    |> Enum.filter(&(is_binary(&1) and &1 != ""))
    |> Enum.uniq()
  end

  defp normalize_string_list(_values), do: []

  defp string_value(map, key) when is_map(map) do
    case optional(map, key) do
      value when is_binary(value) and value != "" -> value
      _other -> nil
    end
  end

  defp string_value(_map, _key), do: nil

  defp normalize_atomish(nil), do: nil
  defp normalize_atomish(value) when is_atom(value), do: value

  defp normalize_atomish(value) when is_binary(value) do
    value
    |> String.trim()
    |> case do
      "" -> nil
      trimmed -> Map.get(@known_atomish_values, trimmed, trimmed)
    end
  end

  defp sha256(value), do: :crypto.hash(:sha256, value) |> Base.encode16(case: :lower)

  defp validate!(%__MODULE__{} = invocation) do
    request = normalized_request!(invocation.invocation_request)
    authority_packet = required!(request, :authority_packet)
    execution_governance = required!(request, :execution_governance)
    execution_envelope = execution_envelope(request)

    require_equals!(required_string!(request, :tenant_id), invocation.tenant_id, :tenant_id)
    require_equals!(required_string!(request, :trace_id), invocation.trace_id, :trace_id)

    require_equals!(
      required_string!(execution_envelope, :installation_id),
      invocation.installation_id,
      :installation_id
    )

    require_equals!(
      required_string!(execution_envelope, :subject_id),
      invocation.subject_id,
      :subject_id
    )

    require_equals!(
      required_string!(execution_envelope, :execution_id),
      invocation.execution_id,
      :execution_id
    )

    require_equals!(
      required_string!(execution_envelope, :submission_dedupe_key),
      invocation.submission_dedupe_key,
      :submission_dedupe_key
    )

    _installation_revision = required!(execution_envelope, :installation_revision)

    :ok = validate_authority_packet!(authority_packet)
    :ok = validate_execution_governance!(execution_governance)
    :ok = validate_action_binding!(invocation, authority_packet, execution_governance)
    :ok = authorize_capability!(invocation, default_capability!(invocation))

    invocation
  end

  defp validate_action_binding!(
         %__MODULE__{action_ref: nil},
         authority_packet,
         execution_governance
       ) do
    case {authority_for_action_ref(authority_packet),
          governance_for_action_ref(execution_governance)} do
      {nil, _governance_ref} ->
        :ok

      {authority_ref, nil} when is_binary(authority_ref) ->
        :ok

      {authority_ref, governance_ref} when authority_ref == governance_ref ->
        :ok

      {authority_ref, governance_ref} ->
        raise ArgumentError,
              "AuthorizedInvocation for_action_ref mismatch: expected #{inspect(authority_ref)}, got #{inspect(governance_ref)}"
    end
  end

  defp validate_action_binding!(
         %__MODULE__{action_ref: action_ref},
         authority_packet,
         execution_governance
       ) do
    authority_ref = authority_for_action_ref(authority_packet)
    governance_ref = governance_for_action_ref(execution_governance)

    cond do
      is_nil(authority_ref) ->
        raise ArgumentError,
              "AuthorizedInvocation action_ref requires Citadel authority for_action_ref"

      action_ref != authority_ref ->
        raise ArgumentError,
              "AuthorizedInvocation action_ref mismatch: expected #{inspect(authority_ref)}, got #{inspect(action_ref)}"

      not is_nil(governance_ref) and governance_ref != action_ref ->
        raise ArgumentError,
              "AuthorizedInvocation for_action_ref mismatch: expected #{inspect(action_ref)}, got #{inspect(governance_ref)}"

      true ->
        :ok
    end
  end

  defp validate_authority_packet!(packet) do
    _contract_version =
      require_equals!(required_string!(packet, :contract_version), "v1", :contract_version)

    _decision_id = required_string!(packet, :decision_id)
    _tenant_id = required_string!(packet, :tenant_id)
    _request_id = required_string!(packet, :request_id)
    _policy_version = required_string!(packet, :policy_version)
    :ok
  end

  defp validate_execution_governance!(packet) do
    _contract_version =
      require_equals!(required_string!(packet, :contract_version), "v1", :contract_version)

    _execution_governance_id = required_string!(packet, :execution_governance_id)
    _authority_ref = required!(packet, :authority_ref)
    _operations = required!(packet, :operations)
    :ok
  end

  defp validate_expected_installation_revision!(attrs, invocation_request) do
    case optional(attrs, :expected_installation_revision) do
      nil ->
        :ok

      expected_revision ->
        invocation_request
        |> actual_installation_revision!()
        |> validate_expected_installation_revision_value!(expected_revision)
    end
  end

  defp actual_installation_revision!(invocation_request) do
    invocation_request
    |> normalized_request!()
    |> execution_envelope()
    |> required!(:installation_revision)
  end

  defp validate_expected_installation_revision_value!(actual, expected) do
    cond do
      not (is_integer(expected) and expected >= 0) ->
        raise ArgumentError,
              "AuthorizedInvocation expected_installation_revision must be a non-negative integer, got: #{inspect(expected)}"

      not (is_integer(actual) and actual >= 0) ->
        raise ArgumentError,
              "AuthorizedInvocation installation_revision must be a non-negative integer, got: #{inspect(actual)}"

      actual == expected ->
        :ok

      true ->
        raise ArgumentError,
              "AuthorizedInvocation stale installation_revision: expected #{inspect(expected)}, got: #{inspect(actual)}"
    end
  end

  defp normalized_request!(%{__struct__: @invocation_request_module} = request),
    do: Map.from_struct(request)

  defp normalized_request!(%{} = request) do
    require_equals!(required!(request, :schema_version), 2, :schema_version)
    request
  end

  defp normalized_request!(request) do
    raise ArgumentError,
          "AuthorizedInvocation.invocation_request must be a Citadel.InvocationRequest.V2 struct or map representation, got: #{inspect(request)}"
  end

  defp execution_envelope(request) do
    request
    |> required!(:extensions)
    |> required!("citadel")
    |> required!("execution_envelope")
  end

  defp authority_packet_ref(authority_packet) do
    "authority-decision://#{required_string!(authority_packet, :decision_id)}"
  end

  defp authority_for_action_ref(authority_packet) do
    authority_packet
    |> optional(:extensions)
    |> citadel_extension()
    |> optional("for_action_ref")
    |> normalize_optional_string()
  end

  defp governance_for_action_ref(execution_governance) do
    execution_governance
    |> optional(:extensions)
    |> citadel_extension()
    |> optional("for_action_ref")
    |> normalize_optional_string()
  end

  defp citadel_extension(%{} = extensions) do
    case optional(extensions, "citadel") do
      %{} = citadel -> citadel
      _other -> %{}
    end
  end

  defp citadel_extension(_extensions), do: %{}

  defp normalize_optional_string(value) when is_binary(value) and value != "", do: value
  defp normalize_optional_string(_value), do: nil

  defp require_equals!(actual, expected, _field) when actual == expected, do: actual

  defp require_equals!(actual, expected, field) do
    raise ArgumentError,
          "AuthorizedInvocation #{field} mismatch: expected #{inspect(expected)}, got #{inspect(actual)}"
  end

  defp required_string!(attrs, key) do
    case required!(attrs, key) do
      value when is_binary(value) ->
        if String.trim(value) == "" do
          raise ArgumentError, "AuthorizedInvocation #{key} must be a non-empty string"
        end

        value

      value ->
        raise ArgumentError,
              "AuthorizedInvocation #{key} must be a non-empty string, got: #{inspect(value)}"
    end
  end

  defp optional_string!(attrs, key) do
    case optional(attrs, key) do
      nil ->
        nil

      value when is_binary(value) and value != "" ->
        value

      value ->
        raise ArgumentError,
              "AuthorizedInvocation #{key} must be a non-empty string, got: #{inspect(value)}"
    end
  end

  defp required!(attrs, key) when is_map(attrs) do
    cond do
      Map.has_key?(attrs, key) ->
        Map.fetch!(attrs, key)

      is_atom(key) and Map.has_key?(attrs, Atom.to_string(key)) ->
        Map.fetch!(attrs, Atom.to_string(key))

      true ->
        raise ArgumentError, "AuthorizedInvocation missing required field #{inspect(key)}"
    end
  end

  defp required!(attrs, key) do
    raise ArgumentError,
          "AuthorizedInvocation expected a map while reading #{inspect(key)}, got: #{inspect(attrs)}"
  end

  defp optional(attrs, key) when is_map(attrs) do
    cond do
      Map.has_key?(attrs, key) ->
        Map.fetch!(attrs, key)

      is_atom(key) and Map.has_key?(attrs, Atom.to_string(key)) ->
        Map.fetch!(attrs, Atom.to_string(key))

      true ->
        nil
    end
  end
end
