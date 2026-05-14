defmodule Mezzanine.IntegrationBridge.ProviderAuthorityAdmission do
  @moduledoc """
  Ref-only provider-dispatch authority admission for integration bridge runtimes.

  The bridge prefers the shared workflow-runtime `AuthorityAdmission` contract
  when it is present in the assembled stack. Package-local tests and direct
  bridge use still enforce the same ref-only shape through the local fallback.
  """

  alias Jido.Integration.V2.GovernedLowerEnvelope
  alias Mezzanine.IntegrationBridge.AuthorizedInvocation

  @required_refs [
    :system_authorization_ref,
    :authority_packet_ref,
    :provider_family,
    :provider_account_ref,
    :connector_instance_ref,
    :connector_binding_ref,
    :credential_handle_ref,
    :credential_lease_ref,
    :target_ref,
    :attach_grant_ref,
    :target_auth_posture_ref,
    :boundary_session_id,
    :workspace_ref,
    :no_egress_posture_ref,
    :process_target_identity_ref,
    :stream_target_identity_ref,
    :operation_scope_ref,
    :operation_policy_ref,
    :policy_revision_ref,
    :idempotency_key
  ]

  @forbidden_material [
    :api_key,
    :auth_json,
    :authorization_header,
    :client,
    :default_client,
    :env,
    :native_auth_file,
    :provider_payload,
    :raw_secret,
    :raw_token,
    :singleton_client,
    :target_credential,
    :target_path,
    :token,
    :token_file,
    :unmanaged_target_config,
    :workspace_secret
  ]

  @spec authorize_direct_run(AuthorizedInvocation.t(), GovernedLowerEnvelope.t(), keyword()) ::
          {:ok, map()} | {:error, term()}
  def authorize_direct_run(
        %AuthorizedInvocation{} = invocation,
        %GovernedLowerEnvelope{} = envelope,
        opts
      )
      when is_list(opts) do
    envelope
    |> direct_run_authority_attrs(invocation, opts)
    |> authorize_provider_dispatch(opts)
  end

  @spec authorize_codex_dispatch(map(), String.t(), keyword(), keyword()) ::
          {:ok, map()} | {:error, term()}
  def authorize_codex_dispatch(attrs, capability_id, invoke_opts, opts)
      when is_map(attrs) and is_binary(capability_id) and is_list(invoke_opts) and is_list(opts) do
    attrs
    |> codex_authority_attrs(capability_id, invoke_opts, opts)
    |> authorize_provider_dispatch(opts)
  end

  @spec authorize_provider_dispatch(map(), keyword()) :: {:ok, map()} | {:error, term()}
  def authorize_provider_dispatch(attrs, opts) when is_map(attrs) and is_list(opts) do
    opts
    |> Keyword.get(:authority_admission_fun, &default_authorize_provider_dispatch/1)
    |> then(& &1.(attrs))
  end

  @spec result_fields(map() | nil) :: map()
  def result_fields(nil), do: %{}
  def result_fields(handoff) when is_map(handoff) and map_size(handoff) == 0, do: %{}

  def result_fields(handoff) when is_map(handoff) do
    %{
      authority_authorized?: true,
      authority_handoff_ref: value(handoff, :handoff_ref),
      authority_packet_ref: value(handoff, :authority_packet_ref),
      connector_binding_ref: value(handoff, :connector_binding_ref),
      credential_lease_ref: value(handoff, :credential_lease_ref),
      authority_raw_material_present?: truthy?(value(handoff, :raw_material_present?))
    }
    |> compact()
  end

  @spec metadata(map() | nil) :: map()
  def metadata(nil), do: %{}
  def metadata(handoff) when is_map(handoff) and map_size(handoff) == 0, do: %{}

  def metadata(handoff) when is_map(handoff) do
    %{
      "authorized?" => true,
      "handoff_ref" => value(handoff, :handoff_ref),
      "authority_packet_ref" => value(handoff, :authority_packet_ref),
      "connector_binding_ref" => value(handoff, :connector_binding_ref),
      "credential_lease_ref" => value(handoff, :credential_lease_ref),
      "raw_material_present?" => truthy?(value(handoff, :raw_material_present?))
    }
    |> compact()
  end

  defp default_authorize_provider_dispatch(attrs) do
    module = :"Elixir.Mezzanine.WorkflowRuntime.AuthorityAdmission"

    if Code.ensure_loaded?(module) and function_exported?(module, :authorize_provider_dispatch, 1) do
      # credo:disable-for-next-line Credo.Check.Refactor.Apply
      apply(module, :authorize_provider_dispatch, [attrs])
    else
      local_authorize_provider_dispatch(attrs)
    end
  end

  defp local_authorize_provider_dispatch(attrs) do
    attrs = normalize(attrs)

    case forbidden_material_present(attrs) do
      [] ->
        case missing_required(attrs) do
          [] -> {:ok, redacted_handoff(attrs)}
          missing -> {:error, {:missing_required_authority_refs, missing}}
        end

      forbidden ->
        {:error, {:forbidden_authority_material, forbidden}}
    end
  end

  defp direct_run_authority_attrs(%GovernedLowerEnvelope{} = envelope, invocation, opts) do
    invoke_opts = Keyword.get(opts, :invoke_opts, [])
    connection_id = string_value(Keyword.get(invoke_opts, :connection_id))
    provider_family = provider_family(envelope.capability_id, envelope.connector_ref)
    connection_or_connector_ref = first_present([connection_id, envelope.connector_ref])
    connection_or_tenant_ref = first_present([connection_id, envelope.tenant_ref])
    connection_or_idempotency = first_present([connection_id, envelope.idempotency_key])

    authority_attrs(%{
      system_authorization_ref:
        "system-authority://#{ref_part(envelope.tenant_ref)}/#{ref_part(envelope.idempotency_key)}",
      authority_packet_ref: envelope.authority_ref,
      authority_decision_ref: envelope.authority_ref,
      provider_family: provider_family,
      provider_account_ref:
        "provider-account://#{provider_family}/#{ref_part(connection_or_tenant_ref)}",
      connector_instance_ref:
        "connector-instance://#{provider_family}/#{ref_part(envelope.connector_ref)}",
      connector_binding_ref: connector_binding_ref(provider_family, connection_or_connector_ref),
      credential_handle_ref:
        "credential-handle://#{provider_family}/#{ref_part(connection_or_idempotency)}",
      credential_lease_ref:
        first_present([
          string_value(Keyword.get(opts, :credential_lease_ref)),
          "credential-lease://#{provider_family}/#{ref_part(connection_or_idempotency)}/#{ref_part(envelope.capability_id)}"
        ]),
      target_ref: envelope.target_ref,
      attach_grant_ref: "attach-grant://#{ref_part(envelope.target_ref)}",
      target_auth_posture_ref: "target-auth-posture://#{ref_part(envelope.target_ref)}",
      boundary_session_id: first_present([envelope.run_ref, envelope.idempotency_key]),
      workspace_ref: envelope.workspace_ref,
      no_egress_posture_ref:
        first_present([
          envelope.network_policy_ref,
          "no-egress-posture://#{ref_part(envelope.tenant_ref)}/#{provider_family}"
        ]),
      process_target_identity_ref: "process-target-identity://#{ref_part(envelope.target_ref)}",
      stream_target_identity_ref: "stream-target-identity://#{ref_part(envelope.target_ref)}",
      operation_scope_ref:
        "operation-scope://#{provider_family}/#{ref_part(envelope.capability_id)}",
      operation_policy_ref:
        first_present([
          envelope.policy_profile_ref,
          "operation-policy://#{provider_family}/#{ref_part(envelope.capability_id)}"
        ]),
      policy_revision_ref:
        first_present([envelope.policy_bundle_hash, envelope.authority_decision_hash]),
      idempotency_key: envelope.idempotency_key,
      trace_id: envelope.trace_id,
      connector_ref: envelope.connector_ref,
      lower_request_ref: envelope.lower_request_ref,
      invocation_id: invocation.idempotency_key
    })
  end

  defp codex_authority_attrs(attrs, capability_id, invoke_opts, _opts) do
    connection_id = string_value(Keyword.get(invoke_opts, :connection_id))

    tenant_ref =
      first_present([
        string_value(Keyword.get(invoke_opts, :tenant_id)),
        string_value(value(attrs, :tenant_ref))
      ])

    run_ref =
      first_present([
        string_value(value(attrs, :run_ref)),
        string_value(value(attrs, :idempotency_key))
      ])

    subject_ref =
      first_present([string_value(value(attrs, :subject_ref)), "subject://codex/runtime"])

    workspace_ref =
      first_present([
        string_value(value(attrs, :workspace_ref)),
        "workspace://codex/#{ref_part(run_ref)}"
      ])

    authority_ref =
      first_present([
        string_value(value(attrs, :authority_context_ref)),
        "authority-packet://#{ref_part(run_ref)}"
      ])

    idempotency_key =
      first_present([string_value(value(attrs, :idempotency_key)), ref_part(run_ref)])

    connection_or_tenant_ref = first_present([connection_id, tenant_ref])
    connection_or_idempotency = first_present([connection_id, idempotency_key])

    authority_attrs(%{
      system_authorization_ref:
        "system-authority://#{ref_part(tenant_ref)}/#{ref_part(idempotency_key)}",
      authority_packet_ref: authority_ref,
      authority_decision_ref: authority_ref,
      provider_family: "codex",
      provider_account_ref: "provider-account://codex/#{ref_part(connection_or_tenant_ref)}",
      connector_instance_ref: "connector-instance://codex/codex_cli",
      connector_binding_ref:
        connector_binding_ref("codex", first_present([connection_id, "codex_cli"])),
      credential_handle_ref: "credential-handle://codex/#{ref_part(connection_or_idempotency)}",
      credential_lease_ref:
        "credential-lease://codex/#{ref_part(connection_or_idempotency)}/#{ref_part(capability_id)}",
      target_ref: subject_ref,
      attach_grant_ref: "attach-grant://#{ref_part(subject_ref)}",
      target_auth_posture_ref: "target-auth-posture://#{ref_part(subject_ref)}",
      boundary_session_id: run_ref,
      workspace_ref: workspace_ref,
      no_egress_posture_ref: "no-egress-posture://#{ref_part(tenant_ref)}/codex",
      process_target_identity_ref: "process-target-identity://#{ref_part(subject_ref)}",
      stream_target_identity_ref: "stream-target-identity://#{ref_part(subject_ref)}",
      operation_scope_ref: "operation-scope://codex/#{ref_part(capability_id)}",
      operation_policy_ref: "operation-policy://codex/#{ref_part(capability_id)}",
      policy_revision_ref: "policy-revision://codex/#{ref_part(idempotency_key)}",
      idempotency_key: idempotency_key,
      trace_id: string_value(value(attrs, :trace_id)),
      connector_ref: "jido/connectors/codex_cli"
    })
  end

  defp authority_attrs(attrs) do
    attrs
    |> Enum.reject(fn {_key, value} -> value in [nil, ""] end)
    |> Map.new()
  end

  defp connector_binding_ref(provider_family, value) do
    "connector-binding://#{provider_family}/#{ref_part(value)}"
  end

  defp first_present(values) when is_list(values) do
    Enum.find(values, &present?/1)
  end

  defp provider_family("linear." <> _rest, _connector_ref), do: "linear"
  defp provider_family("github." <> _rest, _connector_ref), do: "github"
  defp provider_family("codex." <> _rest, _connector_ref), do: "codex"
  defp provider_family(_capability_id, "jido/connectors/linear"), do: "linear"
  defp provider_family(_capability_id, "jido/connectors/github"), do: "github"
  defp provider_family(_capability_id, "jido/connectors/codex_cli"), do: "codex"

  defp provider_family(_capability_id, connector_ref),
    do: connector_ref |> to_string() |> ref_part()

  defp missing_required(attrs) do
    Enum.reject(@required_refs, &present?(Map.get(attrs, &1)))
  end

  defp forbidden_material_present(attrs) do
    Enum.filter(@forbidden_material, &Map.has_key?(attrs, &1))
  end

  defp redacted_handoff(attrs) do
    attrs
    |> Map.take(@required_refs ++ [:trace_id, :authority_decision_ref])
    |> Map.put(
      :handoff_ref,
      "workflow-authority-handoff://#{Map.fetch!(attrs, :idempotency_key)}"
    )
    |> Map.put(:raw_material_present?, false)
  end

  defp normalize(attrs) when is_map(attrs) do
    Map.new(attrs, fn {key, value} -> {normalize_key(key), value} end)
  end

  defp normalize_key(key) when is_atom(key), do: key

  defp normalize_key(key) when is_binary(key) do
    Enum.find(
      @required_refs ++ @forbidden_material ++ [:trace_id, :authority_decision_ref],
      key,
      fn
        candidate -> Atom.to_string(candidate) == key
      end
    )
  end

  defp value(%_{} = struct, key), do: struct |> Map.from_struct() |> value(key)
  defp value(%{} = map, key), do: Map.get(map, key) || Map.get(map, Atom.to_string(key))

  defp string_value(nil), do: nil

  defp string_value(value) when is_binary(value) do
    value = String.trim(value)
    if value == "", do: nil, else: value
  end

  defp string_value(value) when is_atom(value), do: Atom.to_string(value)
  defp string_value(_value), do: nil

  defp ref_part(nil), do: "unknown"

  defp ref_part(value) do
    value
    |> to_string()
    |> URI.encode_www_form()
  end

  defp present?(value) when is_binary(value), do: String.trim(value) != ""
  defp present?(value), do: not is_nil(value)

  defp truthy?(value) when value in [true, "true", 1, "1"], do: true
  defp truthy?(_value), do: false

  defp compact(map) do
    map
    |> Enum.reject(fn {_key, value} -> value in [nil, "", [], %{}] end)
    |> Map.new()
  end
end
