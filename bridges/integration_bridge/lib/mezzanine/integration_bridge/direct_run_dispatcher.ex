defmodule Mezzanine.IntegrationBridge.DirectRunDispatcher do
  @moduledoc """
  Direct public-platform dispatch for narrow run-intent cases.
  """

  alias Jido.Integration.V2.GovernedLowerEnvelope
  alias Mezzanine.IntegrationBridge.AuthorizedInvocation
  alias Mezzanine.IntegrationBridge.ProviderAuthorityAdmission

  @invoke_fun &Jido.Integration.V2.invoke/3

  @spec invoke_run_intent(AuthorizedInvocation.t(), keyword()) :: {:ok, map()} | {:error, term()}
  def invoke_run_intent(%AuthorizedInvocation{} = invocation, opts \\ []) when is_list(opts) do
    invoke_fun = Keyword.get(opts, :invoke_fun, @invoke_fun)
    invoke_opts = Keyword.get(opts, :invoke_opts, [])

    capability_id =
      Keyword.get(opts, :capability_id, AuthorizedInvocation.default_capability!(invocation))

    with {:ok, envelope} <-
           AuthorizedInvocation.governed_lower_envelope(invocation, capability_id, opts),
         :ok <- require_dispatchable(envelope, invoke_opts),
         :ok <- maybe_dry_run_denial(envelope, opts),
         {:ok, authority_handoff} <-
           ProviderAuthorityAdmission.authorize_direct_run(invocation, envelope, opts) do
      input =
        invocation
        |> AuthorizedInvocation.invoke_input(capability_id)
        |> merge_dispatch_input(Keyword.get(opts, :input, %{}))
        |> Map.put(:governed_lower_envelope, GovernedLowerEnvelope.to_map(envelope))

      invoke_opts =
        invoke_opts
        |> put_default_jido_policy_opts(invocation, envelope, opts)
        |> Keyword.put(:governed_lower_envelope, envelope)

      invoke_fun.(capability_id, input, invoke_opts)
      |> attach_governed_receipt(envelope, authority_handoff)
    end
  end

  defp maybe_dry_run_denial(envelope, opts) do
    if Keyword.get(opts, :dry_run?) == true and write_side_effect?(envelope) do
      {:error,
       AuthorizedInvocation.governed_lower_denial(
         envelope,
         :policy_denied,
         "dry run requested before provider dispatch"
       )}
    else
      :ok
    end
  end

  defp write_side_effect?(%{side_effect_class: side_effect_class})
       when side_effect_class in [:write, "write"],
       do: true

  defp write_side_effect?(_envelope), do: false

  defp put_default_jido_policy_opts(invoke_opts, invocation, envelope, opts) do
    invoke_opts
    |> put_new_present(:tenant_id, envelope.tenant_ref)
    |> put_new_present(:trace_id, envelope.trace_id)
    |> put_new_present(:actor_id, actor_id(invocation))
    |> put_new_present(:environment, environment(invocation, opts))
    |> Keyword.put_new(:allowed_operations, envelope.allowed_operations)
  end

  defp actor_id(%AuthorizedInvocation{invocation_request: invocation_request}) do
    invocation_request
    |> request_value(:actor_id)
    |> string_value()
  end

  defp environment(%AuthorizedInvocation{invocation_request: invocation_request}, opts) do
    Keyword.get(opts, :environment) ||
      invocation_request
      |> request_value(:environment)
      |> environment_value() ||
      :prod
  end

  defp request_value(%_{} = struct, key), do: struct |> Map.from_struct() |> request_value(key)
  defp request_value(%{} = map, key), do: Map.get(map, key) || Map.get(map, Atom.to_string(key))

  defp string_value(value) when is_binary(value) and value != "", do: value
  defp string_value(value) when is_atom(value), do: Atom.to_string(value)
  defp string_value(_value), do: nil

  defp environment_value(value) when is_binary(value) and value != "" do
    value
    |> String.trim()
    |> String.to_existing_atom()
  rescue
    ArgumentError -> value
  end

  defp environment_value(value) when is_atom(value), do: value
  defp environment_value(_value), do: nil

  defp put_new_present(keyword, _key, nil), do: keyword
  defp put_new_present(keyword, key, value), do: Keyword.put_new(keyword, key, value)

  defp require_dispatchable(envelope, invoke_opts) do
    if GovernedLowerEnvelope.dispatchable?(envelope) or
         tre_adapter_enabled?(envelope, invoke_opts) do
      :ok
    else
      {:error,
       AuthorizedInvocation.governed_lower_denial(
         envelope,
         :lower_runtime_unavailable,
         "lower runtime kind #{inspect(envelope.lower_runtime_kind)} is reserved or unavailable"
       )}
    end
  end

  defp tre_adapter_enabled?(%GovernedLowerEnvelope{lower_runtime_kind: :tre_rhai}, invoke_opts)
       when is_list(invoke_opts) do
    case Keyword.get(invoke_opts, :tre_adapter) do
      module when is_atom(module) ->
        Code.ensure_loaded?(module) and function_exported?(module, :execute, 3)

      _other ->
        false
    end
  end

  defp tre_adapter_enabled?(%GovernedLowerEnvelope{}, _invoke_opts), do: false

  defp attach_governed_receipt({:ok, result}, envelope, authority_handoff) when is_map(result) do
    receipt = AuthorizedInvocation.governed_lower_receipt!(envelope, :succeeded, result)

    {:ok,
     result
     |> attach_authority_handoff(authority_handoff)
     |> Map.put(:governed_lower_envelope, envelope)
     |> Map.put(:governed_lower_receipt, receipt)
     |> Map.put(:operation_receipt, operation_receipt(envelope, receipt, authority_handoff))}
  end

  defp attach_governed_receipt({:error, result}, envelope, authority_handoff)
       when is_map(result) do
    receipt = AuthorizedInvocation.governed_lower_receipt!(envelope, :failed, result)

    {:error,
     result
     |> attach_authority_handoff(authority_handoff)
     |> Map.put(:governed_lower_envelope, envelope)
     |> Map.put(:governed_lower_receipt, receipt)
     |> Map.put(:operation_receipt, operation_receipt(envelope, receipt, authority_handoff))}
  end

  defp attach_governed_receipt(other, _envelope, _authority_handoff), do: other

  defp attach_authority_handoff(result, authority_handoff) do
    result
    |> Map.merge(ProviderAuthorityAdmission.result_fields(authority_handoff))
    |> Map.put(:authority_handoff, authority_handoff)
  end

  defp operation_receipt(%GovernedLowerEnvelope{} = envelope, receipt, authority_handoff) do
    authority_fields = ProviderAuthorityAdmission.result_fields(authority_handoff)

    %{
      operation_receipt_ref: receipt.lower_receipt_ref,
      lower_receipt_ref: receipt.lower_receipt_ref,
      lower_request_ref: receipt.lower_request_ref,
      lower_runtime_kind: atom_to_string(receipt.lower_runtime_kind),
      status: atom_to_string(receipt.status),
      capability_id: receipt.capability_id,
      action_id: receipt.action_id,
      effect_request_ref: receipt.lower_request_ref,
      connector_ref: receipt.connector_ref,
      connector_manifest_ref: receipt.connector_manifest_ref,
      connector_manifest_hash: receipt.connector_manifest_hash,
      connector_manifest_state: atom_to_string(receipt.connector_manifest_state),
      capability_negotiation_ref: receipt.capability_negotiation_ref,
      connector_binding_ref: Map.get(authority_fields, :connector_binding_ref),
      credential_lease_ref: Map.get(authority_fields, :credential_lease_ref),
      authority_ref: receipt.authority_ref,
      authority_decision_hash: receipt.authority_decision_hash,
      authority_handoff_ref: Map.get(authority_fields, :authority_handoff_ref),
      trace_id: receipt.trace_id,
      tenant_ref: receipt.tenant_ref,
      subject_ref: receipt.subject_ref,
      run_ref: receipt.run_ref,
      workflow_ref: receipt.workflow_ref,
      attempt_ref: receipt.attempt_ref,
      evidence_profile_ref: receipt.evidence_profile_ref,
      redaction_profile_ref: receipt.redaction_profile_ref,
      artifact_refs: receipt.artifact_refs,
      event_refs: receipt.event_refs,
      input_ref: receipt.input_ref,
      input_hash: receipt.input_hash,
      workspace_ref: receipt.workspace_ref,
      target_ref: receipt.target_ref,
      placement_ref: receipt.placement_ref,
      sandbox_profile_ref: receipt.sandbox_profile_ref,
      idempotency_key: envelope.idempotency_key
    }
    |> compact()
  end

  defp merge_dispatch_input(input, extra_input) when is_map(extra_input) do
    Map.merge(input, extra_input)
  end

  defp merge_dispatch_input(input, extra_input) when is_list(extra_input) do
    Map.merge(input, Map.new(extra_input))
  end

  defp merge_dispatch_input(_input, extra_input) do
    raise ArgumentError,
          "dispatch input must be a map or keyword list, got: #{inspect(extra_input)}"
  end

  defp atom_to_string(nil), do: nil
  defp atom_to_string(value) when is_atom(value), do: Atom.to_string(value)

  defp compact(map) do
    map
    |> Enum.reject(fn {_key, value} -> value in [nil, "", [], %{}] end)
    |> Map.new()
  end
end
