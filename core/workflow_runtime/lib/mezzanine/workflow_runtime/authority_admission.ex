defmodule Mezzanine.WorkflowRuntime.AuthorityAdmission do
  @moduledoc """
  Provider-effect authority admission for workflow handoff.

  The contract is deliberately ref-only. It validates that workflow runtime has
  all authority refs needed before a provider effect can be handed to Jido or a
  lower runtime, and it refuses raw material in the admission payload.
  """

  @required_refs [
    :system_authorization_ref,
    :authority_packet_ref,
    :provider_family,
    :provider_account_ref,
    :connector_instance_ref,
    :credential_handle_ref,
    :credential_lease_ref,
    :target_ref,
    :attach_grant_ref,
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
    :token
  ]

  @handoff_fields @required_refs ++ [:trace_id, :authority_decision_ref]

  @spec required_refs() :: [atom()]
  def required_refs, do: @required_refs

  @spec forbidden_material() :: [atom()]
  def forbidden_material, do: @forbidden_material

  @spec authorize_provider_dispatch(map() | keyword()) ::
          {:ok, map()}
          | {:error, {:missing_required_authority_refs, [atom()]}}
          | {:error, {:forbidden_authority_material, [atom()]}}
  def authorize_provider_dispatch(attrs) when is_map(attrs) or is_list(attrs) do
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

  defp missing_required(attrs) do
    Enum.reject(@required_refs, &present?(Map.get(attrs, &1)))
  end

  defp forbidden_material_present(attrs) do
    Enum.filter(@forbidden_material, &Map.has_key?(attrs, &1))
  end

  defp redacted_handoff(attrs) do
    attrs
    |> Map.take(@handoff_fields)
    |> Map.put(
      :handoff_ref,
      "workflow-authority-handoff://#{Map.fetch!(attrs, :idempotency_key)}"
    )
    |> Map.put(:raw_material_present?, false)
  end

  defp normalize(attrs) when is_list(attrs), do: attrs |> Map.new() |> normalize()

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

  defp present?(value) when is_binary(value), do: String.trim(value) != ""
  defp present?(value), do: not is_nil(value)
end
