defmodule Mezzanine.Lifecycle.DispatchEnvelopeRefValidator do
  @moduledoc """
  Phase 2 dispatch/admission guard for opaque execution refs.

  This module validates only ref presence, tenant/install consistency when it is
  locally derivable, and raw credential material rejection. It deliberately does
  not validate lease semantics, connector binding semantics, provider identity,
  or runtime materialization.
  """

  @raw_credential_keys MapSet.new([
                         "access_token",
                         "api_key",
                         "authorization_header",
                         "client_secret",
                         "credential_material",
                         "credential_secret",
                         "credential_value",
                         "password",
                         "private_key",
                         "provider_secret",
                         "raw_credential",
                         "raw_credentials",
                         "refresh_token",
                         "secret",
                         "secret_key",
                         "secrets"
                       ])

  @field_atoms %{
    "authority_decision_ref" => :authority_decision_ref,
    "connector_binding_ref" => :connector_binding_ref,
    "credential_lease_ref" => :credential_lease_ref,
    "credentials_required" => :credentials_required,
    "installation_id" => :installation_id,
    "installation_revision" => :installation_revision,
    "no_credentials_posture_ref" => :no_credentials_posture_ref,
    "tenant_id" => :tenant_id,
    "tenant_ref" => :tenant_ref
  }

  @spec validate(map()) :: :ok | {:error, atom()}
  def validate(input) when is_map(input) do
    with :ok <- require_non_empty(input_value(input, :trace_id), :missing_trace_id),
         :ok <-
           require_non_empty(input_value(input, :submission_dedupe_key), :missing_idempotency_key),
         :ok <- require_installation_revision(input),
         :ok <-
           require_snapshot_ref(
             input,
             :dispatch_envelope,
             "authority_decision_ref",
             :missing_authority_decision_ref
           ),
         :ok <-
           require_snapshot_ref(
             input,
             :binding_snapshot,
             "connector_binding_ref",
             :missing_connector_binding_ref
           ),
         :ok <- require_credential_posture(input),
         :ok <- reject_raw_credential_material(input),
         :ok <- validate_installation_revision(input) do
      validate_tenant_installation(input)
    end
  end

  def validate(_input), do: {:error, :invalid_dispatch_ref_validation_input}

  defp require_non_empty(value, error) when is_binary(value) do
    if String.trim(value) == "", do: {:error, error}, else: :ok
  end

  defp require_non_empty(_value, error), do: {:error, error}

  defp require_installation_revision(input) do
    case input_value(input, :compiled_pack_revision) do
      revision when is_integer(revision) and revision > 0 -> :ok
      _other -> {:error, :missing_installation_revision}
    end
  end

  defp require_snapshot_ref(input, snapshot, field, error) do
    if present_ref?(snapshot_value(input, snapshot, field)), do: :ok, else: {:error, error}
  end

  defp require_credential_posture(input) do
    cond do
      present_ref?(snapshot_value(input, :dispatch_envelope, "credential_lease_ref")) ->
        :ok

      credentials_required?(input) ->
        {:error, :missing_credential_lease_ref}

      present_ref?(snapshot_value(input, :dispatch_envelope, "no_credentials_posture_ref")) ->
        :ok

      true ->
        {:error, :missing_no_credentials_posture_ref}
    end
  end

  defp reject_raw_credential_material(input) do
    if Enum.any?(snapshots(input), &raw_credential_material?/1) do
      {:error, :raw_credential_material_forbidden}
    else
      :ok
    end
  end

  defp validate_installation_revision(input) do
    current_revision = input_value(input, :compiled_pack_revision)

    case snapshot_value(input, "installation_revision") do
      nil ->
        :ok

      revision when revision == current_revision ->
        :ok

      revision when is_binary(revision) ->
        case Integer.parse(revision) do
          {^current_revision, ""} -> :ok
          _other -> {:error, :stale_installation_revision}
        end

      _other ->
        {:error, :stale_installation_revision}
    end
  end

  defp validate_tenant_installation(input) do
    tenant_id = input_value(input, :tenant_id)
    installation_id = input_value(input, :installation_id)

    cond do
      mismatched_ref?(snapshot_value(input, "tenant_id"), tenant_id) ->
        {:error, :tenant_installation_mismatch}

      mismatched_ref?(snapshot_value(input, "tenant_ref"), tenant_id) ->
        {:error, :tenant_installation_mismatch}

      mismatched_ref?(snapshot_value(input, "installation_id"), installation_id) ->
        {:error, :tenant_installation_mismatch}

      true ->
        :ok
    end
  end

  defp credentials_required?(input) do
    snapshot_value(input, :dispatch_envelope, "credentials_required") in [
      true,
      "true",
      :required,
      "required"
    ]
  end

  defp snapshot_value(input, snapshot, field) do
    input
    |> input_value(snapshot)
    |> map_value(field)
  end

  defp snapshot_value(input, field) do
    Enum.find_value(snapshots(input), &map_value(&1, field))
  end

  defp snapshots(input) do
    [
      input_value(input, :binding_snapshot),
      input_value(input, :dispatch_envelope),
      input_value(input, :intent_snapshot)
    ]
    |> Enum.filter(&is_map/1)
  end

  defp raw_credential_material?(%{} = map) do
    Enum.any?(map, fn {key, value} ->
      raw_credential_key?(key) or raw_credential_material?(value)
    end)
  end

  defp raw_credential_material?(list) when is_list(list),
    do: Enum.any?(list, &raw_credential_material?/1)

  defp raw_credential_material?(_value), do: false

  defp raw_credential_key?(key) do
    key
    |> to_string()
    |> String.downcase()
    |> then(&MapSet.member?(@raw_credential_keys, &1))
  end

  defp present_ref?(value) when is_binary(value), do: String.trim(value) != ""
  defp present_ref?(value) when is_atom(value), do: value not in [nil, false]
  defp present_ref?(_value), do: false

  defp mismatched_ref?(nil, _expected), do: false
  defp mismatched_ref?("", _expected), do: false
  defp mismatched_ref?(_value, nil), do: false
  defp mismatched_ref?(value, expected), do: to_string(value) != to_string(expected)

  defp input_value(map, key) do
    case Map.fetch(map, key) do
      {:ok, value} -> value
      :error -> Map.get(map, to_string(key))
    end
  end

  defp map_value(map, key) when is_map(map) do
    case Map.fetch(map, key) do
      {:ok, value} ->
        value

      :error ->
        case Map.fetch(@field_atoms, key) do
          {:ok, atom_key} -> Map.get(map, atom_key)
          :error -> nil
        end
    end
  end
end
