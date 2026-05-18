defmodule Mezzanine.IntegrationBridge.CredentialIngress do
  @moduledoc """
  Generic credential-ingress router for live provider proof commands.

  The bridge root accepts credential intent as data. Concrete provider material
  is handed to an explicit adapter-owned ingress module below this boundary.
  """

  alias Mezzanine.IntegrationBridge.LinearCredentialIngress

  @type credential_request :: map() | keyword()

  @spec prepare_invocation(credential_request(), map() | keyword(), keyword()) ::
          {:ok, map()} | {:error, term()}
  def prepare_invocation(credential_request, attrs, opts \\ [])

  def prepare_invocation(credential_request, attrs, opts)
      when (is_map(credential_request) or is_list(credential_request)) and
             (is_map(attrs) or is_list(attrs)) and is_list(opts) do
    credential_request = normalize(credential_request)

    with {:ok, adapter_ref} <- adapter_ref(credential_request),
         {:ok, credential_kind} <- credential_kind(credential_request) do
      dispatch(adapter_ref, credential_kind, credential_request, attrs, opts)
    end
  end

  def prepare_invocation(_credential_request, _attrs, _opts),
    do: {:error, :invalid_credential_request}

  defp dispatch("linear", :api_key, credential_request, attrs, opts) do
    case material(credential_request) do
      api_key when is_binary(api_key) and api_key != "" ->
        LinearCredentialIngress.prepare_api_key_invocation(api_key, attrs, opts)

      _missing ->
        {:error, :missing_credential_material}
    end
  end

  defp dispatch("linear", :connection, credential_request, attrs, opts) do
    case connection_id(credential_request) do
      connection_id when is_binary(connection_id) and connection_id != "" ->
        LinearCredentialIngress.prepare_connection_invocation(connection_id, attrs, opts)

      _missing ->
        {:error, :missing_connection_id}
    end
  end

  defp dispatch(adapter_ref, credential_kind, _credential_request, _attrs, _opts) do
    {:error, {:unsupported_credential_ingress, adapter_ref, credential_kind}}
  end

  defp adapter_ref(credential_request) do
    case string_value(credential_request, :adapter_ref) ||
           string_value(credential_request, :source_adapter_ref) do
      value when is_binary(value) and value != "" -> {:ok, normalize_adapter_ref(value)}
      _missing -> {:error, :missing_credential_adapter_ref}
    end
  end

  defp credential_kind(credential_request) do
    case value(credential_request, :credential_kind) || value(credential_request, :kind) do
      kind when kind in [:api_key, "api_key"] -> {:ok, :api_key}
      kind when kind in [:connection, "connection"] -> {:ok, :connection}
      other -> {:error, {:unsupported_credential_kind, other}}
    end
  end

  defp material(credential_request) do
    string_value(credential_request, :credential_material) ||
      string_value(credential_request, :material) ||
      string_value(credential_request, :api_key)
  end

  defp connection_id(credential_request) do
    string_value(credential_request, :connection_id) ||
      string_value(credential_request, :credential_material) ||
      string_value(credential_request, :material)
  end

  defp normalize_adapter_ref(adapter_ref) do
    adapter_ref
    |> String.split(["/", ":", "@", "."], trim: true)
    |> List.last()
  end

  defp normalize(%_{} = struct), do: Map.from_struct(struct)
  defp normalize(%{} = map), do: Map.new(map)
  defp normalize(list) when is_list(list), do: Map.new(list)

  defp string_value(map, key) do
    case value(map, key) do
      value when is_binary(value) ->
        value = String.trim(value)
        if value == "", do: nil, else: value

      value when is_atom(value) and not is_nil(value) ->
        Atom.to_string(value)

      _other ->
        nil
    end
  end

  defp value(map, key), do: Map.get(map, key) || Map.get(map, Atom.to_string(key))
end
