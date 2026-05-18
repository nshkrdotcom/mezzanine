defmodule Mezzanine.IntegrationBridge.CredentialIngress do
  @moduledoc """
  Generic credential-ingress router for live provider proof commands.

  The bridge root accepts credential intent as data. Concrete provider material
  is handed to an explicit adapter-owned ingress module below this boundary.
  """

  alias Jido.Integration.Secrets.Broker
  alias Jido.Integration.Secrets.EnvProvider
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
    with {:ok, provider} <- secret_provider(credential_request),
         {:ok, lease_ref} <- lease_ref(credential_request, attrs),
         {:ok, scope} <- secret_scope(credential_request) do
      Broker.with_materialized(
        provider,
        lease_ref,
        scope,
        fn material, public_ref ->
          materialized_api_key_invocation(material, public_ref, attrs, opts)
        end,
        secret_opts(credential_request, opts)
      )
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

  defp connection_id(credential_request) do
    string_value(credential_request, :connection_id) ||
      string_value(credential_request, :connection_ref)
  end

  defp secret_provider(credential_request) do
    provider = value(credential_request, :secret_provider) || EnvProvider

    if is_atom(provider) and Code.ensure_loaded?(provider) and
         function_exported?(provider, :materialize, 3) do
      {:ok, provider}
    else
      {:error, {:secret_provider_unavailable, provider}}
    end
  end

  defp lease_ref(credential_request, attrs) do
    case string_value(credential_request, :lease_ref) ||
           string_value(credential_request, :credential_lease_ref) do
      value when is_binary(value) and value != "" ->
        {:ok, value}

      _missing ->
        adapter_ref = string_value(credential_request, :adapter_ref) || "adapter"
        execution_id = string_value(attrs, :execution_id) || "execution"
        tenant_id = string_value(attrs, :tenant_id) || "tenant"
        {:ok, "credential-lease://#{tenant_id}/#{execution_id}/#{adapter_ref}"}
    end
  end

  defp secret_scope(credential_request) do
    scope =
      case value(credential_request, :secret_scope) do
        %{} = scope -> Map.new(scope)
        list when is_list(list) -> Map.new(list)
        _missing -> %{}
      end

    {:ok,
     scope
     |> Map.put_new(:secret_key, value(credential_request, :secret_key) || :api_key)
     |> maybe_put(:env_var, string_value(credential_request, :env_var))
     |> maybe_put(:key_id, string_value(credential_request, :key_id))}
  end

  defp secret_opts(credential_request, opts) do
    request_opts =
      case value(credential_request, :secret_opts) do
        list when is_list(list) -> list
        %{} = map -> Map.to_list(map)
        _missing -> []
      end

    Keyword.merge(opts, request_opts, fn _key, _left, right -> right end)
  end

  defp api_key_material(material) when is_map(material) do
    Enum.find_value(
      [:api_key, "api_key", :token, "token", :access_token, "access_token"],
      &trimmed_secret_value(Map.get(material, &1))
    )
    |> case do
      value when is_binary(value) -> {:ok, value}
      _missing -> {:error, :missing_secret_material}
    end
  end

  defp api_key_material(_material), do: {:error, :missing_secret_material}

  defp trimmed_secret_value(value) when is_binary(value) do
    case String.trim(value) do
      "" -> nil
      secret -> secret
    end
  end

  defp trimmed_secret_value(_value), do: nil

  defp materialized_api_key_invocation(material, public_ref, attrs, opts) do
    with {:ok, api_key} <- api_key_material(material),
         {:ok, prepared} <-
           LinearCredentialIngress.prepare_api_key_invocation(api_key, attrs, opts) do
      {:ok, attach_secret_receipt(prepared, public_ref)}
    end
  end

  defp attach_secret_receipt(prepared, public_ref) when is_map(prepared) and is_map(public_ref) do
    source_opts =
      prepared
      |> value(:source_opts)
      |> List.wrap()
      |> Keyword.put(:credential_secret_ref, public_ref)

    prepared
    |> Map.put(:credential_secret_ref, public_ref)
    |> Map.put(:source_opts, source_opts)
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

  defp value(list, key) when is_list(list) and is_atom(key) do
    string_key = Atom.to_string(key)

    Enum.find_value(list, fn
      {^key, value} ->
        value

      {binary_key, value} when is_binary(binary_key) ->
        if binary_key == string_key, do: value

      _entry ->
        nil
    end)
  end

  defp value(map, key), do: Map.get(map, key) || Map.get(map, Atom.to_string(key))

  defp maybe_put(map, _key, nil), do: map
  defp maybe_put(map, _key, ""), do: map
  defp maybe_put(map, key, value), do: Map.put(map, key, value)
end
