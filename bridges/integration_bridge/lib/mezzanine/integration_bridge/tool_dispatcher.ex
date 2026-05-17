defmodule Mezzanine.IntegrationBridge.ToolDispatcher do
  @moduledoc """
  Binding-driven runtime tool dispatch for AppKit tool roles.
  """

  alias Mezzanine.IntegrationBridge.AuthorizedInvocation
  alias Mezzanine.IntegrationBridge.LinearGraphQLToolExecutor

  @spec invoke_runtime_tool(
          AuthorizedInvocation.t(),
          term(),
          term(),
          term(),
          map() | keyword() | nil,
          keyword()
        ) :: {:ok, map()} | {:error, term()}
  def invoke_runtime_tool(
        %AuthorizedInvocation{} = invocation,
        tool_role_ref,
        operation_role_ref,
        arguments,
        tool_binding,
        opts \\ []
      )
      when (is_atom(tool_role_ref) or is_binary(tool_role_ref)) and
             (is_atom(operation_role_ref) or is_binary(operation_role_ref)) and is_list(opts) do
    with {:ok, binding} <- normalize_binding(tool_binding),
         allowed_operations <-
           runtime_tool_allowed_operations(
             tool_role_ref,
             operation_role_ref,
             binding,
             arguments,
             opts
           ),
         {:ok, tool_name} <- tool_name(binding, tool_role_ref, operation_role_ref) do
      invocation
      |> LinearGraphQLToolExecutor.execute_dynamic_tool(
        tool_name,
        arguments,
        runtime_tool_opts(binding, allowed_operations, opts)
      )
    end
  end

  @spec runtime_tool_allowed_operations(
          term(),
          term(),
          map() | keyword() | nil,
          term(),
          keyword()
        ) ::
          [String.t()]
  def runtime_tool_allowed_operations(
        _tool_role_ref,
        operation_role_ref,
        tool_binding,
        attrs,
        opts \\ []
      ) do
    explicit_operations =
      Keyword.get(opts, :allowed_operations) ||
        value(attrs, :allowed_operations) ||
        value(tool_binding, :allowed_operations)

    cond do
      is_list(explicit_operations) and explicit_operations != [] ->
        Enum.map(explicit_operations, &to_string/1)

      operation = value(tool_binding, :operation_ref) || value(tool_binding, :operation) ->
        [to_string(operation)]

      is_atom(operation_role_ref) or is_binary(operation_role_ref) ->
        [to_string(operation_role_ref)]

      true ->
        []
    end
  end

  defp normalize_binding(nil), do: {:error, :missing_tool_binding}
  defp normalize_binding(binding) when is_list(binding), do: {:ok, Map.new(binding)}
  defp normalize_binding(%{} = binding), do: {:ok, Map.new(binding)}
  defp normalize_binding(_binding), do: {:error, :invalid_tool_binding}

  defp tool_name(binding, tool_role_ref, operation_role_ref) do
    cond do
      name = value(binding, :tool_name) ->
        {:ok, to_string(name)}

      linear_graphql_binding?(binding, operation_role_ref) ->
        {:ok, "linear_graphql"}

      true ->
        {:error, {:unsupported_runtime_tool_binding, tool_role_ref}}
    end
  end

  defp linear_graphql_binding?(binding, operation_role_ref) do
    adapter_ref = value(binding, :adapter_ref) || value(binding, :connector_ref)
    operation_ref = value(binding, :operation_ref) || value(binding, :operation)

    adapter_ref in [:linear, "linear", "jido/connectors/linear"] or
      operation_ref == "linear.graphql.execute" or
      operation_role_ref in [:execute_query, "execute_query", :"linear.graphql.execute"]
  end

  defp runtime_tool_opts(binding, allowed_operations, opts) do
    opts
    |> Keyword.put_new(:allowed_operations, allowed_operations)
    |> Keyword.put_new(:tool_binding, binding)
  end

  defp value(%_{} = struct, key), do: struct |> Map.from_struct() |> value(key)
  defp value(%{} = map, key), do: Map.get(map, key) || Map.get(map, alternate_key(map, key))
  defp value(list, key) when is_list(list), do: list |> Map.new() |> value(key)
  defp value(_map, _key), do: nil

  defp alternate_key(_map, key) when is_atom(key), do: Atom.to_string(key)

  defp alternate_key(map, key) when is_binary(key) do
    Enum.find(Map.keys(map), fn
      existing_key when is_atom(existing_key) -> Atom.to_string(existing_key) == key
      _existing_key -> false
    end)
  end

  defp alternate_key(_map, _key), do: nil
end
