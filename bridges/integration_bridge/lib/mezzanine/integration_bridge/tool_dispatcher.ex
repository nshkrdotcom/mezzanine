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
    opts
    |> explicit_allowed_operations(attrs, tool_binding)
    |> allowed_operations_or_fallback(binding_operation(tool_binding), operation_role_ref)
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

  defp explicit_allowed_operations(opts, attrs, binding) do
    Keyword.get(opts, :allowed_operations) ||
      value(attrs, :allowed_operations) ||
      value(binding, :allowed_operations)
  end

  defp binding_operation(binding),
    do: value(binding, :operation_ref) || value(binding, :operation)

  defp allowed_operations_or_fallback(operations, _operation, _role_ref)
       when is_list(operations) and operations != [] do
    Enum.map(operations, &to_string/1)
  end

  defp allowed_operations_or_fallback(_operations, operation, _role_ref)
       when operation != nil and operation != false do
    [to_string(operation)]
  end

  defp allowed_operations_or_fallback(_operations, _operation, role_ref)
       when is_atom(role_ref) or is_binary(role_ref) do
    [to_string(role_ref)]
  end

  defp allowed_operations_or_fallback(_operations, _operation, _role_ref), do: []

  defp value(%_{} = struct, key), do: struct |> Map.from_struct() |> value(key)

  defp value(%{} = map, key) when is_atom(key),
    do: Map.get(map, key) || Map.get(map, Atom.to_string(key))

  defp value(list, key) when is_list(list), do: list |> Map.new() |> value(key)
end
