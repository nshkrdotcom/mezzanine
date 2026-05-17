defmodule Mezzanine.IntegrationBridge.RuntimeDispatcher do
  @moduledoc """
  Binding-driven runtime operation dispatch for AppKit runtime roles.
  """

  alias Mezzanine.IntegrationBridge.CodexAgentRuntime

  @spec invoke_runtime_operation(
          term(),
          term(),
          term(),
          map(),
          map() | keyword() | nil,
          keyword()
        ) ::
          {:ok, map()} | {:error, term()}
  def invoke_runtime_operation(
        _invocation,
        runtime_role_ref,
        operation_role_ref,
        attrs,
        runtime_binding,
        opts \\ []
      )
      when (is_atom(runtime_role_ref) or is_binary(runtime_role_ref)) and
             (is_atom(operation_role_ref) or is_binary(operation_role_ref)) and is_map(attrs) and
             is_list(opts) do
    with {:ok, binding} <- normalize_binding(runtime_binding),
         allowed_operations <-
           runtime_operation_allowed_operations(
             runtime_role_ref,
             operation_role_ref,
             binding,
             attrs,
             opts
           ),
         {:ok, adapter} <- runtime_adapter(binding, allowed_operations, opts) do
      attrs
      |> Map.put_new(:runtime_role_ref, runtime_role_ref)
      |> Map.put_new(:operation_role_ref, operation_role_ref)
      |> adapter.run(runtime_opts(binding, allowed_operations, opts))
    end
  end

  @spec runtime_operation_allowed_operations(
          term(),
          term(),
          map() | keyword() | nil,
          map(),
          keyword()
        ) ::
          [String.t()]
  def runtime_operation_allowed_operations(
        _runtime_role_ref,
        operation_role_ref,
        runtime_binding,
        attrs,
        opts \\ []
      ) do
    explicit_operations =
      Keyword.get(opts, :allowed_operations) ||
        value(attrs, :allowed_operations) ||
        value(runtime_binding, :allowed_operations)

    cond do
      is_list(explicit_operations) and explicit_operations != [] ->
        Enum.map(explicit_operations, &to_string/1)

      operation = value(runtime_binding, :operation_ref) || value(runtime_binding, :operation) ->
        [to_string(operation)]

      is_atom(operation_role_ref) or is_binary(operation_role_ref) ->
        [to_string(operation_role_ref)]

      true ->
        []
    end
  end

  defp normalize_binding(nil), do: {:error, :missing_runtime_binding}
  defp normalize_binding(binding) when is_list(binding), do: {:ok, Map.new(binding)}
  defp normalize_binding(%{} = binding), do: {:ok, Map.new(binding)}
  defp normalize_binding(_binding), do: {:error, :invalid_runtime_binding}

  defp runtime_adapter(binding, allowed_operations, opts) do
    cond do
      adapter = Keyword.get(opts, :runtime_adapter) || Keyword.get(opts, :agent_loop_runtime) ->
        {:ok, adapter}

      adapter = value(binding, :adapter_module) ->
        {:ok, adapter}

      codex_runtime_binding?(binding, allowed_operations) ->
        {:ok, CodexAgentRuntime}

      true ->
        {:error, :runtime_adapter_not_configured}
    end
  end

  defp codex_runtime_binding?(binding, allowed_operations) do
    adapter_ref = value(binding, :adapter_ref) || value(binding, :connector_ref)
    manifest_ref = value(binding, :manifest_ref)

    adapter_ref in [:codex_cli, "codex_cli", "jido/connectors/codex_cli"] or
      manifest_ref == "manifest://jido/connectors/codex_cli@local" or
      "codex.session.turn" in allowed_operations
  end

  defp runtime_opts(binding, allowed_operations, opts) do
    opts
    |> Keyword.put_new(:allowed_operations, allowed_operations)
    |> Keyword.put_new(:runtime_binding, binding)
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
