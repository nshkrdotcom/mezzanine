defmodule Mezzanine.IntegrationBridge.EvidenceDispatcher do
  @moduledoc """
  Binding-driven evidence collection dispatch for AppKit evidence roles.
  """

  alias Mezzanine.IntegrationBridge.ProviderAdapters

  @spec collect_evidence(term(), map(), map() | keyword() | nil, keyword()) ::
          {:ok, map()} | {:error, term()}
  def collect_evidence(evidence_role_ref, attrs, evidence_binding, opts \\ [])
      when (is_atom(evidence_role_ref) or is_binary(evidence_role_ref)) and is_map(attrs) and
             is_list(opts) do
    with {:ok, binding} <- normalize_binding(evidence_binding),
         allowed_operations <-
           evidence_allowed_operations(evidence_role_ref, binding, attrs, opts),
         {:ok, adapter} <- evidence_adapter(binding, allowed_operations, opts) do
      attrs
      |> Map.put_new(:evidence_role_ref, evidence_role_ref)
      |> Map.put_new(:allowed_operations, allowed_operations)
      |> adapter.fetch(evidence_opts(binding, allowed_operations, opts))
    end
  end

  @spec evidence_allowed_operations(term(), map() | keyword() | nil, map(), keyword()) ::
          [String.t()]
  def evidence_allowed_operations(evidence_role_ref, evidence_binding, attrs, opts \\ []) do
    opts
    |> explicit_allowed_operations(attrs, evidence_binding)
    |> allowed_operations_or_fallback(binding_operation(evidence_binding), evidence_role_ref)
  end

  defp normalize_binding(nil), do: {:error, :missing_evidence_binding}
  defp normalize_binding(binding) when is_list(binding), do: {:ok, Map.new(binding)}
  defp normalize_binding(%{} = binding), do: {:ok, Map.new(binding)}
  defp normalize_binding(_binding), do: {:error, :invalid_evidence_binding}

  defp evidence_adapter(binding, _allowed_operations, opts) do
    cond do
      adapter = Keyword.get(opts, :evidence_adapter) ->
        {:ok, adapter}

      adapter = value(binding, :adapter_module) ->
        {:ok, adapter}

      adapter_ref = value(binding, :adapter_ref) ->
        ProviderAdapters.resolve(adapter_ref, :evidence)

      true ->
        {:error, :evidence_adapter_not_configured}
    end
  end

  defp evidence_opts(binding, allowed_operations, opts) do
    opts
    |> Keyword.put_new(:allowed_operations, allowed_operations)
    |> Keyword.put_new(:evidence_binding, binding)
  end

  defp operation_refs(%{} = refs) do
    refs
    |> Map.values()
    |> Enum.map(&to_string/1)
    |> Enum.sort()
  end

  defp operation_refs(_refs), do: nil

  defp explicit_allowed_operations(opts, attrs, binding) do
    Keyword.get(opts, :allowed_operations) ||
      value(attrs, :allowed_operations) ||
      operation_refs(value(binding, :operation_refs))
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
