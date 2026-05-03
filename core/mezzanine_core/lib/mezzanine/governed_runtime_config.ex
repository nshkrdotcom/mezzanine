defmodule Mezzanine.GovernedRuntimeConfig do
  @moduledoc """
  Bounded runtime-module resolver for governed Mezzanine effects.

  Explicit modules carried by the caller win. When `:governed_default?` is set
  and the request carries authority/workflow markers, application config is not
  consulted; the compiled default is used instead. Standalone callers can keep
  application config compatibility by leaving that option disabled.
  """

  @runtime_module_keys [
    :citadel_bridge,
    :integration_bridge,
    :lower_gateway_impl,
    :receipt_reducer,
    :temporalex_boundary,
    :workflow_runtime_impl
  ]

  @known_keys [
                :authority_packet_ref,
                :binding_snapshot,
                :dispatch_envelope,
                :lower_submission_ref,
                :permission_decision_ref,
                :release_manifest_ref,
                :runtime_modules,
                :signal_id,
                :tenant_id,
                :tenant_ref,
                :workflow_id,
                :workflow_input_ref,
                :workflow_runtime_modules
              ] ++ @runtime_module_keys

  @key_lookup Map.new(@known_keys, &{Atom.to_string(&1), &1})

  @governed_markers [
    :authority_packet_ref,
    :permission_decision_ref,
    :workflow_id,
    :workflow_input_ref,
    :signal_id,
    :lower_submission_ref,
    :dispatch_envelope,
    :binding_snapshot,
    :tenant_ref,
    :tenant_id,
    :release_manifest_ref
  ]

  @type attrs :: map() | keyword() | struct() | nil

  @spec module(attrs(), atom(), atom(), module()) :: module()
  def module(attrs, app, key, default), do: module(attrs, app, key, default, [])

  @spec module(attrs(), atom(), atom(), module(), keyword()) :: module()
  def module(attrs, app, key, default, opts)
      when is_atom(app) and is_atom(key) and is_atom(default) and is_list(opts) do
    attrs = normalize(attrs)

    explicit_module(attrs, key) ||
      if Keyword.get(opts, :governed_default?, false) and governed?(attrs) do
        default
      else
        Application.get_env(app, key, default)
      end
  end

  @spec governed?(attrs()) :: boolean()
  def governed?(attrs) do
    attrs = normalize(attrs)
    Enum.any?(@governed_markers, &present?(map_value(attrs, &1)))
  end

  defp explicit_module(attrs, key) do
    module =
      map_value(attrs, key) ||
        nested_module(attrs, :runtime_modules, key) ||
        nested_module(attrs, :workflow_runtime_modules, key)

    if is_atom(module), do: module
  end

  defp nested_module(attrs, container_key, key) do
    case map_value(attrs, container_key) do
      %{} = modules -> map_value(modules, key)
      _other -> nil
    end
  end

  defp normalize(nil), do: %{}
  defp normalize(attrs) when is_list(attrs), do: attrs |> Map.new() |> normalize()
  defp normalize(%_{} = attrs), do: attrs |> Map.from_struct() |> normalize()

  defp normalize(attrs) when is_map(attrs) do
    Map.new(attrs, fn {key, value} -> {normalize_key(key), value} end)
  end

  defp normalize_key(key) when is_atom(key), do: key
  defp normalize_key(key) when is_binary(key), do: Map.get(@key_lookup, key, key)
  defp normalize_key(key), do: key

  defp map_value(map, key) when is_map(map) do
    case Map.fetch(map, key) do
      {:ok, value} -> value
      :error -> Map.get(map, Atom.to_string(key))
    end
  end

  defp present?(value) when value in [nil, false, ""], do: false
  defp present?(value) when is_list(value), do: value != []
  defp present?(_value), do: true
end
