defmodule Mezzanine.AgentRuntime.Support do
  @moduledoc false

  @forbidden_exact_keys MapSet.new(~w[
    prompt raw_prompt raw_provider_body raw_provider_payload tool_call workspace_path
  ])

  @absolute_path_prefixes ["/", "~/"]

  def normalize_attrs(attrs) when is_list(attrs), do: {:ok, Map.new(attrs)}
  def normalize_attrs(%_{} = attrs), do: {:ok, Map.from_struct(attrs)}
  def normalize_attrs(attrs) when is_map(attrs), do: {:ok, attrs}
  def normalize_attrs(_attrs), do: {:error, :invalid_attrs}

  def required(attrs, key), do: Map.get(attrs, key, Map.get(attrs, Atom.to_string(key)))

  def optional(attrs, key, default \\ nil),
    do: Map.get(attrs, key, Map.get(attrs, Atom.to_string(key), default))

  def present_string?(value), do: is_binary(value) and String.trim(value) != ""
  def optional_string?(nil), do: true
  def optional_string?(value), do: present_string?(value)
  def optional_integer?(nil), do: true
  def optional_integer?(value), do: is_integer(value)
  def optional_map?(nil), do: true
  def optional_map?(value), do: is_map(value)

  def safe_ref?(value) when is_binary(value) do
    present_string?(value) and not absolute_path?(value)
  end

  def safe_ref?(_value), do: false

  def reject_unsafe(attrs, error) when is_map(attrs) do
    if unsafe?(attrs), do: {:error, error}, else: :ok
  end

  def dump_value(%{__struct__: module} = value) do
    if function_exported?(module, :dump, 1), do: module.dump(value), else: Map.from_struct(value)
  end

  def dump_value(values) when is_list(values), do: Enum.map(values, &dump_value/1)

  def dump_value(%{} = value),
    do: Map.new(value, fn {key, val} -> {to_string(key), dump_value(val)} end)

  def dump_value(value) when is_atom(value) and not is_boolean(value) and not is_nil(value),
    do: Atom.to_string(value)

  def dump_value(value), do: value

  def drop_nil_values(map), do: Map.reject(map, fn {_key, value} -> is_nil(value) end)

  defp unsafe?(%DateTime{}), do: false
  defp unsafe?(%_{} = struct), do: struct |> Map.from_struct() |> unsafe?()

  defp unsafe?(%{} = map) do
    Enum.any?(map, fn {key, value} -> forbidden_key?(key) or unsafe?(value) end)
  end

  defp unsafe?(values) when is_list(values), do: Enum.any?(values, &unsafe?/1)
  defp unsafe?(value) when is_binary(value), do: absolute_path?(value)
  defp unsafe?(_value), do: false

  defp forbidden_key?(key) when is_atom(key), do: forbidden_key?(Atom.to_string(key))

  defp forbidden_key?(key) when is_binary(key) do
    normalized = String.downcase(key)
    parts = String.split(normalized, "_")

    MapSet.member?(@forbidden_exact_keys, normalized) or static_selector_parts?(parts)
  end

  defp forbidden_key?(_key), do: false

  defp static_selector_parts?(parts) do
    ("pr" in parts and Enum.any?(parts, &(&1 in ["id", "number"]))) or
      ("issue" in parts and Enum.any?(parts, &(&1 in ["id", "number"]))) or
      ("model" in parts and "id" in parts)
  end

  defp absolute_path?(value) when is_binary(value) do
    Enum.any?(@absolute_path_prefixes, &String.starts_with?(value, &1)) or
      windows_absolute_path?(value)
  end

  defp windows_absolute_path?(<<drive, ?:, separator, _rest::binary>>) do
    (drive in ?A..?Z or drive in ?a..?z) and separator in [?\\, ?/]
  end

  defp windows_absolute_path?(_value), do: false
end
