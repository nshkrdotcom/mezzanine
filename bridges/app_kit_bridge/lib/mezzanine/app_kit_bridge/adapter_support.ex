defmodule Mezzanine.AppKitBridge.AdapterSupport do
  @moduledoc false

  def fetch_string(attrs, opts, key, error \\ nil) when is_map(attrs) and is_list(opts) do
    error = error || {:missing_required_field, key}

    case Keyword.get(opts, key) || map_value(attrs, key) do
      value when is_binary(value) and value != "" -> {:ok, value}
      _ -> {:error, error}
    end
  end

  def optional_string(attrs, opts, key, default \\ nil) when is_map(attrs) and is_list(opts) do
    case Keyword.get(opts, key) || map_value(attrs, key) do
      value when is_binary(value) and value != "" -> value
      _ -> default
    end
  end

  def map_value(map, key) when is_map(map),
    do: Map.get(map, key) || Map.get(map, Atom.to_string(key))

  def map_value(_map, _key), do: nil

  def actor(tenant_id) when is_binary(tenant_id), do: %{tenant_id: tenant_id}

  def actor_ref(attrs, opts, default \\ "operator") when is_map(attrs) and is_list(opts) do
    Keyword.get(opts, :actor_ref) || map_value(attrs, :actor_ref) || map_value(attrs, :id) ||
      default
  end

  def normalize_state(nil), do: nil
  def normalize_state(value) when is_atom(value), do: Atom.to_string(value)
  def normalize_state(value), do: value

  def normalize_value(%DateTime{} = value), do: value
  def normalize_value(%NaiveDateTime{} = value), do: value
  def normalize_value(%_{} = value), do: value |> Map.from_struct() |> normalize_value()

  def normalize_value(value) when is_map(value) do
    Map.new(value, fn {key, nested_value} -> {key, normalize_value(nested_value)} end)
  end

  def normalize_value(value) when is_list(value), do: Enum.map(value, &normalize_value/1)
  def normalize_value(value), do: value

  def normalize_error(:not_found), do: :bridge_not_found
  def normalize_error({:missing_required, _field}), do: :bridge_failed
  def normalize_error({:missing_required_field, _field}), do: :bridge_failed
  def normalize_error(%Ash.Error.Invalid{}), do: :bridge_failed
  def normalize_error(reason) when is_atom(reason), do: reason
  def normalize_error(_reason), do: :bridge_failed
end
