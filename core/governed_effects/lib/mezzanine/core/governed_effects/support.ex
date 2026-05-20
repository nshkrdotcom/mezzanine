defmodule Mezzanine.Core.GovernedEffects.Support do
  @moduledoc false

  alias GroundPlane.Boundary.Codec

  def normalize_attrs(%_{} = attrs), do: {:ok, Map.from_struct(attrs)}
  def normalize_attrs(attrs) when is_list(attrs), do: {:ok, Map.new(attrs)}
  def normalize_attrs(attrs) when is_map(attrs), do: {:ok, attrs}
  def normalize_attrs(_attrs), do: {:error, :invalid_attrs}

  def required(attrs, key), do: Map.get(attrs, key, Map.get(attrs, Atom.to_string(key)))

  def optional(attrs, key, default \\ nil),
    do: Map.get(attrs, key, Map.get(attrs, Atom.to_string(key), default))

  def reject_unknown(attrs, fields) do
    allowed = MapSet.new(Enum.flat_map(fields, &[&1, Atom.to_string(&1)]))

    if Enum.all?(Map.keys(attrs), &MapSet.member?(allowed, &1)) do
      :ok
    else
      {:error, :unknown_fields}
    end
  end

  def require_fields(attrs, fields) do
    Enum.reduce_while(fields, :ok, fn field, :ok ->
      case required(attrs, field) do
        nil -> {:halt, {:error, {:missing_field, field}}}
        "" -> {:halt, {:error, {:missing_field, field}}}
        _value -> {:cont, :ok}
      end
    end)
  end

  def bounded_atom(value, allowed, error_key) when is_atom(value) do
    if value in allowed, do: {:ok, value}, else: {:error, {error_key, value}}
  end

  def bounded_atom(value, allowed, error_key) when is_binary(value) do
    lookup = Map.new(allowed, &{Atom.to_string(&1), &1})

    case Map.fetch(lookup, value) do
      {:ok, atom} -> {:ok, atom}
      :error -> {:error, {error_key, value}}
    end
  end

  def bounded_atom(value, _allowed, error_key), do: {:error, {error_key, value}}

  def values(attrs, fields, defaults \\ %{}) do
    Map.new(fields, fn field -> {field, optional(attrs, field, Map.get(defaults, field))} end)
  end

  def boundary_map(%_{} = struct, fields) do
    struct
    |> Map.from_struct()
    |> Map.take(fields)
    |> dump_value()
    |> drop_nil_values()
  end

  def ensure_serializable(map) when is_map(map) do
    case Codec.encode(map) do
      {:ok, _encoded} -> :ok
      {:error, reason} -> {:error, {:non_serializable, reason}}
    end
  end

  def encode!(map), do: Codec.encode!(map)
  def digest(map), do: Codec.digest(map)

  def dump_value(%DateTime{} = value), do: DateTime.to_iso8601(value)
  def dump_value(%NaiveDateTime{} = value), do: NaiveDateTime.to_iso8601(value)
  def dump_value(value) when is_atom(value) and not is_nil(value), do: Atom.to_string(value)
  def dump_value(values) when is_list(values), do: Enum.map(values, &dump_value/1)

  def dump_value(%{} = value) do
    Map.new(value, fn {key, item} -> {to_string(key), dump_value(item)} end)
  end

  def dump_value(value), do: value

  def drop_nil_values(map), do: Map.reject(map, fn {_key, value} -> is_nil(value) end)
end
