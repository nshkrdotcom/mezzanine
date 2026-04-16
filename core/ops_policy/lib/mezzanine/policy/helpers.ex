defmodule Mezzanine.Policy.Helpers do
  @moduledoc false

  @spec section(map(), atom() | String.t()) :: map()
  def section(config, key) when is_map(config) do
    case value(config, key, %{}) do
      %{} = section -> section
      _ -> %{}
    end
  end

  @spec value(map(), atom() | String.t(), term()) :: term()
  def value(map, key, default \\ nil)

  def value(map, key, default) when is_map(map) and is_atom(key) do
    cond do
      Map.has_key?(map, key) -> Map.get(map, key)
      Map.has_key?(map, Atom.to_string(key)) -> Map.get(map, Atom.to_string(key))
      true -> default
    end
  end

  def value(map, key, default) when is_map(map) and is_binary(key) do
    case Map.fetch(map, key) do
      {:ok, value} ->
        value

      :error ->
        default
    end
  end

  @spec string_list(term()) :: [String.t()]
  def string_list(values) when is_list(values), do: Enum.map(values, &to_string/1)
  def string_list(_), do: []

  @spec boolean(term(), boolean()) :: boolean()
  def boolean(value, default \\ false)

  def boolean(value, _default) when is_boolean(value), do: value
  def boolean("true", _default), do: true
  def boolean("false", _default), do: false
  def boolean(value, default) when is_binary(value), do: String.trim(value) == "true" || default
  def boolean(_value, default), do: default
end
