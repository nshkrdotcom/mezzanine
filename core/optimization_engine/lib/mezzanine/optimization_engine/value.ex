defmodule Mezzanine.OptimizationEngine.Value do
  @moduledoc false

  @spec get(map(), atom(), term()) :: term()
  def get(attrs, field, default \\ nil) when is_map(attrs) and is_atom(field) do
    Map.get(attrs, field) || Map.get(attrs, Atom.to_string(field)) || default
  end

  @spec string_list(term()) :: [String.t()]
  def string_list(values) when is_list(values) do
    Enum.filter(values, &(is_binary(&1) and &1 != ""))
  end

  def string_list(_values), do: []
end
