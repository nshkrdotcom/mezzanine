defmodule MezzanineOpsModel.Codec do
  @moduledoc """
  Stable serialization helpers for persisting pure operational structs.
  """

  @spec dump(term()) :: term()
  def dump(%DateTime{} = value), do: DateTime.to_iso8601(value)
  def dump(%NaiveDateTime{} = value), do: NaiveDateTime.to_iso8601(value)

  def dump(%_struct{} = value) do
    value
    |> Map.from_struct()
    |> Enum.reduce(%{}, fn {key, item}, acc ->
      Map.put(acc, Atom.to_string(key), dump(item))
    end)
  end

  def dump(value) when is_map(value) do
    Enum.reduce(value, %{}, fn {key, item}, acc ->
      Map.put(acc, dump_key(key), dump(item))
    end)
  end

  def dump(value) when is_list(value), do: Enum.map(value, &dump/1)
  def dump(value) when is_atom(value) and not is_nil(value), do: Atom.to_string(value)
  def dump(value), do: value

  defp dump_key(key) when is_atom(key), do: Atom.to_string(key)
  defp dump_key(key), do: key
end
