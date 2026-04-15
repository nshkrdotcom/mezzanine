defmodule MezzanineOpsModel.Normalizer do
  @moduledoc """
  Deep-normalization helpers for turning external payloads into stable internal
  data.
  """

  @spec normalize_payload(term()) :: term()
  def normalize_payload(value) when is_map(value) do
    value
    |> Enum.reduce(%{}, fn {key, item}, acc ->
      Map.put(acc, normalize_key(key), normalize_payload(item))
    end)
  end

  def normalize_payload(value) when is_list(value) do
    Enum.map(value, &normalize_payload/1)
  end

  def normalize_payload(value) when is_atom(value) and not is_nil(value) do
    Atom.to_string(value)
  end

  def normalize_payload(%_struct{} = value), do: value
  def normalize_payload(value), do: value

  defp normalize_key(key) when is_atom(key), do: Atom.to_string(key)
  defp normalize_key(key), do: key
end
