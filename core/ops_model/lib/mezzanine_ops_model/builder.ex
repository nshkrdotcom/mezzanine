defmodule MezzanineOpsModel.Builder do
  @moduledoc false

  @spec build(module(), map() | keyword()) :: {:ok, struct()} | {:error, Exception.t()}
  def build(module, attrs) do
    {:ok, build!(module, attrs)}
  rescue
    error in [ArgumentError, KeyError] -> {:error, error}
  end

  @spec build!(module(), map() | keyword()) :: struct()
  def build!(module, attrs) when is_list(attrs) do
    attrs
    |> Enum.into(%{})
    |> build!(module)
  end

  def build!(module, attrs) when is_map(attrs) do
    struct!(module, attrs)
  end
end
