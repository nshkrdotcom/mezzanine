defmodule Mezzanine.Policy.GrantResolver do
  @moduledoc """
  Typed capability-grant compiler.
  """

  alias Mezzanine.Policy.Helpers
  alias MezzanineOpsModel.CapabilityGrant

  @spec from_config(map()) ::
          {:ok, [CapabilityGrant.t()]}
          | {:error, {:invalid_capability_grant, term()} | {:invalid_grant_mode, term()}}
  def from_config(config) do
    config
    |> Helpers.value(:capability_grants, [])
    |> Enum.reduce_while({:ok, []}, fn raw_grant, {:ok, grants} ->
      case build_grant(raw_grant) do
        {:ok, grant} -> {:cont, {:ok, [grant | grants]}}
        {:error, reason} -> {:halt, {:error, reason}}
      end
    end)
    |> case do
      {:ok, grants} -> {:ok, Enum.reverse(grants)}
      {:error, reason} -> {:error, reason}
    end
  end

  defp build_grant(%{} = raw_grant) do
    capability_id =
      Helpers.value(raw_grant, :capability_id) || Helpers.value(raw_grant, :capability)

    with true <- is_binary(capability_id) or {:error, {:invalid_capability_grant, raw_grant}},
         {:ok, mode} <- cast_mode(Helpers.value(raw_grant, :mode, "allow")),
         {:ok, grant} <-
           CapabilityGrant.new(%{
             capability_id: capability_id,
             mode: mode,
             scope: Helpers.value(raw_grant, :scope),
             constraints: Helpers.value(raw_grant, :constraints, %{})
           }) do
      {:ok, grant}
    else
      {:error, reason} -> {:error, reason}
    end
  end

  defp build_grant(other), do: {:error, {:invalid_capability_grant, other}}

  defp cast_mode(value) when value in [:allow, :deny, :escalate], do: {:ok, value}
  defp cast_mode("allow"), do: {:ok, :allow}
  defp cast_mode("deny"), do: {:ok, :deny}
  defp cast_mode("escalate"), do: {:ok, :escalate}
  defp cast_mode(value), do: {:error, {:invalid_grant_mode, value}}
end
