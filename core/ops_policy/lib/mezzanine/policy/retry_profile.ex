defmodule Mezzanine.Policy.RetryProfile do
  @moduledoc """
  Typed retry-profile compiler.
  """

  alias Mezzanine.Policy.Helpers

  @type strategy :: :none | :linear | :exponential
  @type t :: %{
          strategy: strategy(),
          max_attempts: pos_integer(),
          initial_backoff_ms: non_neg_integer(),
          max_backoff_ms: non_neg_integer()
        }

  @spec from_config(map()) :: {:ok, t()} | {:error, {:invalid_retry_strategy, term()}}
  def from_config(config) do
    retry = Helpers.section(config, :retry)

    with {:ok, strategy} <- cast_strategy(Helpers.value(retry, :strategy, "none")) do
      {:ok,
       %{
         strategy: strategy,
         max_attempts: Helpers.value(retry, :max_attempts, 1),
         initial_backoff_ms: Helpers.value(retry, :initial_backoff_ms, 0),
         max_backoff_ms: Helpers.value(retry, :max_backoff_ms, 0)
       }}
    end
  end

  defp cast_strategy(value) when value in [:none, :linear, :exponential], do: {:ok, value}
  defp cast_strategy("none"), do: {:ok, :none}
  defp cast_strategy("linear"), do: {:ok, :linear}
  defp cast_strategy("exponential"), do: {:ok, :exponential}
  defp cast_strategy(value), do: {:error, {:invalid_retry_strategy, value}}
end
