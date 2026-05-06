defmodule Mezzanine.AdaptiveControlEngine do
  @moduledoc """
  Closed-loop adaptive-control orchestration facade.
  """

  alias Mezzanine.AdaptiveControlEngine.ControlLoop

  @spec evaluate(map()) :: {:ok, ControlLoop.Receipt.t()} | {:error, term()}
  def evaluate(attrs), do: ControlLoop.evaluate(attrs)
end
