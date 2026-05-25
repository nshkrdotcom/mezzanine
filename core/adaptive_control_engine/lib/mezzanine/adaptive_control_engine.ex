defmodule Mezzanine.AdaptiveControlEngine do
  @moduledoc """
  Closed-loop adaptive-control orchestration facade.
  """

  alias Mezzanine.AdaptiveControlEngine.ControlLoop

  @spec evaluate(map()) :: {:ok, ControlLoop.Receipt.t()} | {:error, term()}
  def evaluate(attrs), do: ControlLoop.evaluate(attrs)

  @spec record_promotion(map()) ::
          {:ok, ControlLoop.PromotionReceipt.t()} | {:error, term()}
  def record_promotion(attrs), do: ControlLoop.record_promotion(attrs)

  @spec record_rollback(map()) ::
          {:ok, ControlLoop.RollbackReceipt.t()} | {:error, term()}
  def record_rollback(attrs), do: ControlLoop.record_rollback(attrs)
end
