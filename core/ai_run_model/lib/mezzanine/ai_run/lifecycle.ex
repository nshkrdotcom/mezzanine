defmodule Mezzanine.AIRun.Lifecycle do
  @moduledoc "Allowed AI run lifecycle states."

  @states [
    :created,
    :admitted,
    :materializing,
    :running,
    :paused,
    :cancel_requested,
    :cancelled,
    :failed,
    :completed,
    :replaced,
    :promotion_requested,
    :promoted,
    :rolled_back
  ]

  @type t ::
          :created
          | :admitted
          | :materializing
          | :running
          | :paused
          | :cancel_requested
          | :cancelled
          | :failed
          | :completed
          | :replaced
          | :promotion_requested
          | :promoted
          | :rolled_back

  @spec initial_state() :: t()
  def initial_state, do: :created

  @spec all() :: [t()]
  def all, do: @states

  @spec valid?(term()) :: boolean()
  def valid?(state), do: state in @states
end
