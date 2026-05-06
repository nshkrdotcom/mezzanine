defmodule Mezzanine.AIRun do
  @moduledoc """
  Public facade for ref-only AI run envelopes and run graph contracts.
  """

  alias Mezzanine.AIRun.Envelope

  @spec new(map()) :: {:ok, Envelope.t()} | {:error, term()}
  defdelegate new(attrs), to: Envelope

  @spec new!(map()) :: Envelope.t()
  defdelegate new!(attrs), to: Envelope
end
