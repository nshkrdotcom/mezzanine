defmodule Mezzanine.Bridges.CitadelBridge.Transport do
  @moduledoc """
  Caller-owned transport contract for Mezzanine authority requests to Citadel.
  """

  @type result :: {:ok, map()} | {:error, map()}

  @callback authorize(request :: map(), opts :: keyword()) :: result()
end
