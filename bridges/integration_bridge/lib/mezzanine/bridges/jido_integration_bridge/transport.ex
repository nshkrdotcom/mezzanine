defmodule Mezzanine.Bridges.JidoIntegrationBridge.Transport do
  @moduledoc """
  Caller-owned transport contract for Mezzanine model invocations through Jido Integration.
  """

  @type result :: {:ok, map()} | {:error, map()}

  @callback submit_invocation(request :: map(), opts :: keyword()) :: result()
  @callback read_invocation(ref :: String.t(), opts :: keyword()) :: result()
end
