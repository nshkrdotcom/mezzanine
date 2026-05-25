defmodule Mezzanine.Bridges.OuterBrainBridge.Transport do
  @moduledoc """
  Caller-owned transport contract for Mezzanine context compile/readback calls.

  Prompt rendering is not a transport callback. Rendering is invoked by
  Mezzanine through `Mezzanine.AIExecution.RuntimeDeps.renderer` after this
  transport returns context refs.
  """

  @type result :: {:ok, map()} | {:error, map()}

  @callback compile_context(request :: map(), opts :: keyword()) :: result()
  @callback readback_context(ref :: String.t(), opts :: keyword()) :: result()
end
