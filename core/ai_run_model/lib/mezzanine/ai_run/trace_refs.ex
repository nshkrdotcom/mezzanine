defmodule Mezzanine.AIRun.TraceRefs do
  @moduledoc "Trace, replay, divergence, span, and receipt ref helpers."

  @spec new([term()] | term()) :: [term()]
  def new(refs) when is_list(refs), do: refs
  def new(ref), do: [ref]
end
