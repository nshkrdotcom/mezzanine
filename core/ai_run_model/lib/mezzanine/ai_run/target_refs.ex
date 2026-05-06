defmodule Mezzanine.AIRun.TargetRefs do
  @moduledoc "Endpoint, target, attach, sandbox, and surface ref helpers."

  @spec new([term()] | term()) :: [term()]
  def new(refs) when is_list(refs), do: refs
  def new(ref), do: [ref]
end
