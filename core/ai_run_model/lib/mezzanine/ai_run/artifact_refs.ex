defmodule Mezzanine.AIRun.ArtifactRefs do
  @moduledoc "Prompt, role, candidate, router, eval, replay, promotion, and rollback ref helpers."

  @spec new([term()] | term()) :: [term()]
  def new(refs) when is_list(refs), do: refs
  def new(ref), do: [ref]
end
