defmodule Mezzanine.AIRun.PromotionRefs do
  @moduledoc "Shadow, canary, promotion, rollback, and artifact lock ref helpers."

  @spec new([term()] | term()) :: [term()]
  def new(refs) when is_list(refs), do: refs
  def new(ref), do: [ref]
end
