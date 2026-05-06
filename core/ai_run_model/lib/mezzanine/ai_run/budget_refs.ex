defmodule Mezzanine.AIRun.BudgetRefs do
  @moduledoc "Cost, token, context, and time budget ref helpers."

  @spec new([term()] | term()) :: [term()]
  def new(refs) when is_list(refs), do: refs
  def new(ref), do: [ref]
end
