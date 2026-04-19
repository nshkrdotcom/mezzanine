defmodule MezzanineLifecycleEngine do
  @moduledoc """
  Lifecycle coordination package facade.
  """

  @spec components() :: [module()]
  def components do
    [
      Mezzanine.LifecycleEvaluator
    ]
  end
end
