defmodule MezzanineSourceEngine do
  @moduledoc """
  Neutral source-event admission package for the Mezzanine rebuild.
  """

  @spec components() :: [module()]
  def components do
    [
      Mezzanine.SourceEngine.Admission,
      Mezzanine.SourceEngine.ProviderAdapters.Linear.Issue,
      Mezzanine.SourceEngine.ProviderAdapters.Linear.SourceFlow,
      Mezzanine.SourceEngine.SourceBinding,
      Mezzanine.SourceEngine.SourceCursor,
      Mezzanine.SourceEngine.SourceEvent,
      Mezzanine.SourceEngine.SourceRefreshRequest
    ]
  end
end
