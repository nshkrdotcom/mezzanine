defmodule MezzanineWorkspaceEngine do
  @moduledoc """
  Neutral workspace allocation and path-safety package for the Mezzanine rebuild.
  """

  @spec components() :: [module()]
  def components do
    [
      Mezzanine.WorkspaceEngine.Allocator,
      Mezzanine.WorkspaceEngine.Hooks,
      Mezzanine.WorkspaceEngine.PathSafety,
      Mezzanine.WorkspaceEngine.WorkspaceLease,
      Mezzanine.WorkspaceEngine.WorkspaceRecord
    ]
  end
end
